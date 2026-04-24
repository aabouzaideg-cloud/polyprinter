"""
dynamic_whale_discovery.py — Dynamic Whale Discovery Engine
Replaces static Dune-based WhaleFinder with a live, filtered leaderboard.

Pipeline:
  1. Fetch top 200 wallets by PnL from Polymarket leaderboard API
  2. For each wallet, fetch recent trade history and compute:
       - trade_count  (min 50 required)
       - win_rate     (min 55% required)
       - specialization (Economics / Politics / Global Affairs inferred from slugs)
  3. Return only "Category Specialist" wallets, ranked by PnL
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp

import config
from modules.market_monitor import WhaleMove

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
LEADERBOARD_URL  = "https://data-api.polymarket.com/v1/leaderboard"
TRADES_URL       = "https://data-api.polymarket.com/trades"
MARKETS_URL      = "https://data-api.polymarket.com/markets"

MIN_TRADES       = 50
MIN_WIN_RATE     = 0.55
LEADERBOARD_PULL = 200   # How many top wallets to evaluate

# Slug keyword sets that map to each specialist category
CATEGORY_KEYWORDS = {
    "economics":      ["fed", "rate", "inflation", "cpi", "fomc", "gdp", "recession",
                       "unemployment", "debt", "treasury", "opec", "oil", "energy",
                       "tariff", "trade-war", "dollar", "currency", "ipo", "earnings"],
    "politics":       ["trump", "biden", "harris", "election", "president", "senate",
                       "congress", "republican", "democrat", "vote", "poll", "impeach",
                       "starmer", "sunak", "uk", "eu", "macron", "chancellor", "parliament",
                       "referendum", "prime-minister"],
    "global_affairs": ["war", "ceasefire", "invasion", "sanctions", "iran", "russia",
                       "ukraine", "china", "nato", "missile", "nuclear", "conflict",
                       "coup", "blockade", "airstrike", "treaty", "un", "g7", "g20"],
}

TARGET_CATEGORIES = set(CATEGORY_KEYWORDS.keys())


@dataclass
class WalletProfile:
    wallet: str
    pnl: float
    vol: float
    trade_count: int = 0
    win_rate: float  = 0.0
    categories: set  = field(default_factory=set)
    qualified: bool  = False

    def __str__(self) -> str:
        cats = ", ".join(self.categories) if self.categories else "unclassified"
        return (
            f"{self.wallet[:10]}... | PnL: ${self.pnl:,.0f} | "
            f"Trades: {self.trade_count} | WinRate: {self.win_rate:.0%} | "
            f"Tags: {cats} | {'✓ QUALIFIED' if self.qualified else '✗ filtered'}"
        )


class DynamicWhaleDiscovery:
    """
    Runs the full discovery pipeline and returns a filtered, ranked
    list of wallet addresses ready for MarketMonitor.
    """

    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        # Cache wallet profiles to avoid re-fetching on every run
        self._profile_cache: dict[str, WalletProfile] = {}

    async def discover(self, top_n: int = 20) -> list[str]:
        """
        Full pipeline: fetch → profile → filter → rank → return top_n wallets.
        Falls back to cached results if the API is unavailable.
        """
        logger.info("[WhaleDiscovery] Starting discovery pipeline (top %d)...", top_n)

        # Step 1: Fetch leaderboard
        candidates = await self._fetch_leaderboard(LEADERBOARD_PULL)
        if not candidates:
            logger.warning("[WhaleDiscovery] Leaderboard fetch failed — using cache.")
            return self._from_cache(top_n)

        logger.info("[WhaleDiscovery] Evaluating %d candidate wallets...", len(candidates))

        # Step 2: Profile each wallet (with concurrency limit to avoid rate limits)
        sem = asyncio.Semaphore(10)
        tasks = [self._profile_wallet(w, sem) for w in candidates]
        profiles = await asyncio.gather(*tasks, return_exceptions=True)

        qualified = []
        for p in profiles:
            if isinstance(p, WalletProfile) and p.qualified:
                qualified.append(p)

        # Step 3: Rank by PnL descending
        qualified.sort(key=lambda p: p.pnl, reverse=True)

        logger.info(
            "[WhaleDiscovery] %d/%d wallets passed all filters.",
            len(qualified), len(candidates),
        )
        for p in qualified[:top_n]:
            logger.info("  %s", p)

        wallets = [p.wallet for p in qualified[:top_n]]

        # Always include manually specified wallets from .env
        for w in config.WHALE_WALLETS:
            if w not in wallets:
                wallets.append(w)

        return wallets

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _fetch_leaderboard(self, limit: int) -> list[dict]:
        """Fetch top wallets from Polymarket leaderboard."""
        try:
            async with self._session.get(
                LEADERBOARD_URL,
                params={"limit": limit},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            logger.error("[WhaleDiscovery] Leaderboard fetch error: %s", e)
            return []

    async def _profile_wallet(
        self, entry: dict, sem: asyncio.Semaphore
    ) -> WalletProfile:
        """Fetch trade history for a wallet and compute quality metrics."""
        wallet = entry.get("proxyWallet", "")
        pnl    = float(entry.get("pnl", 0))
        vol    = float(entry.get("vol", 0))

        profile = WalletProfile(wallet=wallet, pnl=pnl, vol=vol)

        # Use cache if available
        if wallet in self._profile_cache:
            cached = self._profile_cache[wallet]
            cached.pnl = pnl  # Update PnL with latest
            return cached

        async with sem:
            try:
                trades = await self._fetch_trades(wallet, limit=200)
            except Exception as e:
                logger.debug("[WhaleDiscovery] Trade fetch failed for %s: %s", wallet[:10], e)
                return profile

        if not trades:
            return profile

        profile.trade_count = len(trades)

        # ── Win rate: resolved trades only ────────────────────────────────────
        wins = losses = 0
        categories: set[str] = set()

        for trade in trades:
            slug    = trade.get("slug", "").lower()
            outcome = trade.get("outcome", "").upper()
            winner  = trade.get("winner", None)  # May be set if market resolved

            # Category classification from slug
            for cat, keywords in CATEGORY_KEYWORDS.items():
                if any(kw in slug for kw in keywords):
                    categories.add(cat)

            # Win rate from resolved markets
            if winner is not None:
                if (outcome == "YES" and winner is True) or \
                   (outcome == "NO"  and winner is False):
                    wins += 1
                else:
                    losses += 1

        total_resolved = wins + losses
        if total_resolved >= 10:
            profile.win_rate = wins / total_resolved
        elif profile.pnl > 0 and profile.vol > 0:
            # Fallback: use PnL/Vol ratio as win-rate proxy (positive PnL = good trader)
            roi = profile.pnl / profile.vol
            profile.win_rate = min(0.5 + roi, 0.95)  # Map ROI to 0.5–0.95 range
        else:
            profile.win_rate = 0.0
        profile.categories = categories

        # ── Apply filters ─────────────────────────────────────────────────────
        has_category  = bool(categories & TARGET_CATEGORIES)
        meets_trades  = profile.trade_count >= MIN_TRADES
        meets_winrate = profile.win_rate >= MIN_WIN_RATE

        profile.qualified = has_category and meets_trades and meets_winrate

        self._profile_cache[wallet] = profile
        return profile

    async def _fetch_trades(self, wallet: str, limit: int = 200) -> list[dict]:
        """GET data-api.polymarket.com/trades?user={wallet}&limit={limit}"""
        async with self._session.get(
            TRADES_URL,
            params={"user": wallet, "limit": limit},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data if isinstance(data, list) else []

    def _from_cache(self, top_n: int) -> list[str]:
        """Return best known wallets from profile cache."""
        qualified = [p for p in self._profile_cache.values() if p.qualified]
        qualified.sort(key=lambda p: p.pnl, reverse=True)
        return [p.wallet for p in qualified[:top_n]]
