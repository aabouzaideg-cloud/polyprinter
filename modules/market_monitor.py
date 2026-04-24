"""
market_monitor.py — Polymarket Intelligence Module
Polls the Polymarket CLOB API for whale wallet activity and detects
implied-probability shifts that exceed the configured threshold.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp

import config

logger = logging.getLogger(__name__)


@dataclass
class WhaleMove:
    """Represents a significant position taken by a tracked whale wallet."""
    wallet: str
    market_id: str          # Polymarket condition_id
    slug: str               # Human-readable market slug
    question: str           # Full market question text
    outcome: str            # "YES" or "NO"
    size_usdc: float        # Size of the trade in USDC
    price_before: float     # Implied probability before the move (0-1)
    price_after: float      # Implied probability after the move (0-1)
    prob_shift_pct: float   # Absolute shift in percentage points
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def direction(self) -> str:
        """'BULLISH' if whale bets YES, 'BEARISH' if NO."""
        return "BULLISH" if self.outcome == "YES" else "BEARISH"


class MarketMonitor:
    """
    Monitors tracked whale wallets on Polymarket and emits WhaleMove events
    when implied-probability shifts exceed WHALE_TRIGGER_THRESHOLD.
    """

    CLOB_BASE = config.POLYMARKET_BASE_URL
    DATA_API  = "https://data-api.polymarket.com"

    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        # Cache: market_id -> last known YES price, to detect shifts
        self._price_cache: dict[str, float] = {}
        # Cache: market_id -> market metadata (slug, question)
        self._market_cache: dict[str, dict] = {}
        # Trades we've already processed, to avoid duplicate signals
        self._seen_trade_ids: set[str] = set()

    # ── Public API ─────────────────────────────────────────────────────────────

    async def scan_whales(self) -> list[WhaleMove]:
        """
        Scan all tracked whale wallets for new large positions.
        Returns a list of WhaleMove events that crossed the threshold.
        """
        moves: list[WhaleMove] = []
        for wallet in config.WHALE_WALLETS:
            try:
                wallet_moves = await self._scan_wallet(wallet)
                moves.extend(wallet_moves)
            except Exception as exc:
                logger.warning("Failed to scan wallet %s: %s", wallet, exc)
        return moves

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _scan_wallet(self, wallet: str) -> list[WhaleMove]:
        """Fetch recent trades for a single wallet and evaluate each one."""
        trades = await self._fetch_trades(wallet)
        moves = []
        for trade in trades:
            trade_id = trade.get("transactionHash", trade.get("id", ""))
            if trade_id in self._seen_trade_ids:
                continue
            self._seen_trade_ids.add(trade_id)

            move = await self._evaluate_trade(wallet, trade)
            if move:
                moves.append(move)
        return moves

    async def _evaluate_trade(self, wallet: str, trade: dict) -> Optional[WhaleMove]:
        """
        Determine if a single trade constitutes a significant whale move.
        Returns a WhaleMove if the probability shift exceeds the threshold.
        """
        # data-api.polymarket.com field names
        market_id: str  = trade.get("conditionId", trade.get("market", ""))
        outcome: str    = trade.get("outcome", "YES").upper()
        size: float     = float(trade.get("size", 0))
        price: float    = float(trade.get("price", 0))
        slug: str       = trade.get("slug", market_id)
        question: str   = trade.get("title", "Unknown market")

        if not market_id or size < 1000:  # Ignore tiny trades (< $1,000 USDC)
            return None

        # Calculate probability shift vs our last cached price
        yes_price = price if outcome == "YES" else (1.0 - price)
        prev_price = self._price_cache.get(market_id, yes_price)
        shift_pct = abs(yes_price - prev_price) * 100.0

        # Update price cache
        self._price_cache[market_id] = yes_price

        if shift_pct < config.WHALE_TRIGGER_THRESHOLD:
            logger.debug(
                "Whale move on %s (shift=%.2f%%) below threshold — skipping.",
                slug, shift_pct
            )
            return None

        logger.info(
            "WHALE SIGNAL | Wallet: %s | Market: %s | %s %s | Shift: %.2f%%",
            wallet[:8] + "...", slug, outcome, f"${size:,.0f}", shift_pct
        )

        return WhaleMove(
            wallet=wallet,
            market_id=market_id,
            slug=slug,
            question=question,
            outcome=outcome,
            size_usdc=size,
            price_before=prev_price,
            price_after=yes_price,
            prob_shift_pct=shift_pct,
        )

    async def _fetch_trades(self, wallet: str) -> list[dict]:
        """GET data-api.polymarket.com/trades?user={wallet}&limit=20 (no auth required)"""
        url = f"{self.DATA_API}/trades"
        params = {"user": wallet, "limit": 20}
        try:
            async with self._session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data if isinstance(data, list) else data.get("data", [])
        except aiohttp.ClientResponseError as e:
            logger.error("Polymarket trades API error for %s: %s %s", wallet, e.status, e.message)
            return []

    async def _get_market_info(self, market_id: str) -> Optional[dict]:
        """GET /markets/{market_id} — cached to avoid repeated calls."""
        if market_id in self._market_cache:
            return self._market_cache[market_id]

        url = f"{self.CLOB_BASE}/markets/{market_id}"
        headers = self._build_headers()
        try:
            async with self._session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
                info = await resp.json()
                self._market_cache[market_id] = info
                return info
        except Exception as e:
            logger.warning("Could not fetch market info for %s: %s", market_id, e)
            return None

    def _build_headers(self) -> dict:
        headers = {"Accept": "application/json"}
        if config.POLYMARKET_API_KEY:
            headers["Authorization"] = f"Bearer {config.POLYMARKET_API_KEY}"
        return headers
