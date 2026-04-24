"""
market_feed.py — Massive.com WebSocket Market Data Feed
Streams real-time prices for all Capital.com assets correlated in mappings.json.
Replaces REST polling in OrderExecutor.get_market_snapshot() with a live cache.

Streams:
  - wss://socket.massive.com/forex/C     → EURUSD, USDJPY
  - wss://socket.massive.com/crypto/XT   → BTC-USD, ETH-USD, SOL-USD
  - wss://socket.massive.com/indices/V   → I:SPX (US500 proxy)

Auth flow (post-connect):
  1. Send: {"action": "auth", "params": "<API_KEY>"}
  2. Send: {"action": "subscribe", "params": "<TICKER>"}
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

import config

logger = logging.getLogger(__name__)


# ── Ticker Maps ───────────────────────────────────────────────────────────────
# Capital.com Epic ID → (Massive feed path, Massive ticker, price field)
EPIC_TO_FEED: dict[str, tuple[str, str, str]] = {
    # Forex
    "EURUSD":    ("forex/C",    "EUR-USD",  "mid"),   # mid = (ask+bid)/2
    "USDJPY":    ("forex/C",    "USD-JPY",  "mid"),
    # Crypto
    "BTCUSD":    ("crypto/XT",  "BTC-USD",  "p"),
    "ETHUSD":    ("crypto/XT",  "ETH-USD",  "p"),
    "SOLUSD":    ("crypto/XT",  "SOL-USD",  "p"),
    # Indices (SPX as US500 proxy)
    "US500":     ("indices/V",  "I:SPX",    "val"),
    # Commodities — no Massive feed, fallback to REST
    "GOLD":      (None, None, None),
    "OIL_CRUDE": (None, None, None),
}

# Reverse map: Massive ticker → Epic ID
TICKER_TO_EPIC: dict[str, str] = {
    v[1]: k for k, v in EPIC_TO_FEED.items() if v[1]
}


@dataclass
class PriceSnapshot:
    epic: str
    bid: float = 0.0
    ask: float = 0.0
    mid: float = 0.0
    spread: float = 0.0
    source: str = "websocket"   # "websocket" or "rest_fallback"


class MarketFeed:
    """
    Maintains a live price cache for all correlated assets.
    Runs three concurrent WebSocket connections (forex, crypto, indices).
    Provides get_snapshot() for the OrderExecutor to call synchronously.
    """

    def __init__(self):
        self._cache: dict[str, PriceSnapshot] = {}
        self._running = False
        self._tasks: list[asyncio.Task] = []

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_snapshot(self, epic: str) -> Optional[PriceSnapshot]:
        """Return the latest cached price snapshot for an epic, or None."""
        return self._cache.get(epic)

    async def start(self) -> None:
        """Launch all WebSocket stream tasks. Skips if no API key or plan lacks access."""
        if not config.MASSIVE_API_KEY:
            logger.info("[MarketFeed] No MASSIVE_API_KEY — skipping WebSocket feed.")
            return
        self._running = True
        feeds = {
            "forex/C":   [t for e, (f, t, _) in EPIC_TO_FEED.items() if f == "forex/C"],
            "crypto/XT": [t for e, (f, t, _) in EPIC_TO_FEED.items() if f == "crypto/XT"],
            "indices/V": [t for e, (f, t, _) in EPIC_TO_FEED.items() if f == "indices/V"],
        }
        for path, tickers in feeds.items():
            if tickers:
                task = asyncio.create_task(
                    self._stream(path, tickers),
                    name=f"feed:{path}",
                )
                self._tasks.append(task)
        logger.info("[MarketFeed] Started %d WebSocket streams.", len(self._tasks))

    async def stop(self) -> None:
        """Cancel all stream tasks."""
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("[MarketFeed] All streams stopped.")

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _stream(self, path: str, tickers: list[str]) -> None:
        """
        Maintain a single WebSocket connection with auto-reconnect.
        Authenticates, subscribes, and routes incoming messages to _handle().
        """
        url = f"{config.MASSIVE_WS_BASE}/{path}"
        backoff = 1

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        url,
                        timeout=aiohttp.ClientWSTimeout(ws_close=10),
                        heartbeat=30,
                    ) as ws:
                        logger.info("[MarketFeed] Connected: %s", url)
                        backoff = 1  # Reset on successful connect

                        # Step 1: Authenticate
                        await ws.send_str(json.dumps({
                            "action": "auth",
                            "params": config.MASSIVE_API_KEY,
                        }))

                        # Step 2: Subscribe to all tickers for this feed
                        sub_str = ",".join(tickers)
                        await ws.send_str(json.dumps({
                            "action": "subscribe",
                            "params": sub_str,
                        }))
                        logger.info("[MarketFeed] Subscribed to: %s", sub_str)

                        # Check auth response for plan limitation
                        auth_resp = await asyncio.wait_for(ws.receive(), timeout=5)
                        auth_data = json.loads(auth_resp.data or "[]")
                        for item in (auth_data if isinstance(auth_data, list) else [auth_data]):
                            if "plan" in str(item.get("message", "")).lower():
                                logger.info(
                                    "[MarketFeed] WebSocket unavailable on current plan — "
                                    "upgrade at massive.com/pricing. Using REST fallback."
                                )
                                return

                        # Step 3: Message loop
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._handle(msg.data)
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                logger.warning("[MarketFeed] Stream %s closed/error.", path)
                                break

            except asyncio.CancelledError:
                return
            except Exception as e:
                err = str(e)
                if "plan" in err.lower() or "upgrade" in err.lower() or "404" in err:
                    logger.info(
                        "[MarketFeed] WebSocket unavailable on current plan — "
                        "upgrade at massive.com/pricing to enable. Using REST fallback."
                    )
                    return  # Don't retry — plan limitation, not a transient error
                logger.warning(
                    "[MarketFeed] Stream %s disconnected (%s) — reconnecting in %ds.",
                    path, e, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def _handle(self, raw: str) -> None:
        """Parse a raw WebSocket message and update the price cache."""
        try:
            messages = json.loads(raw)
            if not isinstance(messages, list):
                messages = [messages]

            for msg in messages:
                ev = msg.get("ev", "")

                if ev == "C":       # Forex quote
                    self._update_forex(msg)
                elif ev == "XT":    # Crypto trade
                    self._update_crypto(msg)
                elif ev == "V":     # Index value
                    self._update_index(msg)
                elif ev in ("status", "auth_success", "auth_failed"):
                    logger.info("[MarketFeed] Status: %s", msg)

        except (json.JSONDecodeError, KeyError) as e:
            logger.debug("[MarketFeed] Could not parse message: %s — %s", raw[:100], e)

    def _update_forex(self, msg: dict) -> None:
        """ev=C: {p: 'EUR-USD', a: ask, b: bid}"""
        ticker = msg.get("p", "").replace("/", "-")
        epic   = TICKER_TO_EPIC.get(ticker)
        if not epic:
            return
        ask = float(msg.get("a", 0))
        bid = float(msg.get("b", 0))
        mid = (ask + bid) / 2.0
        self._cache[epic] = PriceSnapshot(
            epic=epic, bid=bid, ask=ask, mid=mid, spread=ask - bid
        )

    def _update_crypto(self, msg: dict) -> None:
        """ev=XT: {pair: 'BTC-USD', p: price}"""
        ticker = msg.get("pair", "")
        epic   = TICKER_TO_EPIC.get(ticker)
        if not epic:
            return
        price = float(msg.get("p", 0))
        # No bid/ask on trade ticks — use price as mid, estimate 0.1% spread
        spread = price * 0.001
        self._cache[epic] = PriceSnapshot(
            epic=epic, bid=price - spread / 2,
            ask=price + spread / 2, mid=price, spread=spread,
        )

    def _update_index(self, msg: dict) -> None:
        """ev=V: {T: 'I:SPX', val: value}"""
        ticker = msg.get("T", "")
        epic   = TICKER_TO_EPIC.get(ticker)
        if not epic:
            return
        val = float(msg.get("val", 0))
        spread = val * 0.0002  # Indices typically ~0.02% spread
        self._cache[epic] = PriceSnapshot(
            epic=epic, bid=val - spread / 2,
            ask=val + spread / 2, mid=val, spread=spread,
        )
