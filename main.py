"""
main.py — Asynchronous Event Loop
Orchestrates all modules in a non-blocking poll cycle:

  1. MarketMonitor   → detects whale moves on Polymarket
  2. NewsAnalyzer    → scores macro sentiment alignment
  3. CorrelationMapper → maps to Capital.com Epic IDs + direction
  4. RiskManager     → sizes positions with Kelly Criterion
  5. OrderExecutor   → executes on Capital.com (DEMO or LIVE)

Run with:
    python main.py

Set CAPITAL_MODE=DEMO in .env before going near real money.
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timezone

import aiohttp
import colorlog

import config
from modules.correlation_mapper import CorrelationMapper
from modules.dynamic_whale_discovery import DynamicWhaleDiscovery
from modules.insider_trigger import InsiderTrigger
from modules.market_feed import MarketFeed
from modules.market_monitor import MarketMonitor
from modules.news_analyzer import NewsAnalyzer
from modules.order_executor import OrderExecutor
from modules.risk_manager import RiskManager
from modules.whale_finder import WhaleFinder


# ── Logging Setup ─────────────────────────────────────────────────────────────

def setup_logging() -> None:
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(levelname)s] %(name)s: %(message)s%(reset)s",
        datefmt="%H:%M:%S",
        log_colors={
            "DEBUG":    "cyan",
            "INFO":     "green",
            "WARNING":  "yellow",
            "ERROR":    "red",
            "CRITICAL": "bold_red",
        },
    ))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


# ── Main Orchestrator ─────────────────────────────────────────────────────────

class TradingBot:
    """
    Top-level orchestrator. Each poll cycle:
      1. Scans whale wallets for new positions.
      2. For each WhaleMove → runs news sentiment analysis.
      3. Maps to Capital.com assets.
      4. Risk-checks and sizes each signal.
      5. Executes approved orders.
    """

    def __init__(self):
        self._running = False
        self._risk_manager = RiskManager()
        self._mapper = CorrelationMapper()
        self._feed = MarketFeed()
        self._insider: InsiderTrigger | None = None
        self._cycle_count = 0

    async def run(self) -> None:
        self._running = True
        self._print_banner()

        async with aiohttp.ClientSession() as http_session:
            monitor  = MarketMonitor(http_session)
            analyzer = NewsAnalyzer(http_session)
            analyzer.set_mapper(self._mapper)

            self._insider = InsiderTrigger(http_session)

            async with OrderExecutor(http_session) as executor:
                # Start real-time market data feed
                await self._feed.start()
                await asyncio.sleep(2)  # Let feed populate initial prices
                logger.info("Bot started. Polling every %ds. Mode: %s",
                            config.POLL_INTERVAL_SECONDS, config.CAPITAL_MODE)

                while self._running:
                    try:
                        await self._cycle(monitor, analyzer, executor)
                    except asyncio.CancelledError:
                        break
                    except Exception as exc:
                        logger.exception("Unhandled error in poll cycle: %s", exc)

                    await asyncio.sleep(config.POLL_INTERVAL_SECONDS)

                await self._feed.stop()

        logger.info("Bot stopped cleanly.")

    def stop(self) -> None:
        logger.info("Shutdown signal received — stopping after current cycle...")
        self._running = False

    # ── Single Poll Cycle ──────────────────────────────────────────────────────

    async def _cycle(
        self,
        monitor: MarketMonitor,
        analyzer: NewsAnalyzer,
        executor: OrderExecutor,
    ) -> None:
        self._cycle_count += 1
        logger.info("── Cycle #%d ─────────────────────────────", self._cycle_count)

        # Step 1: Scan all whales
        whale_moves = await monitor.scan_whales()
        if not whale_moves:
            logger.info("No significant whale moves detected this cycle.")
            return

        logger.info("Detected %d whale move(s) this cycle.", len(whale_moves))

        # Step 2–5: Process each move through the full pipeline
        account_balance = await executor.get_account_balance()
        if account_balance <= 0:
            logger.warning("Account balance is zero or unavailable — skipping trades.")
            return

        for move in whale_moves:
            await self._process_move(move, analyzer, executor, account_balance)

    async def _process_move(self, move, analyzer, executor, account_balance: float) -> None:
        logger.info(
            "Processing: %s | %s | Shift: %.2f%% | Size: $%.0f",
            move.slug, move.direction, move.prob_shift_pct, move.size_usdc,
        )

        # Step 2: Insider trigger gate (4h before news spike rule)
        trigger = await self._insider.evaluate(move)
        if not trigger.approved:
            logger.info(
                "InsiderTrigger: %s — %s", trigger.verdict, trigger.reason
            )
            return

        # Step 3: Sentiment analysis
        sentiment = await analyzer.analyze(move)

        if sentiment.recommendation == "SKIP":
            logger.info("NewsAnalyzer says SKIP (score=%.1f) — no trade.", sentiment.confidence_score)
            return

        if sentiment.recommendation == "MONITOR":
            logger.info("NewsAnalyzer says MONITOR (score=%.1f) — watching, no trade.", sentiment.confidence_score)
            return

        # Step 4: Map to Capital.com signals
        signals = self._mapper.resolve(sentiment)
        if not signals:
            logger.warning("No correlated Capital.com assets found for slug '%s'.", move.slug)
            return

        # Step 5 & 6: Risk check + execute each signal
        open_epics = await executor.get_open_epics()
        tasks = [
            self._evaluate_and_execute(signal, executor, account_balance, open_epics)
            for signal in signals
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error("Trade pipeline error: %s", result)
            else:
                logger.info(str(result))

    async def _evaluate_and_execute(self, signal, executor, account_balance: float, open_epics: set = set()):
        """Fetch live price (WebSocket feed first, REST fallback) → risk-size → execute."""
        if signal.epic in open_epics:
            logger.info("[Executor] Skipping %s — position already open.", signal.epic)
            return None
        feed_snap = self._feed.get_snapshot(signal.epic)
        if feed_snap and feed_snap.mid > 0:
            current_price = feed_snap.mid
            spread        = feed_snap.spread
            logger.debug("[Feed] %s price from WebSocket: %.5f", signal.epic, current_price)
        else:
            rest_snap     = await executor.get_market_snapshot(signal.epic)
            current_price = rest_snap["mid"]
            spread        = rest_snap["spread"]
            logger.debug("[Feed] %s price from REST fallback: %.5f", signal.epic, current_price)

        if current_price <= 0:
            logger.warning("Cannot price %s — skipping.", signal.epic)
            return None

        sized_order = self._risk_manager.evaluate(
            signal=signal,
            account_balance=account_balance,
            current_price=current_price,
            spread=spread,
        )
        logger.info(str(sized_order))

        if not sized_order.approved:
            return sized_order

        return await executor.execute(sized_order)

    # ── Cosmetic ───────────────────────────────────────────────────────────────

    @staticmethod
    def _print_banner() -> None:
        mode_color = "\033[93m" if config.CAPITAL_MODE == "DEMO" else "\033[91m"
        reset = "\033[0m"
        print(f"""
{mode_color}
╔══════════════════════════════════════════════════════╗
║        POLYMARKET ↔ CAPITAL.COM TRADING BOT          ║
║  Mode: {config.CAPITAL_MODE:<8}  Whales: {len(config.WHALE_WALLETS):<4}  Poll: {config.POLL_INTERVAL_SECONDS}s     ║
║  Max Risk/Trade: {config.MAX_RISK_PER_TRADE_PCT}%   Daily DD Limit: {config.MAX_DAILY_DRAWDOWN_PCT}%     ║
╚══════════════════════════════════════════════════════╝
{reset}""")


logger = logging.getLogger(__name__)


# ── Entry Point ───────────────────────────────────────────────────────────────

async def main() -> None:
    setup_logging()

    # Dynamic whale discovery: leaderboard → filter → qualify
    async with aiohttp.ClientSession() as discovery_session:
        discovery = DynamicWhaleDiscovery(discovery_session)
        wallets = await discovery.discover(top_n=20)
        if wallets:
            config.WHALE_WALLETS = wallets
            logger.info(
                "[Main] Dynamic discovery complete — monitoring %d wallets.", len(wallets)
            )

    if config.CAPITAL_MODE == "LIVE":
        logger.critical("⚠  LIVE MODE ACTIVE — real money is at risk!")
        logger.critical("    You have 5 seconds to Ctrl+C if this is unintentional.")
        await asyncio.sleep(5)

    bot = TradingBot()

    # Graceful shutdown on SIGINT / SIGTERM
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, bot.stop)

    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(0)
