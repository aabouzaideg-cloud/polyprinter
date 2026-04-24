"""
whale_finder.py — Dune Analytics Whale Discovery
Fetches top Polymarket traders by volume from Dune query 6260455
and auto-populates the whale wallet list at bot startup.
"""

import logging
import os
from dotenv import load_dotenv, set_key
from dune_client.client import DuneClient

import config

logger = logging.getLogger(__name__)

DUNE_QUERY_ID = 6260455
ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")


class WhaleFinder:
    """
    Queries Dune Analytics for the top Polymarket wallets by volume
    and returns a ranked list of wallet addresses.
    """

    def __init__(self, top_n: int = 10, min_volume_usdc: float = 1_000_000):
        self._top_n = top_n
        self._min_volume = min_volume_usdc
        self._client = DuneClient(config.DUNE_API_KEY)

    def fetch_top_wallets(self) -> list[str]:
        """
        Pull latest results from Dune query and return top N wallet addresses
        filtered by minimum volume threshold.
        """
        logger.info("[WhaleFinder] Fetching top Polymarket wallets from Dune (query #%d)...", DUNE_QUERY_ID)
        try:
            result = self._client.get_latest_result(DUNE_QUERY_ID)
            rows = result.result.rows

            # Filter by minimum volume and sort by volume descending
            filtered = [
                r for r in rows
                if float(r.get("total_volume_usdc", 0)) >= self._min_volume
                and r.get("wallet")
            ]
            filtered.sort(key=lambda r: float(r["total_volume_usdc"]), reverse=True)

            wallets = [r["wallet"] for r in filtered[:self._top_n]]

            logger.info(
                "[WhaleFinder] Found %d qualifying wallets (top %d selected, min volume $%s).",
                len(filtered), len(wallets), f"{self._min_volume:,.0f}",
            )
            for i, r in enumerate(filtered[:self._top_n], 1):
                logger.info(
                    "  #%d | %s | Volume: $%s | Trades: %s",
                    i, r["wallet"],
                    f"{float(r['total_volume_usdc']):,.0f}",
                    f"{int(r.get('trades_count', 0)):,}",
                )
            return wallets

        except Exception as e:
            logger.error("[WhaleFinder] Dune fetch failed: %s — falling back to .env wallets.", e)
            return config.WHALE_WALLETS

    def update_env_wallets(self, wallets: list[str]) -> None:
        """Persist the discovered wallets back to .env for future runs."""
        if not wallets:
            return
        wallet_str = ",".join(wallets)
        set_key(ENV_PATH, "WHALE_WALLETS", wallet_str)
        logger.info("[WhaleFinder] Updated WHALE_WALLETS in .env with %d addresses.", len(wallets))
