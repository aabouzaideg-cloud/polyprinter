"""
correlation_mapper.py — Cross-Platform Correlation Mapper
Translates a Polymarket slug + outcome into a list of Capital.com
Epic IDs and their trade directions.
"""

import json
import logging
from dataclasses import dataclass

import config
from modules.market_monitor import WhaleMove
from modules.news_analyzer import SentimentResult

logger = logging.getLogger(__name__)


@dataclass
class TradeSignal:
    """A fully resolved trade instruction ready for the execution engine."""
    epic: str               # Capital.com Epic ID (e.g. "GOLD", "US500")
    direction: str          # "BUY" or "SELL"
    rationale: str          # Human-readable justification
    confidence_score: float # From NewsAnalyzer
    category: str           # Matched Polymarket category
    slug: str               # Source Polymarket slug
    whale_wallet: str       # Source whale wallet (truncated)
    prob_shift_pct: float   # Probability shift that triggered this

    def __str__(self) -> str:
        return (
            f"[Signal] {self.direction} {self.epic} | "
            f"Slug: {self.slug} | "
            f"Confidence: {self.confidence_score:.1f} | "
            f"Reason: {self.rationale}"
        )


class CorrelationMapper:
    """
    Loads the JSON mapping file and resolves Polymarket events to
    Capital.com trade signals.
    """

    def __init__(self):
        self._mappings: dict = self._load_mappings()

    def resolve(self, sentiment: SentimentResult) -> list[TradeSignal]:
        """
        Given a SentimentResult, return all applicable TradeSignals.
        Direction is flipped if the whale bet "NO" (bearish).
        """
        move     = sentiment.whale_move
        category = sentiment.category
        cat_data = self._mappings.get("categories", {}).get(category)

        if not cat_data:
            logger.warning("No mapping found for category '%s' — cannot generate signals.", category)
            return []

        signals: list[TradeSignal] = []
        for asset in cat_data.get("assets", []):
            epic      = asset["epic"]
            base_dir  = asset["direction_on_yes"]   # "BUY" or "SELL"
            rationale = asset["rationale"]

            # Flip direction if whale bet NO
            direction = base_dir if move.outcome == "YES" else self._flip(base_dir)

            signals.append(TradeSignal(
                epic=epic,
                direction=direction,
                rationale=rationale,
                confidence_score=sentiment.confidence_score,
                category=category,
                slug=move.slug,
                whale_wallet=move.wallet[:10] + "...",
                prob_shift_pct=move.prob_shift_pct,
            ))
            logger.info(str(signals[-1]))

        return signals

    # ── Helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _flip(direction: str) -> str:
        return "SELL" if direction == "BUY" else "BUY"

    def _load_mappings(self) -> dict:
        try:
            with open(config.MAPPINGS_PATH, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error("mappings.json not found at %s", config.MAPPINGS_PATH)
            return {}
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in mappings.json: %s", e)
            return {}
