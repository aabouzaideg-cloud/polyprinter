"""
news_analyzer.py — Global News & Sentiment Engine
Fetches recent headlines relevant to a Polymarket slug and scores
confidence by keyword alignment between the whale bet and current news.
"""

import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote_plus

import aiohttp

import config
from modules.market_monitor import WhaleMove

logger = logging.getLogger(__name__)


# ── Keyword Taxonomy ──────────────────────────────────────────────────────────
# Maps topic families to headline keywords and their sentiment sign.
# sign=+1 means "bullish for YES outcome", sign=-1 means "bearish for YES"
KEYWORD_TAXONOMY: dict[str, list[tuple[str, int]]] = {
    "geopolitics": [
        ("war", +1), ("conflict", +1), ("attack", +1), ("invasion", +1),
        ("sanctions", +1), ("ceasefire", -1), ("peace", -1), ("talks", -1),
    ],
    "monetary_policy": [
        ("rate cut", +1), ("dovish", +1), ("pivot", +1), ("easing", +1),
        ("rate hike", -1), ("hawkish", -1), ("tightening", -1), ("inflation", +1),
    ],
    "crypto_regulation": [
        ("etf approved", +1), ("sec approves", +1), ("bullish", +1),
        ("sec sues", -1), ("ban", -1), ("crackdown", -1), ("rejected", -1),
    ],
    "energy_commodities": [
        ("opec cut", +1), ("supply disruption", +1), ("pipeline", +1),
        ("oversupply", -1), ("production increase", -1), ("demand falls", -1),
    ],
    "us_politics": [
        ("trump wins", +1), ("election won", +1), ("landslide", +1),
        ("impeach", -1), ("indicted", -1), ("concedes", -1),
    ],
    "macro_economy": [
        ("recession", +1), ("gdp contracts", +1), ("unemployment rises", +1),
        ("strong growth", -1), ("gdp beats", -1), ("jobs added", -1),
    ],
}


@dataclass
class SentimentResult:
    """Output of the News Analyzer for a given whale move."""
    whale_move: WhaleMove
    confidence_score: float         # 0–100
    supporting_headlines: list[str]
    contradicting_headlines: list[str]
    category: str                   # Which category was matched
    recommendation: str             # "TRADE", "SKIP", "MONITOR"

    def __str__(self) -> str:
        return (
            f"[NewsAnalyzer] {self.whale_move.slug} | "
            f"Category: {self.category} | "
            f"Confidence: {self.confidence_score:.1f}/100 | "
            f"Rec: {self.recommendation} | "
            f"Support: {len(self.supporting_headlines)} | "
            f"Contra: {len(self.contradicting_headlines)}"
        )


class NewsAnalyzer:
    """
    Fetches recent news articles via NewsAPI and scores how well the
    current macro narrative supports a whale's position.

    Confidence scoring logic:
      - Base score: 40 (neutral)
      - Each supporting headline: +8 (max 3 counted)
      - Each contradicting headline: -10 (max 3 counted)
      - Whale bet size bonus: up to +10 for large bets (>$50k)
      - Category match: +5 if slug keywords match a known category
    """

    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        self._correlation_mapper = None  # Injected after init to avoid circular deps

    def set_mapper(self, mapper) -> None:
        self._correlation_mapper = mapper

    async def analyze(self, move: WhaleMove) -> SentimentResult:
        """
        Main entry point. Fetches news, scores sentiment, and returns a
        SentimentResult with a TRADE / SKIP / MONITOR recommendation.
        """
        category = self._classify_slug(move.slug)
        headlines = await self._fetch_headlines(move.slug, category)

        supporting, contradicting = self._score_headlines(headlines, category, move.outcome)

        score = self._compute_score(supporting, contradicting, move)

        if score >= config.MIN_CONFIDENCE_SCORE:
            recommendation = "TRADE"
        elif score >= 45:
            recommendation = "MONITOR"
        else:
            recommendation = "SKIP"

        result = SentimentResult(
            whale_move=move,
            confidence_score=score,
            supporting_headlines=supporting,
            contradicting_headlines=contradicting,
            category=category,
            recommendation=recommendation,
        )
        logger.info(str(result))
        return result

    # ── Internal ───────────────────────────────────────────────────────────────

    def _classify_slug(self, slug: str) -> str:
        """
        Map a Polymarket slug to the nearest category using:
        1. slug_keywords from mappings.json (most specific)
        2. KEYWORD_TAXONOMY sentiment keywords
        3. Category name words as fallback
        """
        slug_lower = slug.lower()

        # Priority 1: mappings.json slug_keywords (loaded via CorrelationMapper)
        if self._correlation_mapper:
            categories = self._correlation_mapper._mappings.get("categories", {})
            # Score each category by number of keyword hits
            scores: dict[str, int] = {}
            for cat_name, cat_data in categories.items():
                for kw in cat_data.get("slug_keywords", []):
                    if kw in slug_lower:
                        scores[cat_name] = scores.get(cat_name, 0) + 1
            if scores:
                return max(scores, key=lambda c: scores[c])

        # Priority 2: KEYWORD_TAXONOMY sentiment words
        for category in KEYWORD_TAXONOMY:
            keywords = [kw for kw, _ in KEYWORD_TAXONOMY[category]]
            for kw in keywords:
                if kw.replace(" ", "-") in slug_lower or kw in slug_lower:
                    return category

        # Priority 3: category name words
        for category in KEYWORD_TAXONOMY:
            if any(w in slug_lower for w in category.split("_")):
                return category

        return "geopolitics"

    async def _fetch_headlines(self, slug: str, category: str) -> list[str]:
        """
        Fetch recent headlines from Google News RSS (free, no API key required).
        Returns a list of headline strings.
        """
        slug_query = slug.replace("-", " ")
        category_terms = [kw for kw, _ in KEYWORD_TAXONOMY.get(category, [])][:3]
        query = f"{slug_query} {' '.join(category_terms)}"
        encoded = quote_plus(query)
        url = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"

        headers = {"User-Agent": "Mozilla/5.0 (compatible; TradingBot/1.0)"}
        try:
            async with self._session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                resp.raise_for_status()
                text = await resp.text()
                root = ET.fromstring(text)
                titles = [
                    item.findtext("title", "")
                    for item in root.findall(".//item")
                ]
                headlines = [t for t in titles if t][:10]
                logger.debug("[NewsAnalyzer] Google News returned %d headlines for '%s'", len(headlines), slug_query)
                return headlines
        except Exception as exc:
            logger.warning("Google News RSS fetch failed: %s", exc)
            return []

    def _score_headlines(
        self,
        headlines: list[str],
        category: str,
        outcome: str,
    ) -> tuple[list[str], list[str]]:
        """
        Split headlines into supporting vs contradicting based on keyword
        taxonomy and the whale's outcome direction (YES/NO).
        """
        supporting: list[str] = []
        contradicting: list[str] = []
        taxonomy = KEYWORD_TAXONOMY.get(category, [])

        for headline in headlines:
            h_lower = headline.lower()
            pos_hits = sum(1 for kw, sign in taxonomy if sign == +1 and kw in h_lower)
            neg_hits = sum(1 for kw, sign in taxonomy if sign == -1 and kw in h_lower)

            if outcome == "YES":
                if pos_hits > neg_hits:
                    supporting.append(headline)
                elif neg_hits > pos_hits:
                    contradicting.append(headline)
            else:  # NO bet — flip the alignment
                if neg_hits > pos_hits:
                    supporting.append(headline)
                elif pos_hits > neg_hits:
                    contradicting.append(headline)

        return supporting[:5], contradicting[:5]

    def _compute_score(
        self,
        supporting: list[str],
        contradicting: list[str],
        move: WhaleMove,
    ) -> float:
        """Compute a 0–100 confidence score."""
        score = 40.0  # Neutral baseline

        # News alignment
        score += min(len(supporting), 3) * 8.0
        score -= min(len(contradicting), 3) * 10.0

        # Whale bet size: large bets signal strong conviction
        if move.size_usdc >= 100_000:
            score += 10.0
        elif move.size_usdc >= 50_000:
            score += 7.0
        elif move.size_usdc >= 10_000:
            score += 4.0

        # Probability shift magnitude: bigger shift = stronger signal
        if move.prob_shift_pct >= 10.0:
            score += 10.0
        elif move.prob_shift_pct >= 5.0:
            score += 5.0

        # Category bonus: if we could classify the slug, we have higher certainty
        score += 5.0

        return max(0.0, min(100.0, score))
