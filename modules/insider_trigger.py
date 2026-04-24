"""
insider_trigger.py — The "Insider" Trigger Gate
Blocks trade execution unless a whale entered their position at least
4 hours BEFORE a significant news spike on the same topic.

Logic:
  - When a WhaleMove is detected, record its timestamp.
  - On each trade evaluation, fetch current Google News article count for the slug.
  - A "news spike" = sudden surge above baseline article count.
  - Gate: only PASS if (news_spike_time - whale_entry_time) >= 4 hours.
    i.e. the whale knew BEFORE the news broke → insider-like signal.

If no news spike is detected yet, the signal is held as PENDING.
If the whale entered AFTER the spike, the signal is STALE (skip).
"""

import asyncio
import logging
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus

import aiohttp

from modules.market_monitor import WhaleMove

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MIN_HOURS_BEFORE_NEWS = 4        # Whale must have entered this many hours before spike
SPIKE_THRESHOLD       = 3        # Min new articles in last 30 min to count as spike
BASELINE_WINDOW_MIN   = 60       # Minutes to establish article baseline
MAX_PENDING_HOURS     = 48       # Discard pending signals older than this


@dataclass
class NewsSnapshot:
    """Article count sample at a point in time."""
    timestamp: datetime
    count: int
    headlines: list[str] = field(default_factory=list)


@dataclass
class TriggerResult:
    whale_move: WhaleMove
    verdict: str            # "PASS", "PENDING", "STALE", "NO_SPIKE"
    reason: str
    hours_before_spike: float = 0.0

    @property
    def approved(self) -> bool:
        return self.verdict == "PASS"

    def __str__(self) -> str:
        return (
            f"[InsiderTrigger] {self.verdict} | {self.whale_move.slug} | "
            f"{self.reason}"
        )


class InsiderTrigger:
    """
    Tracks whale entry timestamps and news activity per market slug.
    Acts as a gate before any trade reaches the execution engine.
    """

    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        # slug → list of NewsSnapshot (rolling history)
        self._news_history: dict[str, list[NewsSnapshot]] = defaultdict(list)
        # slug → first detection time of a news spike
        self._spike_times: dict[str, datetime] = {}

    async def evaluate(self, move: WhaleMove) -> TriggerResult:
        """
        Main gate. Returns a TriggerResult with verdict PASS/PENDING/STALE/NO_SPIKE.
        """
        slug       = move.slug
        entry_time = move.timestamp

        # Step 1: Sample current news activity
        await self._sample_news(slug)

        # Step 2: Detect spike
        spike_time = self._detect_spike(slug)

        if spike_time is None:
            # No spike yet — hold as pending
            age_hours = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600
            if age_hours > MAX_PENDING_HOURS:
                result = TriggerResult(
                    whale_move=move,
                    verdict="STALE",
                    reason=f"Signal aged out after {age_hours:.1f}h with no news spike.",
                )
            else:
                result = TriggerResult(
                    whale_move=move,
                    verdict="PENDING",
                    reason=f"No news spike detected yet. Whale entered {age_hours:.1f}h ago. Watching...",
                )
            logger.info(str(result))
            return result

        # Step 3: Calculate lead time
        lead_seconds = (spike_time - entry_time).total_seconds()
        lead_hours   = lead_seconds / 3600

        if lead_hours >= MIN_HOURS_BEFORE_NEWS:
            result = TriggerResult(
                whale_move=move,
                verdict="PASS",
                reason=f"Whale entered {lead_hours:.1f}h BEFORE news spike — insider signal confirmed.",
                hours_before_spike=lead_hours,
            )
        elif lead_hours >= 0:
            result = TriggerResult(
                whale_move=move,
                verdict="PENDING",
                reason=f"Whale entered {lead_hours:.1f}h before spike (need {MIN_HOURS_BEFORE_NEWS}h). Too close.",
                hours_before_spike=lead_hours,
            )
        else:
            result = TriggerResult(
                whale_move=move,
                verdict="STALE",
                reason=f"Whale entered {abs(lead_hours):.1f}h AFTER news spike — reactive, not insider.",
                hours_before_spike=lead_hours,
            )

        logger.info(str(result))
        return result

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _sample_news(self, slug: str) -> None:
        """Fetch current article count for a slug and append to history."""
        query   = slug.replace("-", " ")
        encoded = quote_plus(query)
        url     = f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"
        headers = {"User-Agent": "Mozilla/5.0 (compatible; TradingBot/1.0)"}

        try:
            async with self._session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                resp.raise_for_status()
                text = await resp.text()
                root = ET.fromstring(text)
                items    = root.findall(".//item")
                titles   = [i.findtext("title", "") for i in items]
                snapshot = NewsSnapshot(
                    timestamp=datetime.now(timezone.utc),
                    count=len(items),
                    headlines=titles[:5],
                )
                history = self._news_history[slug]
                history.append(snapshot)
                # Keep last 48 samples (48 poll cycles = 24h at 30s interval)
                self._news_history[slug] = history[-48:]
        except Exception as e:
            logger.debug("[InsiderTrigger] News sample failed for '%s': %s", slug, e)

    def _detect_spike(self, slug: str) -> datetime | None:
        """
        Detect if a news spike has occurred for this slug.
        Spike = article count in the latest sample exceeds the rolling
        baseline by SPIKE_THRESHOLD or more.

        Returns the estimated spike time, or None if no spike detected.
        """
        # Return cached spike time if already detected
        if slug in self._spike_times:
            return self._spike_times[slug]

        history = self._news_history.get(slug, [])
        if len(history) < 2:
            return None

        latest  = history[-1]
        # Baseline = average of all but the last sample
        baseline_counts = [s.count for s in history[:-1]]
        baseline_avg    = sum(baseline_counts) / len(baseline_counts)

        surge = latest.count - baseline_avg

        if surge >= SPIKE_THRESHOLD:
            spike_time = latest.timestamp
            self._spike_times[slug] = spike_time
            logger.info(
                "[InsiderTrigger] NEWS SPIKE detected for '%s' at %s "
                "(articles: %d vs baseline %.1f, surge: +%.1f)",
                slug, spike_time.strftime("%H:%M:%S"),
                latest.count, baseline_avg, surge,
            )
            if latest.headlines:
                for h in latest.headlines[:3]:
                    logger.info("  → %s", h)
            return spike_time

        return None
