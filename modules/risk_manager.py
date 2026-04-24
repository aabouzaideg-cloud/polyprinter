"""
risk_manager.py — Advanced Risk Management
Implements Kelly Criterion position sizing, per-trade max-risk caps,
daily drawdown circuit breakers, and spread slippage checks.
"""

import logging
from dataclasses import dataclass, field
from datetime import date

import config
from modules.correlation_mapper import TradeSignal

logger = logging.getLogger(__name__)


@dataclass
class SizedOrder:
    """A TradeSignal enriched with concrete position sizing."""
    signal: TradeSignal
    epic: str
    direction: str
    size: float             # Units to trade (notional / current_price)
    stop_distance: float    # Distance in price points for stop-loss
    limit_distance: float   # Distance in price points for take-profit
    risk_amount: float      # USD amount at risk on this trade
    approved: bool          # Passed all risk checks?
    rejection_reason: str = ""

    def __str__(self) -> str:
        status = "APPROVED" if self.approved else f"REJECTED ({self.rejection_reason})"
        return (
            f"[RiskManager] {status} | "
            f"{self.direction} {self.epic} | "
            f"Size: {self.size:.4f} | "
            f"Risk: ${self.risk_amount:.2f}"
        )


class RiskManager:
    """
    All risk computations live here. The execution engine must call
    `evaluate()` before placing any order and must respect the returned
    `SizedOrder.approved` flag.
    """

    def __init__(self):
        self._daily_pnl: float = 0.0          # Running P&L for today (USD)
        self._daily_pnl_date: date = date.today()
        self._open_risk: float = 0.0           # Total USD currently at risk

    # ── Public API ─────────────────────────────────────────────────────────────

    def evaluate(
        self,
        signal: TradeSignal,
        account_balance: float,
        current_price: float,
        spread: float,
    ) -> SizedOrder:
        """
        Given a TradeSignal and live market data, compute position size and
        run all safety checks. Returns a SizedOrder with approved=True/False.
        """
        self._reset_daily_pnl_if_new_day()

        # --- 1. Spread / slippage check ---
        spread_pct = (spread / current_price) * 100.0 if current_price > 0 else 999.0
        if spread_pct > config.MAX_SPREAD_PCT:
            return self._reject(signal, current_price,
                                f"Spread {spread_pct:.3f}% exceeds max {config.MAX_SPREAD_PCT}%")

        # --- 2. Daily drawdown circuit breaker ---
        drawdown_pct = (abs(self._daily_pnl) / account_balance) * 100.0 if account_balance > 0 else 0.0
        if self._daily_pnl < 0 and drawdown_pct >= config.MAX_DAILY_DRAWDOWN_PCT:
            return self._reject(signal, current_price,
                                f"Daily drawdown {drawdown_pct:.2f}% hit circuit breaker")

        # --- 3. Kelly Criterion position sizing ---
        kelly_fraction = self._kelly(signal.confidence_score)

        # Cap Kelly at the configured max risk %
        max_risk_fraction = config.MAX_RISK_PER_TRADE_PCT / 100.0
        risk_fraction = min(kelly_fraction, max_risk_fraction)

        risk_amount = account_balance * risk_fraction

        # --- 4. Derive position size ---
        # Risk amount = size * stop_distance
        # We use a fixed 1% stop below/above entry as default
        stop_distance = current_price * 0.01
        if stop_distance <= 0:
            return self._reject(signal, current_price, "Cannot compute stop distance (zero price)")

        size = risk_amount / stop_distance
        limit_distance = stop_distance * 2.0  # 2:1 reward/risk ratio

        logger.info(
            "[RiskManager] Kelly=%.3f | RiskFraction=%.3f | "
            "RiskAmt=$%.2f | Size=%.4f | Stop=%.4f pts",
            kelly_fraction, risk_fraction, risk_amount, size, stop_distance,
        )

        self._open_risk += risk_amount

        return SizedOrder(
            signal=signal,
            epic=signal.epic,
            direction=signal.direction,
            size=round(size, 4),
            stop_distance=round(stop_distance, 5),
            limit_distance=round(limit_distance, 5),
            risk_amount=round(risk_amount, 2),
            approved=True,
        )

    def record_trade_pnl(self, pnl: float, risk_amount: float) -> None:
        """Call this after a position closes to update daily P&L tracking."""
        self._daily_pnl += pnl
        self._open_risk = max(0.0, self._open_risk - risk_amount)
        logger.info(
            "[RiskManager] Trade closed | PnL: $%.2f | Daily PnL: $%.2f | Open Risk: $%.2f",
            pnl, self._daily_pnl, self._open_risk,
        )

    @property
    def daily_pnl(self) -> float:
        return self._daily_pnl

    # ── Internal ───────────────────────────────────────────────────────────────

    @staticmethod
    def _kelly(confidence_score: float) -> float:
        """
        Half-Kelly formula: f* = (p - q) / b  scaled to half for safety.

        confidence_score (0–100) is treated as the estimated win probability.
        We assume a fixed 2:1 payout (reward/risk), so b = 2.
        """
        p = confidence_score / 100.0   # win probability
        q = 1.0 - p                    # loss probability
        b = 2.0                        # reward-to-risk ratio

        if p <= 0 or q <= 0:
            return 0.0

        full_kelly = (b * p - q) / b
        half_kelly = full_kelly / 2.0  # Half-Kelly dampens volatility
        return max(0.0, half_kelly)

    def _reset_daily_pnl_if_new_day(self) -> None:
        today = date.today()
        if today != self._daily_pnl_date:
            logger.info("[RiskManager] New trading day — resetting daily P&L (was $%.2f).", self._daily_pnl)
            self._daily_pnl = 0.0
            self._daily_pnl_date = today

    @staticmethod
    def _reject(signal: TradeSignal, price: float, reason: str) -> SizedOrder:
        logger.warning("[RiskManager] REJECTED %s %s — %s", signal.direction, signal.epic, reason)
        return SizedOrder(
            signal=signal,
            epic=signal.epic,
            direction=signal.direction,
            size=0.0,
            stop_distance=0.0,
            limit_distance=0.0,
            risk_amount=0.0,
            approved=False,
            rejection_reason=reason,
        )
