"""
order_executor.py — Capital.com Execution Engine
Manages the Capital.com REST API session lifecycle and executes
directional trades based on approved SizedOrders.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

import aiohttp

import config
from modules.risk_manager import SizedOrder

logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    """Result of a single order submission to Capital.com."""
    deal_reference: str
    epic: str
    direction: str
    size: float
    status: str         # "ACCEPTED", "REJECTED", "ERROR"
    message: str
    deal_id: str = ""

    def __str__(self) -> str:
        return (
            f"[Executor] {self.status} | {self.direction} {self.epic} "
            f"x{self.size} | Ref: {self.deal_reference} | {self.message}"
        )


class OrderExecutor:
    """
    Wraps the Capital.com REST API.

    Session management:
      - Authenticates on first use (lazy init).
      - Re-authenticates automatically on 401 responses.
      - Deletes the session on graceful shutdown.

    Usage:
        async with OrderExecutor() as executor:
            result = await executor.execute(sized_order)
    """

    def __init__(self, session: aiohttp.ClientSession):
        self._http = session
        self._cst_token: Optional[str] = None       # Capital.com CST header
        self._security_token: Optional[str] = None  # Capital.com X-SECURITY-TOKEN
        self._account_id: Optional[str] = None

    # ── Context manager ────────────────────────────────────────────────────────

    async def __aenter__(self):
        await self._authenticate()
        return self

    async def __aexit__(self, *_):
        await self._logout()

    # ── Public API ─────────────────────────────────────────────────────────────

    async def get_open_epics(self) -> set[str]:
        """Return the set of Epic IDs that already have an open position."""
        await self._ensure_authenticated()
        url = f"{config.CAPITAL_BASE_URL}/api/v1/positions"
        try:
            async with self._http.get(url, headers=self._auth_headers()) as resp:
                resp.raise_for_status()
                data = await resp.json()
                positions = data.get("positions", [])
                return {p["market"]["epic"] for p in positions if "market" in p}
        except Exception as e:
            logger.error("Failed to fetch open positions: %s", e)
            return set()

    async def get_account_balance(self) -> float:
        """Return current account balance in USD."""
        await self._ensure_authenticated()
        url = f"{config.CAPITAL_BASE_URL}/api/v1/accounts"
        try:
            async with self._http.get(url, headers=self._auth_headers()) as resp:
                resp.raise_for_status()
                data = await resp.json()
                accounts = data.get("accounts", [])
                if accounts:
                    balance = accounts[0].get("balance", {}).get("available", 0.0)
                    logger.debug("Account balance: $%.2f", balance)
                    return float(balance)
        except Exception as e:
            logger.error("Failed to fetch account balance: %s", e)
        return 0.0

    async def get_market_snapshot(self, epic: str) -> dict:
        """
        Fetch live bid/ask/mid price for an epic.
        Returns dict with keys: bid, ask, mid, spread
        """
        await self._ensure_authenticated()
        url = f"{config.CAPITAL_BASE_URL}/api/v1/markets/{epic}"
        try:
            async with self._http.get(url, headers=self._auth_headers()) as resp:
                resp.raise_for_status()
                data = await resp.json()
                instrument = data.get("instrument", {})
                snapshot = data.get("snapshot", {})
                bid    = float(snapshot.get("bid", 0))
                offer  = float(snapshot.get("offer", 0))
                mid    = (bid + offer) / 2.0
                spread = offer - bid
                return {"bid": bid, "ask": offer, "mid": mid, "spread": spread, "epic": epic}
        except Exception as e:
            logger.error("Failed to fetch market snapshot for %s: %s", epic, e)
            return {"bid": 0, "ask": 0, "mid": 0, "spread": 999, "epic": epic}

    async def execute(self, order: SizedOrder) -> ExecutionResult:
        """
        Submit an approved SizedOrder to Capital.com as a market order
        with an attached stop-loss and take-profit.
        """
        if not order.approved:
            return ExecutionResult(
                deal_reference="N/A",
                epic=order.epic,
                direction=order.direction,
                size=order.size,
                status="REJECTED",
                message=f"Risk check failed: {order.rejection_reason}",
            )

        await self._ensure_authenticated()

        payload = {
            "epic": order.epic,
            "direction": order.direction,
            "size": str(order.size),
            "orderType": "MARKET",
            "stopDistance": str(order.stop_distance),
            "profitDistance": str(order.limit_distance),
            "guaranteedStop": False,
            "forceOpen": True,
        }

        logger.info(
            "[Executor] Submitting order: %s %s x%s | Stop: %s | Limit: %s | Mode: %s",
            order.direction, order.epic, order.size,
            order.stop_distance, order.limit_distance,
            config.CAPITAL_MODE,
        )

        url = f"{config.CAPITAL_BASE_URL}/api/v1/positions"
        try:
            async with self._http.post(
                url,
                json=payload,
                headers=self._auth_headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()

                if resp.status == 200:
                    ref = data.get("dealReference", "UNKNOWN")
                    logger.info("[Executor] Order ACCEPTED — DealRef: %s", ref)
                    return ExecutionResult(
                        deal_reference=ref,
                        epic=order.epic,
                        direction=order.direction,
                        size=order.size,
                        status="ACCEPTED",
                        message="Order placed successfully.",
                    )
                elif resp.status == 401:
                    logger.warning("[Executor] 401 Unauthorised — re-authenticating...")
                    await self._authenticate()
                    return await self.execute(order)  # Retry once
                else:
                    reason = data.get("errorCode", str(resp.status))
                    logger.error("[Executor] Order REJECTED by Capital.com: %s", reason)
                    return ExecutionResult(
                        deal_reference="N/A",
                        epic=order.epic,
                        direction=order.direction,
                        size=order.size,
                        status="REJECTED",
                        message=reason,
                    )
        except aiohttp.ClientError as e:
            logger.exception("[Executor] Network error during order submission: %s", e)
            return ExecutionResult(
                deal_reference="N/A",
                epic=order.epic,
                direction=order.direction,
                size=order.size,
                status="ERROR",
                message=str(e),
            )

    # ── Session Management ─────────────────────────────────────────────────────

    async def _authenticate(self) -> None:
        """POST /api/v1/session — obtain CST and X-SECURITY-TOKEN."""
        url = f"{config.CAPITAL_BASE_URL}/api/v1/session"
        payload = {
            "identifier": config.CAPITAL_EMAIL,
            "password": config.CAPITAL_API_PASSWORD,
            "encryptedPassword": False,
        }
        headers = {
            "X-CAP-API-KEY": config.CAPITAL_API_KEY,
            "Content-Type": "application/json",
        }
        try:
            async with self._http.post(url, json=payload, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=15)) as resp:
                resp.raise_for_status()
                self._cst_token      = resp.headers.get("CST")
                self._security_token = resp.headers.get("X-SECURITY-TOKEN")
                data = await resp.json()
                self._account_id = data.get("currentAccountId", "")
                logger.info(
                    "[Executor] Authenticated with Capital.com (%s) | Account: %s",
                    config.CAPITAL_MODE, self._account_id,
                )
        except Exception as e:
            logger.critical("[Executor] Authentication failed: %s", e)
            raise

    async def _logout(self) -> None:
        """DELETE /api/v1/session — clean up the session token."""
        if not self._cst_token:
            return
        url = f"{config.CAPITAL_BASE_URL}/api/v1/session"
        try:
            async with self._http.delete(url, headers=self._auth_headers(),
                                         timeout=aiohttp.ClientTimeout(total=10)) as resp:
                logger.info("[Executor] Session closed (status %s).", resp.status)
        except Exception as e:
            logger.warning("[Executor] Failed to close session cleanly: %s", e)
        finally:
            self._cst_token = None
            self._security_token = None

    async def _ensure_authenticated(self) -> None:
        if not self._cst_token or not self._security_token:
            await self._authenticate()

    def _auth_headers(self) -> dict:
        return {
            "CST": self._cst_token or "",
            "X-SECURITY-TOKEN": self._security_token or "",
            "Content-Type": "application/json",
        }
