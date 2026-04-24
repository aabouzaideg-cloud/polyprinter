"""
config.py — Centralised configuration loaded from .env
All modules import from here; never read os.environ directly elsewhere.
"""

import os
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required environment variable '{key}' is not set. Check your .env file.")
    return val


def _float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, default))
    except ValueError:
        raise EnvironmentError(f"Environment variable '{key}' must be a number.")


# ── Capital.com ───────────────────────────────────────────────────────────────
CAPITAL_API_KEY: str = _require("CAPITAL_API_KEY")
CAPITAL_EMAIL: str   = _require("CAPITAL_EMAIL")
CAPITAL_PASSWORD: str = _require("CAPITAL_PASSWORD")
# Separate password set when creating the API key (used for session auth)
CAPITAL_API_PASSWORD: str = os.getenv("CAPITAL_API_PASSWORD") or CAPITAL_PASSWORD

CAPITAL_MODE: str = os.getenv("CAPITAL_MODE", "DEMO").upper()
if CAPITAL_MODE not in ("DEMO", "LIVE"):
    raise EnvironmentError("CAPITAL_MODE must be 'DEMO' or 'LIVE'.")

CAPITAL_BASE_URL: str = (
    "https://demo-api-capital.backend-capital.com"
    if CAPITAL_MODE == "DEMO"
    else "https://api-capital.backend-capital.com"
)

# ── Polymarket ────────────────────────────────────────────────────────────────
POLYMARKET_BASE_URL: str  = "https://clob.polymarket.com"
POLYMARKET_API_KEY: str   = os.getenv("POLYMARKET_API_KEY", "")

_raw_wallets = os.getenv("WHALE_WALLETS", "")
WHALE_WALLETS: list[str]  = [w.strip() for w in _raw_wallets.split(",") if w.strip()]

# ── News ──────────────────────────────────────────────────────────────────────
NEWS_API_KEY: str          = os.getenv("NEWS_API_KEY", "")
NEWS_API_BASE_URL: str     = "https://newsapi.org/v2"

# ── Bot Parameters ────────────────────────────────────────────────────────────
WHALE_TRIGGER_THRESHOLD: float  = _float("WHALE_TRIGGER_THRESHOLD", 3.0)
MAX_RISK_PER_TRADE_PCT: float   = _float("MAX_RISK_PER_TRADE_PCT", 2.0)
MAX_DAILY_DRAWDOWN_PCT: float   = _float("MAX_DAILY_DRAWDOWN_PCT", 5.0)
MAX_SPREAD_PCT: float           = _float("MAX_SPREAD_PCT", 0.1)
POLL_INTERVAL_SECONDS: int      = int(_float("POLL_INTERVAL_SECONDS", 30))
MIN_CONFIDENCE_SCORE: float     = _float("MIN_CONFIDENCE_SCORE", 60.0)

MAPPINGS_PATH: str = os.path.join(os.path.dirname(__file__), "data", "mappings.json")

# ── Massive.com WebSocket Market Data ────────────────────────────────────────
MASSIVE_API_KEY: str     = os.getenv("MASSIVE_API_KEY", "")
MASSIVE_WS_BASE: str     = "wss://socket.massive.com"

# ── Dune Analytics ────────────────────────────────────────────────────────────
DUNE_API_KEY: str = os.getenv("DUNE_API_KEY", "")
