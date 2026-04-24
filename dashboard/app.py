"""
dashboard/app.py — PolyPrinter Dashboard Backend
FastAPI server that streams live data from Capital.com and bot.log
to the frontend via Server-Sent Events (SSE).
"""

import asyncio
import json
import os
import re
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# Add parent dir to path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent))
import config

app = FastAPI(title="PolyPrinter")
app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")

BOT_LOG = Path(__file__).parent.parent / "bot.log"
LOG_BUFFER: deque = deque(maxlen=200)  # Keep last 200 log lines

# ── Capital.com helpers ───────────────────────────────────────────────────────

async def _capital_session() -> tuple[aiohttp.ClientSession, str, str]:
    session = aiohttp.ClientSession()
    async with session.post(
        f"{config.CAPITAL_BASE_URL}/api/v1/session",
        json={"identifier": config.CAPITAL_EMAIL,
              "password": config.CAPITAL_API_PASSWORD,
              "encryptedPassword": False},
        headers={"X-CAP-API-KEY": config.CAPITAL_API_KEY,
                 "Content-Type": "application/json"},
    ) as r:
        cst = r.headers.get("CST", "")
        sec = r.headers.get("X-SECURITY-TOKEN", "")
    return session, cst, sec


async def get_capital_data() -> dict:
    try:
        session, cst, sec = await _capital_session()
        headers = {"CST": cst, "X-SECURITY-TOKEN": sec}

        async with session.get(f"{config.CAPITAL_BASE_URL}/api/v1/accounts",
                               headers=headers) as r:
            acct_data = await r.json()

        async with session.get(f"{config.CAPITAL_BASE_URL}/api/v1/positions",
                               headers=headers) as r:
            pos_data = await r.json()

        await session.close()

        accounts  = acct_data.get("accounts", [{}])
        acct      = accounts[0] if accounts else {}
        bal       = acct.get("balance", {})
        positions = pos_data.get("positions", [])

        return {
            "balance":   round(bal.get("balance", 0), 2),
            "available": round(bal.get("available", 0), 2),
            "pnl":       round(bal.get("profitLoss", 0), 2),
            "deposit":   round(bal.get("deposit", 0), 2),
            "currency":  acct.get("currency", "AED"),
            "mode":      config.CAPITAL_MODE,
            "positions": [
                {
                    "epic":      p["market"]["epic"],
                    "name":      p["market"].get("instrumentName", p["market"]["epic"]),
                    "direction": p["position"]["direction"],
                    "size":      p["position"]["size"],
                    "entry":     p["position"].get("openLevel", 0),
                    "pnl":       round(p["position"].get("upl", 0), 2),
                    "currency":  p["position"].get("currency", "AED"),
                }
                for p in positions
            ],
        }
    except Exception as e:
        return {"error": str(e), "positions": [], "mode": config.CAPITAL_MODE}


# ── Log tail ──────────────────────────────────────────────────────────────────

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

def _strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text)

def _classify_log(line: str) -> str:
    if "ACCEPTED" in line or "Order placed" in line:
        return "trade"
    if "PASS" in line:
        return "pass"
    if "WHALE SIGNAL" in line:
        return "signal"
    if "ERROR" in line or "CRITICAL" in line:
        return "error"
    if "WARNING" in line:
        return "warning"
    if "PENDING" in line or "STALE" in line:
        return "pending"
    return "info"

def tail_log(n: int = 100) -> list[dict]:
    lines = []
    if BOT_LOG.exists():
        with open(BOT_LOG, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            buf  = bytearray()
            pos  = size
            while len(lines) < n and pos > 0:
                chunk = min(4096, pos)
                pos  -= chunk
                f.seek(pos)
                buf   = f.read(chunk) + buf
                lines = buf.decode("utf-8", errors="replace").splitlines()
        lines = lines[-n:]
    result = []
    for l in lines:
        clean = _strip_ansi(l).strip()
        if clean:
            result.append({"text": clean, "type": _classify_log(clean)})
    return result


# ── SSE stream ────────────────────────────────────────────────────────────────

async def event_stream():
    last_size = 0
    while True:
        try:
            capital = await get_capital_data()
            logs    = tail_log(100)

            payload = json.dumps({"capital": capital, "logs": logs,
                                  "ts": datetime.now(timezone.utc).isoformat(),
                                  "wallets": len(config.WHALE_WALLETS)})
            yield f"data: {payload}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

        await asyncio.sleep(5)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/stream")
async def stream():
    return StreamingResponse(event_stream(),
                             media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache",
                                      "X-Accel-Buffering": "no"})

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(html_path.read_text())


if __name__ == "__main__":
    uvicorn.run("dashboard.app:app", host="0.0.0.0", port=8080, reload=False)
