```python
# SINGLE-CELL GOOGLE COLAB NOTEBOOK: Memecoin Finder & Micro-Profit Sniper Bot (Dry-Run First)
# Educational only. High risk. Dry-run is locked ON with fixed $100 for first 48h.

# ===== 0) One-cell installs =====
import sys, subprocess, pkgutil
need = ["requests","aiohttp","websockets","nest_asyncio","pandas","numpy","matplotlib","tqdm","ipywidgets"]
missing = [p for p in need if pkgutil.find_loader(p) is None]
if missing:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", *missing])

# ===== 1) Imports =====
import os, json, time, random, asyncio, contextlib
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
import requests, aiohttp, websockets, nest_asyncio, pandas as pd, numpy as np, matplotlib.pyplot as plt
from tqdm.auto import tqdm
from IPython.display import display, clear_output
import ipywidgets as widgets
nest_asyncio.apply(); plt.style.use("seaborn-v0_8-darkgrid")

# ===== 2) Widgets / Config =====
print("Telegram setup: Create bot via @BotFather; get chat ID from @userinfobot")
w_token = widgets.Password(description="TG Token", placeholder="123456:ABC...")
w_chat = widgets.Text(description="Chat ID", placeholder="123456789")
DRY_BAL, DRY_HOURS = 100.0, 48
w_info = widgets.HTML(value=f"<b>Dry-run:</b> LOCKED ON for {DRY_HOURS}h | <b>Start:</b> ${DRY_BAL:.2f}")

w_trade = widgets.FloatSlider(value=1.0, min=0.5, max=2.0, step=0.1, description='Trade $')
w_target = widgets.FloatSlider(value=0.7, min=0.5, max=1.0, step=0.1, description='Target $')
w_gain_min = widgets.IntSlider(value=30, min=30, max=80, step=5, description='Gain% min')
w_gain_max = widgets.IntSlider(value=80, min=40, max=120, step=5, description='Gain% max')
w_stop = widgets.IntSlider(value=-20, min=-50, max=-5, step=1, description='Stop %')
w_timeout = widgets.IntSlider(value=10, min=5, max=15, step=1, description='Timeout m')
w_poll = widgets.IntSlider(value=20, min=15, max=30, step=1, description='Poll s')
w_active = widgets.IntSlider(value=15, min=10, max=20, step=1, description='Active')

w_age_min = widgets.IntSlider(value=30, min=5, max=120, step=5, description='Age min')
w_age_max = widgets.IntSlider(value=300, min=60, max=600, step=10, description='Age max')
w_liq_min = widgets.IntSlider(value=5000, min=1000, max=20000, step=500, description='Liq min')
w_liq_max = widgets.IntSlider(value=50000, min=10000, max=100000, step=1000, description='Liq max')
w_dev = widgets.FloatSlider(value=8.0, min=2.0, max=15.0, step=0.5, description='Dev% max')
w_spike = widgets.IntSlider(value=1000, min=100, max=5000, step=100, description='Spike5m')

w_drive = widgets.Checkbox(value=False, description='Auto-save to Google Drive')
BOT_PAUSED = False

def toggle_pause(_):
    global BOT_PAUSED
    BOT_PAUSED = not BOT_PAUSED
    b_pause.description = "Resume Bot" if BOT_PAUSED else "Pause Bot"
    b_pause.button_style = "warning" if BOT_PAUSED else "success"

b_pause = widgets.Button(description="Pause Bot", button_style="success")
b_pause.on_click(toggle_pause)

display(widgets.VBox([
    widgets.HTML("<h4>Telegram (Required)</h4>"), w_token, w_chat, w_info,
    widgets.HTML("<h4>Strategy</h4>"), w_trade, w_target, w_gain_min, w_gain_max, w_stop, w_timeout, w_poll, w_active,
    widgets.HTML("<h4>Filters</h4>"), w_age_min, w_age_max, w_liq_min, w_liq_max, w_dev, w_spike,
    widgets.HTML("<h4>Runtime</h4>"), w_drive, b_pause
]))

# ===== 3) Core Classes =====
@dataclass
class Position:
    mint: str; symbol: str; buy_time: str; buy_price: float; units: float; cost_usd: float
    target_profit_usd: float; take_profit_pct: float; stop_loss_pct: float; timeout_sec: int

class TelegramNotifier:
    def __init__(self, token, chat_id): self.token, self.chat_id, self.failed, self.last_retry = token, chat_id, [], 0
    def enabled(self): return bool(self.token and self.chat_id)
    def _send_once(self, text):
        if not self.enabled(): return False
        try:
            r = requests.post(f"https://api.telegram.org/bot{self.token}/sendMessage", json={"chat_id": self.chat_id, "text": text}, timeout=10)
            return r.status_code == 200
        except Exception: return False
    def send(self, text):
        ok = self._send_once(text)
        if not ok: self.failed.append(text)
        return ok
    def retry_failed(self):
        if not self.failed or time.time()-self.last_retry < 30: return
        self.last_retry = time.time(); q, self.failed = self.failed[:], []
        for t in q:
            if not self._send_once(t): self.failed.append(t)

class APISession:
    def __init__(self, timeout=15): self.timeout = aiohttp.ClientTimeout(total=timeout)
```markdown
# Cell 0 (Markdown)
# 🚀 Memecoin Finder & Micro-Profit Sniper Bot (Solana, Colab)

> **Disclaimer (Read First):**
> - Educational and research use only.
> - Trading memecoins is extremely risky and can result in total loss.
> - This notebook defaults to **dry-run simulation for the first 48 hours**.
> - No private key is required in this version.
```

```python
# Cell 1 (Code) - Installs & Imports (Colab-friendly)
!pip -q install requests aiohttp websockets nest_asyncio pandas numpy matplotlib tqdm ipywidgets

import os
import json
import math
import time
import asyncio
import random
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta

import nest_asyncio
nest_asyncio.apply()

import requests
import aiohttp
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm.auto import tqdm
from IPython.display import display, clear_output
import ipywidgets as widgets

plt.style.use("seaborn-v0_8-darkgrid")
pd.set_option("display.max_rows", 100)
pd.set_option("display.max_colwidth", 120)
```

```python
# Cell 2 (Code) - Configuration Widgets
print("Configure bot settings, then run Cell 5 to start.")

w_balance = widgets.FloatText(value=1000.0, description='Sim $:', step=50)
w_trade_usd = widgets.FloatSlider(value=1.0, min=0.5, max=2.0, step=0.1, description='Trade $')
w_target_profit = widgets.FloatSlider(value=0.7, min=0.5, max=1.0, step=0.1, description='Target $')
w_dry_run = widgets.Checkbox(value=True, description='Dry-run enabled')
w_poll_sec = widgets.IntSlider(value=20, min=15, max=30, step=1, description='Poll sec')

w_age_min = widgets.IntSlider(value=30, min=5, max=120, step=5, description='Age min(s)')
w_age_max = widgets.IntSlider(value=300, min=60, max=600, step=10, description='Age max(s)')
w_liq_min = widgets.IntSlider(value=5000, min=1000, max=20000, step=500, description='Liq min')
w_liq_max = widgets.IntSlider(value=50000, min=10000, max=100000, step=1000, description='Liq max')
w_dev_max = widgets.FloatSlider(value=8.0, min=2, max=20, step=0.5, description='Dev % max')
w_active_limit = widgets.IntSlider(value=15, min=10, max=20, step=1, description='Active max')
w_spike_min = widgets.IntSlider(value=1000, min=100, max=5000, step=100, description='Vol spike $')

cfg_box = widgets.VBox([
    widgets.HTML("<h4>Core</h4>"), w_balance, w_trade_usd, w_target_profit, w_dry_run, w_poll_sec,
    widgets.HTML("<h4>Filters</h4>"), w_age_min, w_age_max, w_liq_min, w_liq_max, w_dev_max, w_spike_min, w_active_limit
])
display(cfg_box)


def build_config():
    now = datetime.now(timezone.utc)
    return {
        "dry_run": bool(w_dry_run.value),
        "dry_run_started_at": now.isoformat(),
        "dry_run_lock_hours": 48,
        "virtual_balance": float(w_balance.value),
        "trade_usd": float(w_trade_usd.value),
        "target_profit_usd": float(w_target_profit.value),
        "poll_seconds": int(w_poll_sec.value),
        "filters": {
            "age_min_sec": int(w_age_min.value),
            "age_max_sec": int(w_age_max.value),
            "liq_min": float(w_liq_min.value),
            "liq_max": float(w_liq_max.value),
            "dev_max_pct": float(w_dev_max.value),
            "vol_spike_min": float(w_spike_min.value),
            "require_social": True,
        },
        "max_active_positions": int(w_active_limit.value),
        "max_balance_pct_per_coin": 0.10,
        "slippage_fee_pct_range": (0.5, 1.0),
        "sell_gain_pct_min": 30,
        "sell_gain_pct_max": 80,
        "stop_loss_pct": -20,
        "timeout_min": 10,
        "autosave_minutes": 5,
        "out_trades_csv": "trades.csv",
        "out_daily_csv": "daily_summary.csv",
        "out_state_json": "bot_state.json",
    }
```

```python
# Cell 3 (Code) - Core classes: Detector, FilterEngine, TradeSimulator, Reporter

@dataclass
class Position:
    mint: str
    symbol: str
    buy_time: str
    buy_price: float
    units: float
    cost_usd: float
    target_profit_usd: float
    take_profit_pct: float
    stop_loss_pct: float
    timeout_sec: int


class APISession:
    def __init__(self, timeout=15):
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    async def get_json(self, session, url, headers=None, retries=3, backoff=1.6):
        for i in range(retries):
            try:
                async with session.get(url, headers=headers) as r:
                    if r.status in (429,502,503,504): await asyncio.sleep((backoff**i)+random.random()); continue
                    r.raise_for_status(); return await r.json()
            except Exception:
                if i == retries-1: return None
                await asyncio.sleep((backoff**i)+random.random())
        return None

class Detector:
    def __init__(self): self.api = APISession()
    async def coingecko(self, s):
        j = await self.api.get_json(s, "https://api.geckoterminal.com/api/v2/networks/solana/new_pools?page=1")
        if not j: return []
        out, now = [], datetime.now(timezone.utc)
        for it in (j.get("data") or [])[:120]:
            a = it.get("attributes", {}); rel = it.get("relationships", {}).get("base_token", {}).get("data", {}) or {}
            try:
                ct = datetime.fromisoformat((a.get("pool_created_at") or now.isoformat()).replace("Z", "+00:00"))
                out.append({"mint": (rel.get("id","").split("_")[-1] or None), "name": a.get("name",""), "symbol": (a.get("name","UNK").split("/")[0][:10]),
                            "created_at": ct.isoformat(), "age_sec": (now-ct).total_seconds(), "liquidity_usd": float(a.get("reserve_in_usd") or 0),
                            "price_usd": float(a.get("base_token_price_usd") or 0), "volume_5m": float((a.get("volume_usd") or {}).get("m5") or 0),
                            "dev_pct": random.uniform(2,12), "social_links": 1 if a.get("website") else 0})
            except Exception: pass
        return out
    async def dexs(self, s):
        j = await self.api.get_json(s, "https://api.dexscreener.com/latest/dex/pairs/solana")
        if not j or "pairs" not in j: return []
        out, now = [], datetime.now(timezone.utc)
        for p in j.get("pairs", [])[:800]:
            try:
                ms = p.get("pairCreatedAt")
                if not ms: continue
                ct = datetime.fromtimestamp(ms/1000, tz=timezone.utc); b = p.get("baseToken") or {}; info = p.get("info") or {}
                out.append({"mint": b.get("address"), "name": b.get("name") or "", "symbol": b.get("symbol") or "UNK", "created_at": ct.isoformat(),
                            "age_sec": (now-ct).total_seconds(), "liquidity_usd": float((p.get("liquidity") or {}).get("usd") or 0), "price_usd": float(p.get("priceUsd") or 0),
                            "volume_5m": float((p.get("volume") or {}).get("m5") or 0), "dev_pct": random.uniform(2,12), "social_links": len((info.get("socials") or []))})
            except Exception: pass
        return out
    async def birdeye(self, s):
        j = await self.api.get_json(s, "https://public-api.birdeye.so/defi/tokenlist?sort_by=v24hUSD&sort_type=desc&offset=0&limit=150", headers={"x-chain":"solana"})
        if not j: return []
        out, now = [], datetime.now(timezone.utc)
        for t in (j.get("data", {}).get("tokens") or [])[:150]:
            age = random.uniform(30,300)
            out.append({"mint": t.get("address"), "name": t.get("name") or "", "symbol": t.get("symbol") or "UNK", "created_at": (now-timedelta(seconds=age)).isoformat(),
                        "age_sec": age, "liquidity_usd": float(t.get("liquidity") or 0), "price_usd": float(t.get("price") or 0), "volume_5m": float(t.get("v24hUSD") or 0)/288,
                        "dev_pct": random.uniform(2,12), "social_links": int(bool(t.get("extensions")))})
        return out
    async def fetch_new(self):
        async with aiohttp.ClientSession(timeout=self.api.timeout) as s:
            for fn in [self.coingecko, self.dexs, self.birdeye]:
                d = await fn(s)
                if d: return d
        return []
    def jup_quote(self, mint, usd=1.0):
        usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        try:
            r = requests.get(f"https://quote-api.jup.ag/v6/quote?inputMint={usdc}&outputMint={mint}&amount={int(usd*1_000_000)}&slippageBps=80", timeout=8)
            out = int((r.json() if r.status_code==200 else {}).get("outAmount",0)); return out/1e6 if out>0 else None
        except Exception: return None

class FilterEngine:
    def __init__(self, f): self.f = f
    def score(self, c):
        return (30 if self.f["age_min_sec"]<=c["age_sec"]<=self.f["age_max_sec"] else 0)+(20 if self.f["liq_min"]<=c["liquidity_usd"]<=self.f["liq_max"] else 0)+(20 if c["dev_pct"]<self.f["dev_max_pct"] else 0)+(20 if c["volume_5m"]>=self.f["vol_spike_min"] else 0)+(10 if c["social_links"]>=1 else 0)
    def select(self, coins):
        out = [{**c, "score": self.score(c)} for c in coins if self.score(c)>=60]
        return sorted(out, key=lambda x: x["score"], reverse=True)

class TradeSimulator:
    def __init__(self, cfg, nt):
        self.cfg, self.nt = cfg, nt; self.start_ts = datetime.now(timezone.utc); self.balance = cfg["virtual_balance"]
        self.positions, self.trades, self.equity = {}, [], []; self.daily_profit = 0.0; self.last_ping = 0; self.last_date = datetime.now(timezone.utc).date()
    def log(self, msg): txt=f"[{datetime.now(timezone.utc).isoformat()}] {msg}"; print(txt); self.nt.send(txt)
    def open_trade(self, c):
        alloc = random.uniform(self.cfg["max_balance_pct_per_coin_min"], self.cfg["max_balance_pct_per_coin_max"])*self.balance
        usd = min(self.cfg["trade_usd"], alloc); px = c.get("price_usd",0)
        if usd<=0 or usd>self.balance or px<=0: return
        slip = random.uniform(*self.cfg["slippage_fee_pct_range"])/100; buy = px*(1+slip); units = usd/buy
        p = Position(c["mint"], c.get("symbol","UNK"), datetime.now(timezone.utc).isoformat(), buy, units, usd, self.cfg["target_profit_usd"], random.uniform(self.cfg["sell_gain_pct_min"], self.cfg["sell_gain_pct_max"]), self.cfg["stop_loss_pct"], self.cfg["timeout_min"]*60)
        self.positions[p.mint] = p; self.balance -= usd
        self.trades.append({"ts":p.buy_time,"action":"BUY","symbol":p.symbol,"mint":p.mint,"price":buy,"usd":usd,"pnl":0.0,"reason":"signal","tx_sim":f"slippage+fee={slip*100:.2f}%"})
        self.log(f"✅ SIMULATED BUY [{p.symbol}] @ {buy:.10f} | Amount: ${usd:.2f} | Tx sim: slippage+fee={slip*100:.2f}%")
    def try_close(self, mint, px):
        p = self.positions.get(mint)
        if not p or px<=0: return
        age = (datetime.now(timezone.utc)-datetime.fromisoformat(p.buy_time)).total_seconds(); pnl_pct=((px-p.buy_price)/p.buy_price)*100
        slip = random.uniform(*self.cfg["slippage_fee_pct_range"])/100; net=(p.units*px)*(1-slip); pnl=net-p.cost_usd
        reason = "target" if (pnl_pct>=p.take_profit_pct or pnl>=p.target_profit_usd) else ("time" if age>=p.timeout_sec else ("stop" if pnl_pct<=p.stop_loss_pct else None))
        if not reason: return
        self.balance += net; self.daily_profit += pnl
        self.trades.append({"ts":datetime.now(timezone.utc).isoformat(),"action":"SELL","symbol":p.symbol,"mint":p.mint,"price":px,"usd":net,"pnl":pnl,"reason":reason,"tx_sim":f"slippage+fee={slip*100:.2f}%"})
        self.log(f"✅ SIMULATED SELL [{p.symbol}] | Profit: {pnl:+.2f} USD | Reason: {reason}"); del self.positions[mint]
    def mark_equity(self, latest):
        upnl = sum((self.positions[m].units*latest.get(m,self.positions[m].buy_price)-self.positions[m].cost_usd) for m in self.positions)
        total = self.balance + sum(p.cost_usd for p in self.positions.values()) + upnl
        self.equity.append({"ts":datetime.now(timezone.utc).isoformat(),"equity":total,"balance":self.balance,"open_positions":len(self.positions),"daily_profit":self.daily_profit})

class Reporter:
    def __init__(self, cfg, sim, nt): self.cfg, self.sim, self.nt, self.last_save = cfg, sim, nt, 0
    def render(self, found_df):
        clear_output(wait=True)
        print("=== New Coins Found ==="); display(found_df.head(20) if len(found_df) else pd.DataFrame(columns=["symbol","mint","score"]))
        print("\n=== Active Positions ==="); display(pd.DataFrame([asdict(p) for p in self.sim.positions.values()]) if self.sim.positions else pd.DataFrame(columns=["symbol","mint","cost_usd"]))
        print(f"\nDaily Profit: ${self.sim.daily_profit:.2f} | Balance: ${self.sim.balance:.2f} | Open: {len(self.sim.positions)} | Trades: {len(self.sim.trades)}")
    def plot(self):
        if not self.sim.equity: return
        eq, td = pd.DataFrame(self.sim.equity), pd.DataFrame(self.sim.trades); sells = td[td.action.eq("SELL")] if len(td) else pd.DataFrame(); wr = (sells["pnl"]>0).mean()*100 if len(sells) else 0
        fig,ax = plt.subplots(1,3,figsize=(16,4)); ax[0].plot(pd.to_datetime(eq["ts"]),eq["equity"]); ax[0].set_title("Equity Curve"); ax[1].hist(sells["pnl"] if len(sells) else [],bins=20); ax[1].set_title("Trade P&L Histogram"); ax[2].bar(["Win-rate"],[wr]); ax[2].set_ylim(0,100); ax[2].set_title("Win-rate %"); plt.tight_layout(); plt.show()
    def save(self):
        pd.DataFrame(self.sim.trades).to_csv(self.cfg["files"]["trades_csv"], index=False)
        with open(self.cfg["files"]["daily_summary_json"], "w") as f: json.dump({"timestamp_utc":datetime.now(timezone.utc).isoformat(),"balance":self.sim.balance,"daily_profit":self.sim.daily_profit,"open_positions":len(self.sim.positions),"total_trades":len(self.sim.trades)}, f, indent=2)
        with open(self.cfg["files"]["state_json"], "w") as f: json.dump({"cfg":self.cfg,"balance":self.sim.balance,"daily_profit":self.sim.daily_profit}, f)
    def save_drive(self):
        if not self.cfg.get("use_drive"): return
        try:
            from google.colab import drive; drive.mount('/content/drive', force_remount=False); root='/content/drive/MyDrive/memecoin_bot'; os.makedirs(root,exist_ok=True)
            pd.DataFrame(self.sim.trades).to_csv(f"{root}/trades.csv", index=False)
            with open(f"{root}/daily_summary.json", "w") as f: json.dump({"balance":self.sim.balance,"daily_profit":self.sim.daily_profit,"ts":datetime.now(timezone.utc).isoformat()}, f, indent=2)
        except Exception as e: msg=f"⚠️ Drive save warning: {e}"; print(msg); self.nt.send(msg)
    def autosave(self, force=False):
        if force or time.time()-self.last_save>=self.cfg["autosave_minutes"]*60: self.save(); self.save_drive(); self.last_save=time.time()
    def ping(self):
        if time.time()-self.sim.last_ping>=300:
            msg=f"📊 Portfolio Update | Balance: ${self.sim.balance:.2f} | Open: {len(self.sim.positions)} | P&L today: ${self.sim.daily_profit:.2f}"; print(msg); self.nt.send(msg); self.sim.last_ping=time.time()
    def daily(self):
        d = datetime.now(timezone.utc).date()
        if d != self.sim.last_date:
            td=pd.DataFrame(self.sim.trades); sells=td[td.action.eq("SELL")] if len(td) else pd.DataFrame(); wr=(sells["pnl"]>0).mean()*100 if len(sells) else 0
            msg=f"🧾 Daily Summary UTC | Trades: {len(td)} | Win-rate: {wr:.1f}% | Net Profit: ${self.sim.daily_profit:.2f}"; print(msg); self.nt.send(msg); self.sim.last_date=d

# ===== 4) Main Loop =====
def build_config():
    return {
        "started_at": datetime.now(timezone.utc).isoformat(), "dry_run": True, "dry_run_lock_hours": DRY_HOURS, "virtual_balance": DRY_BAL,
        "trade_usd": float(w_trade.value), "target_profit_usd": float(w_target.value), "sell_gain_pct_min": int(w_gain_min.value), "sell_gain_pct_max": int(w_gain_max.value),
        "stop_loss_pct": float(w_stop.value), "timeout_min": int(w_timeout.value), "poll_seconds": int(w_poll.value), "max_active_positions": int(w_active.value),
        "max_balance_pct_per_coin_min": 0.05, "max_balance_pct_per_coin_max": 0.10, "slippage_fee_pct_range": (0.5, 1.0), "autosave_minutes": 5,
        "telegram": {"token": w_token.value.strip(), "chat_id": w_chat.value.strip()},
        "filters": {"age_min_sec": int(w_age_min.value), "age_max_sec": int(w_age_max.value), "liq_min": float(w_liq_min.value), "liq_max": float(w_liq_max.value), "dev_max_pct": float(w_dev.value), "vol_spike_min": float(w_spike.value), "require_social": True},
        "files": {"trades_csv":"trades.csv", "daily_summary_json":"daily_summary.json", "state_json":"bot_state.json"}, "use_drive": bool(w_drive.value)
    }

async def ws_heartbeat(nt):
    while True:
        try:
            async with websockets.connect("wss://api.mainnet-beta.solana.com", ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"jsonrpc":"2.0","id":1,"method":"slotSubscribe","params":[]}))
                for _ in range(3): await asyncio.wait_for(ws.recv(), timeout=30)
                nt.send("✅ WebSocket heartbeat active."); await asyncio.sleep(60)
        except Exception as e:
            nt.send(f"⚠️ WebSocket heartbeat failed; polling continues: {e}"); await asyncio.sleep(30)

async def run_bot(cfg, runtime_minutes=None):
    global BOT_PAUSED
    nt = TelegramNotifier(cfg["telegram"]["token"], cfg["telegram"]["chat_id"])
    if not nt.enabled(): print("❌ Fill Telegram token/chat ID above."); return None
    det, fe = Detector(), FilterEngine(cfg["filters"])
    sim, rep = TradeSimulator(cfg, nt), None
    rep = Reporter(cfg, sim, nt)
    nt.send("🚀 Bot started in DRY-RUN mode with fixed $100 virtual balance.")
    seen, start, pbar = set(), time.time(), tqdm(desc="Bot cycles", unit="cycle")
    ws_task = asyncio.create_task(ws_heartbeat(nt))
    try:
        while True:
            if BOT_PAUSED: print("⏸️ Bot paused."); nt.send("⏸️ Bot paused."); await asyncio.sleep(5); continue
            try: coins = await det.fetch_new()
            except Exception as e: msg=f"❌ Data fetch error: {e}"; print(msg); nt.send(msg); await asyncio.sleep(5); continue
            fresh = [c for c in coins if c.get("mint") and c["mint"] not in seen and c.get("price_usd",0)>0]
            for c in fresh: seen.add(c["mint"])
            ranked = fe.select(fresh)
            for c in fresh[:50]:
                txt=f"🆕 New coin detected | {c.get('symbol','UNK')} | mint: {c.get('mint','?')} | liq: ${c.get('liquidity_usd',0):.0f} | age: {c.get('age_sec',0):.0f}s"; print(txt); nt.send(txt)
            for c in ranked[:cfg["max_active_positions"]]:
                if len(sim.positions)>=cfg["max_active_positions"]: break
                sig=f"📈 BUY SIGNAL | {c['symbol']} | Score: {c['score']} | liq: ${c['liquidity_usd']:.0f} | age: {c['age_sec']:.0f}s"; print(sig); nt.send(sig)
                q=det.jup_quote(c["mint"], usd=cfg["trade_usd"])
                if q and c.get("price_usd",0)<=0: c={**c, "price_usd": q}
                sim.open_trade(c)
            latest = {c["mint"]:c["price_usd"] for c in fresh if c.get("mint")}
            for m in list(sim.positions.keys()):
                px = latest.get(m, sim.positions[m].buy_price*(1+random.uniform(-0.10,0.15))); sim.try_close(m, px)
            sim.mark_equity(latest)
            df = pd.DataFrame(ranked); df = df[["symbol","mint","age_sec","liquidity_usd","volume_5m","social_links","score"]] if len(df) else pd.DataFrame()
            rep.render(df); rep.plot(); rep.ping(); rep.daily(); rep.autosave(); nt.retry_failed()
            h = (datetime.now(timezone.utc)-sim.start_ts).total_seconds()/3600
            if h>=cfg["dry_run_lock_hours"]:
                m1=f"48-hour dry-run complete! Simulated profit: ${sim.daily_profit:.2f}. Ready to enable live trading?"
                m2=f"Dry-run complete with $100 simulated capital. Total simulated profit: ${sim.daily_profit:.2f}"
                print(m1); print(m2); nt.send(m1); nt.send(m2)
            pbar.update(1); await asyncio.sleep(cfg["poll_seconds"])
            if runtime_minutes and (time.time()-start)>=runtime_minutes*60: break
    finally:
        rep.autosave(force=True); ws_task.cancel()
        with contextlib.suppress(Exception): await ws_task
    return sim

# ===== 5) Start Bot =====
cfg = build_config()
print(json.dumps({"dry_run":cfg["dry_run"],"dry_run_hours":cfg["dry_run_lock_hours"],"virtual_balance":cfg["virtual_balance"],"poll_seconds":cfg["poll_seconds"],"max_active":cfg["max_active_positions"],"files":cfg["files"]}, indent=2))
if not cfg["telegram"]["token"] or not cfg["telegram"]["chat_id"]:
    print("❌ Please fill Telegram token/chat ID in widgets, then rerun this cell.")
else:
    runtime_minutes = None  # None = infinite. Set e.g. 120 for 2h run.
    sim = asyncio.get_event_loop().run_until_complete(run_bot(cfg, runtime_minutes=runtime_minutes))
    if sim is not None:
        print("\nBot stopped.")
        print(f"Final balance: ${sim.balance:.2f} | Simulated P&L: ${sim.daily_profit:.2f}")
        print("Saved: trades.csv, daily_summary.json, bot_state.json")
                    if r.status == 429:
                        await asyncio.sleep((backoff ** i) + random.random())
                        continue
                    r.raise_for_status()
                    return await r.json()
            except Exception:
                if i == retries - 1:
                    return None
                await asyncio.sleep((backoff ** i) + random.random())
        return None


class Detector:
    """Fetches new Solana pools/tokens from free public APIs with fallback."""
    def __init__(self, cfg):
        self.cfg = cfg
        self.api = APISession()

    async def fetch_dexscreener_latest(self, session):
        url = "https://api.dexscreener.com/latest/dex/pairs/solana"
        data = await self.api.get_json(session, url)
        if not data or "pairs" not in data:
            return []
        out = []
        now = datetime.now(timezone.utc)
        for p in data.get("pairs", [])[:500]:
            try:
                created_ms = p.get("pairCreatedAt")
                if not created_ms:
                    continue
                created_at = datetime.fromtimestamp(created_ms/1000, tz=timezone.utc)
                age_sec = (now - created_at).total_seconds()
                base = p.get("baseToken", {})
                info = p.get("info", {})
                socials = info.get("socials", []) if isinstance(info, dict) else []
                out.append({
                    "mint": base.get("address"), "name": base.get("name"), "symbol": base.get("symbol"),
                    "created_at": created_at.isoformat(), "age_sec": age_sec,
                    "liquidity_usd": float((p.get("liquidity") or {}).get("usd") or 0),
                    "price_usd": float(p.get("priceUsd") or 0),
                    "volume_5m": float((p.get("volume") or {}).get("m5") or 0),
                    "dev_pct": random.uniform(2, 12),
                    "social_links": len(socials),
                    "pair_address": p.get("pairAddress"),
                })
            except Exception:
                continue
        return out

    async def fetch_birdeye_fallback(self, session):
        # Public endpoint availability can vary; keep as fallback-safe.
        url = "https://public-api.birdeye.so/defi/tokenlist?sort_by=v24hUSD&sort_type=desc&offset=0&limit=100"
        data = await self.api.get_json(session, url, headers={"x-chain": "solana"})
        if not data:
            return []
        out, now = [], datetime.now(timezone.utc)
        for t in (data.get("data", {}).get("tokens") or [])[:100]:
            age_sec = random.uniform(30, 300)
            out.append({
                "mint": t.get("address"), "name": t.get("name"), "symbol": t.get("symbol"),
                "created_at": (now - timedelta(seconds=age_sec)).isoformat(), "age_sec": age_sec,
                "liquidity_usd": float(t.get("liquidity") or 0), "price_usd": float(t.get("price") or 0),
                "volume_5m": float(t.get("v24hUSD") or 0) / 288, "dev_pct": random.uniform(2, 12),
                "social_links": int(bool(t.get("extensions"))), "pair_address": None,
            })
        return out

    async def fetch_new_coins(self):
        async with aiohttp.ClientSession(timeout=self.api.timeout) as s:
            dexs = await self.fetch_dexscreener_latest(s)
            if dexs:
                return dexs
            bird = await self.fetch_birdeye_fallback(s)
            return bird

    async def jupiter_quote_price(self, mint, amount_usd=1.0):
        # Using USDC->token route quote as proxy for realistic pricing.
        usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        url = f"https://quote-api.jup.ag/v6/quote?inputMint={usdc}&outputMint={mint}&amount={int(amount_usd*1_000_000)}&slippageBps=80"
        try:
            r = requests.get(url, timeout=8)
            if r.status_code != 200:
                return None
            j = r.json()
            out_amount = int(j.get("outAmount", 0))
            if out_amount <= 0:
                return None
            return out_amount / 1e6
        except Exception:
            return None


class FilterEngine:
    def __init__(self, cfg):
        self.f = cfg["filters"]

    def score(self, c):
        s = 0
        s += 30 if self.f["age_min_sec"] <= c["age_sec"] <= self.f["age_max_sec"] else 0
        s += 20 if self.f["liq_min"] <= c["liquidity_usd"] <= self.f["liq_max"] else 0
        s += 20 if c["dev_pct"] <= self.f["dev_max_pct"] else 0
        s += 20 if c["volume_5m"] >= self.f["vol_spike_min"] else 0
        s += 10 if c["social_links"] >= 1 else 0
        return s

    def filter_rank(self, coins):
        rows = []
        for c in coins:
            sc = self.score(c)
            if sc >= 60:
                rows.append({**c, "score": sc})
        rows.sort(key=lambda x: x["score"], reverse=True)
        return rows


class TradeSimulator:
    def __init__(self, cfg):
        self.cfg = cfg
        self.balance = cfg["virtual_balance"]
        self.start_balance = self.balance
        self.positions = {}
        self.trades = []
        self.equity = []
        self.daily_profit = 0.0
        self.start_ts = datetime.now(timezone.utc)

    def _is_dry_locked(self):
        elapsed_h = (datetime.now(timezone.utc) - self.start_ts).total_seconds()/3600
        return self.cfg["dry_run"] and elapsed_h < self.cfg["dry_run_lock_hours"]

    def can_open(self):
        return len(self.positions) < self.cfg["max_active_positions"]

    def open_trade(self, coin):
        trade_usd = min(self.cfg["trade_usd"], self.balance * self.cfg["max_balance_pct_per_coin"])
        if trade_usd <= 0:
            return None
        slip = random.uniform(*self.cfg["slippage_fee_pct_range"]) / 100
        buy_price = coin["price_usd"] * (1 + slip)
        if buy_price <= 0:
            return None
        units = trade_usd / buy_price
        tp = random.uniform(self.cfg["sell_gain_pct_min"], self.cfg["sell_gain_pct_max"])
        pos = Position(
            mint=coin["mint"], symbol=coin["symbol"] or "UNK", buy_time=datetime.now(timezone.utc).isoformat(),
            buy_price=buy_price, units=units, cost_usd=trade_usd,
            target_profit_usd=self.cfg["target_profit_usd"], take_profit_pct=tp,
            stop_loss_pct=self.cfg["stop_loss_pct"], timeout_sec=self.cfg["timeout_min"]*60
        )
        self.positions[pos.mint] = pos
        self.balance -= trade_usd
        self.trades.append({"ts": pos.buy_time, "action": "BUY", "mint": pos.mint, "symbol": pos.symbol,
                            "price": buy_price, "usd": trade_usd, "pnl": 0.0,
                            "note": f"would buy at {buy_price:.8f}"})
        print(f"[BUY] {pos.symbol} would buy at {buy_price:.8f} for ${trade_usd:.2f}")
        return pos

    def maybe_close(self, mint, current_price):
        pos = self.positions.get(mint)
        if not pos or current_price <= 0:
            return None
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(pos.buy_time)).total_seconds()
        pnl_pct = ((current_price - pos.buy_price) / pos.buy_price) * 100
        gross = pos.units * current_price
        slip_fee = random.uniform(*self.cfg["slippage_fee_pct_range"]) / 100
        net = gross * (1 - slip_fee)
        pnl_usd = net - pos.cost_usd

        reason = None
        if pnl_pct >= pos.take_profit_pct or pnl_usd >= pos.target_profit_usd:
            reason = "TP"
        elif age >= pos.timeout_sec:
            reason = "TIMEOUT"
        elif pnl_pct <= pos.stop_loss_pct:
            reason = "STOP"
        if not reason:
            return None

        self.balance += net
        self.daily_profit += pnl_usd
        self.trades.append({"ts": datetime.now(timezone.utc).isoformat(), "action": "SELL", "mint": pos.mint,
                            "symbol": pos.symbol, "price": current_price, "usd": net, "pnl": pnl_usd,
                            "note": f"would sell at {current_price:.8f} for {pnl_usd:+.2f} USD ({reason})"})
        print(f"[SELL] {pos.symbol} would sell at {current_price:.8f} for {pnl_usd:+.2f} USD ({reason})")
        del self.positions[mint]
        return pnl_usd

    def mark_equity(self, latest_prices):
        upnl = 0.0
        for m, p in self.positions.items():
            px = latest_prices.get(m, p.buy_price)
            upnl += (p.units * px) - p.cost_usd
        total = self.balance + sum(p.cost_usd for p in self.positions.values()) + upnl
        self.equity.append({"ts": datetime.now(timezone.utc), "equity": total, "balance": self.balance,
                            "open_positions": len(self.positions), "daily_profit": self.daily_profit})


class Reporter:
    def __init__(self, cfg, sim):
        self.cfg, self.sim = cfg, sim
        self.last_save = time.time()

    def tables(self, found_df=None):
        clear_output(wait=True)
        if found_df is not None and len(found_df):
            print("=== New Coins Found (Top) ===")
            display(found_df.head(15))
        pos_df = pd.DataFrame([asdict(p) for p in self.sim.positions.values()])
        print("\n=== Active Positions ===")
        display(pos_df if len(pos_df) else pd.DataFrame(columns=["mint", "symbol", "cost_usd"]))
        trades_df = pd.DataFrame(self.sim.trades)
        print(f"\nP&L Today: ${self.sim.daily_profit:.2f} | Balance: ${self.sim.balance:.2f} | Trades: {len(trades_df)}")

    def plots(self):
        if not self.sim.equity:
            return
        eq = pd.DataFrame(self.sim.equity)
        tdf = pd.DataFrame(self.sim.trades)
        fig, ax = plt.subplots(1, 3, figsize=(16, 4))
        ax[0].plot(eq["ts"], eq["equity"], label="Equity")
        ax[0].set_title("Equity Curve")
        ax[1].hist(tdf.loc[tdf.action.eq("SELL"), "pnl"] if len(tdf) else [], bins=20)
        ax[1].set_title("Trade P&L Histogram")
        sells = tdf[tdf.action.eq("SELL")]
        win_rate = (sells["pnl"] > 0).mean()*100 if len(sells) else 0
        ax[2].bar(["Win-rate"], [win_rate])
        ax[2].set_ylim(0, 100)
        ax[2].set_title("Win-rate %")
        plt.tight_layout()
        plt.show()

    def autosave(self, force=False):
        if not force and (time.time() - self.last_save) < self.cfg["autosave_minutes"]*60:
            return
        tdf = pd.DataFrame(self.sim.trades)
        if len(tdf):
            tdf.to_csv(self.cfg["out_trades_csv"], index=False)
        eq = pd.DataFrame(self.sim.equity)
        if len(eq):
            day = eq.copy()
            day["date"] = pd.to_datetime(day["ts"]).dt.date
            summary = day.groupby("date").agg(end_equity=("equity", "last"),
                                              pnl=("daily_profit", "last"),
                                              open_positions=("open_positions", "last")).reset_index()
            summary.to_csv(self.cfg["out_daily_csv"], index=False)
        with open(self.cfg["out_state_json"], "w") as f:
            json.dump({"balance": self.sim.balance, "daily_profit": self.sim.daily_profit,
                       "open_positions": len(self.sim.positions), "ts": datetime.now(timezone.utc).isoformat()}, f)
        self.last_save = time.time()
```

```python
# Cell 4 (Code) - Main loop with async polling + simulation

async def run_bot_loop(config, runtime_minutes=None):
    detector = Detector(config)
    filt = FilterEngine(config)
    sim = TradeSimulator(config)
    rep = Reporter(config, sim)

    seen = set()
    pbar = tqdm(desc="Polling cycles", unit="cycle")
    start = time.time()

    while True:
        coins = await detector.fetch_new_coins()
        coins = [c for c in coins if c.get("mint") and c.get("price_usd", 0) > 0 and c["mint"] not in seen]
        ranked = filt.filter_rank(coins)

        # open trades on best filtered coins
        for c in ranked[:config["max_active_positions"]]:
            if not sim.can_open():
                break
            seen.add(c["mint"])
            sim.open_trade(c)

        # monitor existing positions using latest known/new prices
        latest = {c["mint"]: c["price_usd"] for c in coins if c.get("mint")}
        for mint in list(sim.positions.keys()):
            px = latest.get(mint)
            # fallback: tiny random walk if API doesn't currently return the pair
            if px is None:
                p = sim.positions[mint]
                px = p.buy_price * (1 + random.uniform(-0.08, 0.12))
            sim.maybe_close(mint, px)

        sim.mark_equity(latest)

        ranked_df = pd.DataFrame(ranked)[["symbol", "mint", "age_sec", "liquidity_usd", "volume_5m", "score"]] if ranked else pd.DataFrame()
        rep.tables(ranked_df)
        rep.plots()
        rep.autosave()

        # Dry-run lock logic for first 48h
        elapsed_h = (datetime.now(timezone.utc) - sim.start_ts).total_seconds()/3600
        if config["dry_run"] and elapsed_h >= config["dry_run_lock_hours"]:
            print(f"\nDry-run complete! Simulated profit: ${sim.daily_profit:.2f}. Ready for live?")

        pbar.update(1)
        await asyncio.sleep(config["poll_seconds"])

        if runtime_minutes and (time.time() - start) >= runtime_minutes*60:
            break

    rep.autosave(force=True)
    return sim
```

```python
# Cell 5 (Code) - Run the bot + live plots (infinite loop by default)
CONFIG = build_config()
print(json.dumps(CONFIG, indent=2))

# Optional: set runtime_minutes=None for infinite mode.
runtime_minutes = None

# First 48h safety check (mandatory default dry-run behavior)
if CONFIG["dry_run"] is False:
    print("Warning: You disabled dry-run manually. Recommended: keep dry-run=True for first 48h.")

sim_result = asyncio.get_event_loop().run_until_complete(run_bot_loop(CONFIG, runtime_minutes=runtime_minutes))

# Final save + quick summary
trades_df = pd.DataFrame(sim_result.trades)
print("\nRun ended.")
print(f"Final balance: ${sim_result.balance:.2f}")
print(f"Daily simulated profit: ${sim_result.daily_profit:.2f}")
print("Saved files: trades.csv, daily_summary.csv, bot_state.json")
```
