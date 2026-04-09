"""
mev_bot.py
──────────────────────────────────────────────────────────────────────────────
MEV Bot Profitability Analyzer + Sandwich Attack Simulator

Two modes:
  1. LIVE   — Connect to Dune Analytics API, pull real MEV data, analyze
  2. SIM    — Simulate sandwich attacks using AMM math, compute P&L

Components:
  • DuneClient         — Fetch query results from Dune API
  • SandwichSimulator  — Compute exact sandwich P&L using x·y=k / V3 math
  • MempoolMonitor     — Scan pending txs for sandwichable targets (mock/live)
  • ProfitTracker      — Track cumulative P&L, gas costs, success rates
  • ReportGenerator    — Export charts + JSON summary

Usage:
  pip install requests pandas numpy matplotlib dune-client web3 python-dotenv

  # Simulate only (no API key needed)
  python mev_bot.py --mode sim --capital 10 --blocks 1000

  # Pull live data from Dune (needs DUNE_API_KEY in .env)
  python mev_bot.py --mode live --days 30

  # Full: simulate + pull Dune data + generate report
  python mev_bot.py --mode full --days 7 --output report/
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter, MaxNLocator


# ──────────────────────────────────────────────────────────────────────────────
#  Config & Constants
# ──────────────────────────────────────────────────────────────────────────────

DUNE_API_BASE = "https://api.dune.com/api/v1"

# Dune query IDs (publish your queries and paste IDs here)
DUNE_QUERY_IDS = {
    "daily_profit":       3_698_201,
    "sandwich_attacks":   3_698_202,
    "slippage_impact":    3_698_203,
    "top_bots":           3_698_204,
    "attack_by_pool":     3_698_205,
    "victim_distribution":3_698_206,
    "cumulative_mev":     3_698_207,
}

ETH_PRICE_USD = 2_400.0
BASE_GAS_UNITS = 350_000      # typical sandwich bundle gas
AAVE_FLASH_FEE = 0.0009       # 0.09%

DOLLAR = FuncFormatter(lambda x, _: f"${x:,.0f}")
PCT    = FuncFormatter(lambda x, _: f"{x:.2f}%")

# ──────────────────────────────────────────────────────────────────────────────
#  Data Classes
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class SandwichAttack:
    block:          int
    pool:           str
    attacker:       str
    victim:         str
    frontrun_size:  float          # ETH
    victim_size:    float          # ETH
    backrun_size:   float          # ETH
    pool_reserve_x: float          # token0 reserves before attack
    pool_reserve_y: float          # token1 reserves before attack
    gas_gwei:       float

    # Computed after __post_init__
    frontrun_output:   float = 0.0
    victim_output:     float = 0.0
    victim_fair_output:float = 0.0
    backrun_output:    float = 0.0
    gross_profit:      float = 0.0
    gas_cost_eth:      float = 0.0
    net_profit:        float = 0.0
    victim_loss:       float = 0.0
    slippage_pct:      float = 0.0
    success:           bool  = True

    def __post_init__(self):
        self._compute_amm()

    def _compute_amm(self):
        """
        Exact AMM sandwich math using x·y=k constant product.
        All amounts in token0 (ETH-equivalent) units.
        """
        rx, ry = self.pool_reserve_x, self.pool_reserve_y
        fee = 0.003  # Uniswap V2-style 0.3%

        # ── Frontrun: attacker buys token1 with token0 ────────────────────
        dx_fr = self.frontrun_size
        # dy = ry * dx*(1-fee) / (rx + dx*(1-fee))
        dy_fr = ry * dx_fr * (1 - fee) / (rx + dx_fr * (1 - fee))
        rx1 = rx + dx_fr
        ry1 = ry - dy_fr
        self.frontrun_output = dy_fr

        # ── Victim swap ────────────────────────────────────────────────────
        # Victim expected fair output (without frontrun)
        self.victim_fair_output = (
            ry * self.victim_size * (1 - fee)
            / (rx + self.victim_size * (1 - fee))
        )
        # Actual output after frontrun moved reserves to (rx1, ry1)
        dx_v = self.victim_size
        dy_v = ry1 * dx_v * (1 - fee) / (rx1 + dx_v * (1 - fee))
        rx2 = rx1 + dx_v
        ry2 = ry1 - dy_v
        self.victim_output = dy_v

        # ── Backrun: attacker sells dy_fr token1 for token0 ───────────────
        dy_br = self.frontrun_output   # sell exactly what was bought
        dx_br = rx2 * dy_br * (1 - fee) / (ry2 + dy_br * (1 - fee))
        self.backrun_output = dx_br

        # ── P&L ───────────────────────────────────────────────────────────
        self.gross_profit = dx_br - dx_fr
        self.gas_cost_eth = BASE_GAS_UNITS * self.gas_gwei * 1e-9
        self.net_profit   = self.gross_profit - self.gas_cost_eth

        # Victim impact
        self.victim_loss  = self.victim_fair_output - self.victim_output
        self.slippage_pct = (
            (self.victim_fair_output - self.victim_output)
            / self.victim_fair_output * 100
            if self.victim_fair_output > 0 else 0
        )
        self.success = self.net_profit > 0


@dataclass
class BotSession:
    address:         str = "0xYourBotAddress"
    capital_eth:     float = 10.0
    blocks_scanned:  int = 0
    attacks_attempted: int = 0
    attacks_profitable: int = 0
    total_gross_eth: float = 0.0
    total_gas_eth:   float = 0.0
    total_net_eth:   float = 0.0
    total_victim_loss_eth: float = 0.0
    attacks:         list = field(default_factory=list)
    daily_pnl:       dict = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        return self.attacks_profitable / max(self.attacks_attempted, 1)

    @property
    def roi_pct(self) -> float:
        return (self.total_net_eth / self.capital_eth) * 100

    @property
    def net_usd(self) -> float:
        return self.total_net_eth * ETH_PRICE_USD


# ──────────────────────────────────────────────────────────────────────────────
#  Dune Analytics Client
# ──────────────────────────────────────────────────────────────────────────────

class DuneClient:
    """
    Wrapper for Dune Analytics API v1.
    Executes queries and fetches result CSVs.
    """

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"X-Dune-API-Key": api_key}

    def execute_query(self, query_id: int, params: dict = None) -> str:
        """Trigger query execution, return execution_id."""
        import requests
        url  = f"{DUNE_API_BASE}/query/{query_id}/execute"
        body = {"query_parameters": params or {}}
        resp = requests.post(url, headers=self.headers, json=body, timeout=10)
        resp.raise_for_status()
        return resp.json()["execution_id"]

    def poll_execution(self, exec_id: str, timeout: int = 120) -> dict:
        """Poll until query completes, return result dict."""
        import requests
        url = f"{DUNE_API_BASE}/execution/{exec_id}/status"
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = requests.get(url, headers=self.headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            state = data.get("state", "")
            if state == "QUERY_STATE_COMPLETED":
                return self.fetch_results(exec_id)
            elif state == "QUERY_STATE_FAILED":
                raise RuntimeError(f"Dune query failed: {data}")
            time.sleep(3)
        raise TimeoutError(f"Query {exec_id} timed out after {timeout}s")

    def fetch_results(self, exec_id: str) -> pd.DataFrame:
        """Download result rows as DataFrame."""
        import requests
        url  = f"{DUNE_API_BASE}/execution/{exec_id}/results"
        resp = requests.get(url, headers=self.headers, timeout=30)
        resp.raise_for_status()
        rows = resp.json()["result"]["rows"]
        return pd.DataFrame(rows)

    def run(self, query_id: int, params: dict = None) -> pd.DataFrame:
        """Execute + poll + return results as DataFrame."""
        exec_id = self.execute_query(query_id, params)
        return self.poll_execution(exec_id)

    def fetch_all(self) -> dict[str, pd.DataFrame]:
        """Run all configured queries in sequence."""
        results = {}
        for name, qid in DUNE_QUERY_IDS.items():
            print(f"  [dune] running: {name} (id={qid})")
            try:
                results[name] = self.run(qid)
                print(f"  [dune] ✓ {name}: {len(results[name])} rows")
            except Exception as e:
                print(f"  [dune] ✗ {name}: {e}")
                results[name] = pd.DataFrame()
        return results


# ──────────────────────────────────────────────────────────────────────────────
#  Sandwich Attack Simulator
# ──────────────────────────────────────────────────────────────────────────────

class SandwichSimulator:
    """
    Simulate sandwich attacks across synthetic mempool transactions.
    Uses GBM for price evolution and Poisson for victim tx arrival.
    """

    def __init__(
        self,
        capital_eth: float = 10.0,
        gas_strategy: str  = "adaptive",   # "fixed" | "adaptive" | "aggressive"
        target_pools: list = None,
        seed: int = 42
    ):
        self.capital = capital_eth
        self.gas_strategy = gas_strategy
        self.target_pools = target_pools or [
            "ETH/USDC-5bps", "WBTC/ETH-30bps", "LINK/ETH-30bps"
        ]
        self.rng = np.random.default_rng(seed)

    def _gas_gwei(self, block: int, base_fee: float) -> float:
        """Estimate competitive gas to win block inclusion."""
        if self.gas_strategy == "fixed":
            return 30.0
        elif self.gas_strategy == "adaptive":
            # 10-20% above base fee to be competitive
            return base_fee * self.rng.uniform(1.1, 1.3)
        else:  # aggressive
            return base_fee * self.rng.uniform(1.5, 2.5)

    def _optimal_frontrun_size(
        self,
        victim_size: float,
        rx: float,
        ry: float
    ) -> float:
        """
        Analytically compute optimal frontrun size.
        Maximize gross profit = backrun_output - frontrun_size.
        For x·y=k: optimal dx_f = sqrt(rx * (rx + victim_size)) - rx
        (derived from first-order condition)
        """
        return math.sqrt(rx * (rx + victim_size * (1 - 0.003))) - rx

    def run(self, n_blocks: int = 1000) -> BotSession:
        """
        Simulate n_blocks blocks of MEV bot operation.
        Returns BotSession with full attack history.
        """
        session = BotSession(capital_eth=self.capital)

        # Initial pool state: ETH/USDC with $10M TVL
        rx = 1_666.0   # ~4166 ETH at $2400
        ry = 4_000_000.0  # USDC

        # GBM for ETH price
        p  = 2400.0
        sigma_h = 0.85 / math.sqrt(8760)  # hourly vol

        base_fee = 30.0  # Gwei

        for block in range(n_blocks):
            session.blocks_scanned += 1

            # Update price
            p *= math.exp(sigma_h * self.rng.standard_normal() - 0.5 * sigma_h**2)

            # Update base fee (mean-reverting)
            base_fee = max(5, base_fee + self.rng.normal(0, 5))

            # Poisson arrivals: avg 3 sandwichable txs per block
            n_victims = self.rng.poisson(3)

            for _ in range(n_victims):
                # Victim trade size: lognormal, mean ~$2k
                victim_usd  = self.rng.lognormal(math.log(2000), 1.2)
                victim_eth  = victim_usd / p
                if victim_eth < 0.05:
                    continue  # too small to be worth it

                fr_size = min(
                    self._optimal_frontrun_size(victim_eth, rx, ry),
                    self.capital * 0.8  # never deploy > 80% of capital
                )
                if fr_size <= 0:
                    continue

                gas_gwei = self._gas_gwei(block, base_fee)

                attack = SandwichAttack(
                    block          = block,
                    pool           = self.rng.choice(self.target_pools),
                    attacker       = "0xYourBot",
                    victim         = f"0xVictim{block:04d}",
                    frontrun_size  = fr_size,
                    victim_size    = victim_eth,
                    backrun_size   = fr_size,
                    pool_reserve_x = rx,
                    pool_reserve_y = ry,
                    gas_gwei       = gas_gwei,
                )

                session.attacks_attempted += 1
                if attack.success:
                    session.attacks_profitable += 1
                    session.total_gross_eth += attack.gross_profit
                    session.total_net_eth   += attack.net_profit

                session.total_gas_eth        += attack.gas_cost_eth
                session.total_victim_loss_eth += max(0, attack.victim_loss)
                session.attacks.append(attack)

                # Update pool reserves after victim swap
                fee = 0.003
                ry_new = ry - (ry * victim_eth * (1 - fee) / (rx + victim_eth * (1 - fee)))
                rx = rx + victim_eth
                ry = ry_new

        return session


# ──────────────────────────────────────────────────────────────────────────────
#  Visualization
# ──────────────────────────────────────────────────────────────────────────────

def plot_simulation(session: BotSession, output_dir: str = "."):
    """6-panel MEV analytics dashboard."""

    attacks = session.attacks
    if not attacks:
        print("[warn] No attacks to plot")
        return

    df = pd.DataFrame([{
        "block":          a.block,
        "net_eth":        a.net_profit,
        "gross_eth":      a.gross_profit,
        "gas_eth":        a.gas_cost_eth,
        "victim_loss":    max(0, a.victim_loss),
        "slippage_pct":   a.slippage_pct,
        "victim_size":    a.victim_size,
        "frontrun_size":  a.frontrun_size,
        "success":        a.success,
    } for a in attacks])

    df["cumulative_net"] = df["net_eth"].cumsum()
    df["cumulative_victim"] = df["victim_loss"].cumsum()

    # ── Style ──────────────────────────────────────────────────────────────
    BG       = "#09090b"
    SURFACE  = "#111116"
    BORDER   = "#27272a"
    ACCENT   = "#f97316"      # orange — danger aesthetic
    GREEN    = "#22c55e"
    RED      = "#ef4444"
    AMBER    = "#eab308"
    BLUE     = "#3b82f6"
    MUTED    = "#52525b"
    TEXT     = "#d4d4d8"

    fig = plt.figure(figsize=(20, 13), facecolor=BG)
    fig.suptitle(
        "MEV Bot Profitability — Sandwich Attack Analysis",
        color=TEXT, fontsize=14, fontweight="bold",
        fontfamily="monospace", y=0.97
    )

    gs = gridspec.GridSpec(3, 3, figure=fig, hspace=0.50, wspace=0.35)
    axes = [
        fig.add_subplot(gs[0, :2]),   # 0: cumulative P&L
        fig.add_subplot(gs[0, 2]),    # 1: victim loss cumulative
        fig.add_subplot(gs[1, 0]),    # 2: per-attack net profit dist
        fig.add_subplot(gs[1, 1]),    # 3: slippage distribution
        fig.add_subplot(gs[1, 2]),    # 4: success rate over time
        fig.add_subplot(gs[2, :]),    # 5: stats table
    ]

    def sax(ax, title=""):
        ax.set_facecolor(SURFACE)
        ax.tick_params(colors=MUTED, labelsize=8)
        for spine in ax.spines.values():
            spine.set_edgecolor(BORDER)
        if title:
            ax.set_title(title, color=TEXT, fontsize=9, pad=7,
                         fontfamily="monospace")

    # ── 0: Cumulative Bot P&L ─────────────────────────────────────────────
    sax(axes[0], "Cumulative net P&L (ETH)")
    blocks_x = df["block"].values
    cum      = df["cumulative_net"].values
    axes[0].plot(blocks_x, cum, color=ACCENT, lw=2)
    axes[0].fill_between(blocks_x, 0, cum,
        where=(cum >= 0), color=ACCENT, alpha=0.12)
    axes[0].fill_between(blocks_x, 0, cum,
        where=(cum < 0),  color=RED,    alpha=0.12)
    axes[0].axhline(0, color=MUTED, lw=0.6, ls=":")
    axes[0].yaxis.set_major_formatter(FuncFormatter(lambda v,_: f"{v:.3f} ETH"))
    axes[0].set_xlabel("Block", color=MUTED, fontsize=8)

    # ── 1: Victim Loss Cumulative ─────────────────────────────────────────
    sax(axes[1], "Cumulative victim extraction (ETH)")
    cvl = df["cumulative_victim"].values
    axes[1].plot(blocks_x, cvl, color=RED, lw=1.5)
    axes[1].fill_between(blocks_x, 0, cvl, color=RED, alpha=0.1)
    axes[1].yaxis.set_major_formatter(FuncFormatter(lambda v,_: f"{v:.2f}"))

    # ── 2: Per-attack profit histogram ────────────────────────────────────
    sax(axes[2], "Net profit per attack (ETH)")
    profitable = df.loc[df["success"],  "net_eth"].values
    lossy      = df.loc[~df["success"], "net_eth"].values
    all_vals   = df["net_eth"].values
    bins = np.linspace(all_vals.min(), min(all_vals.max(), 0.01), 40)
    axes[2].hist(profitable, bins=bins, color=ACCENT, alpha=0.7, label="Profitable")
    axes[2].hist(lossy,      bins=bins, color=RED,    alpha=0.5, label="Loss")
    axes[2].axvline(0, color=MUTED, lw=0.8, ls="--")
    axes[2].legend(facecolor=SURFACE, edgecolor=BORDER, labelcolor=TEXT, fontsize=7)
    axes[2].set_xlabel("Net ETH", color=MUTED, fontsize=8)

    # ── 3: Slippage distribution ──────────────────────────────────────────
    sax(axes[3], "Victim slippage % distribution")
    slip = df["slippage_pct"].clip(0, 5).values
    axes[3].hist(slip, bins=30, color=RED, alpha=0.75)
    axes[3].axvline(slip.mean(), color=AMBER, lw=1.2, ls="--",
                    label=f"Mean {slip.mean():.2f}%")
    axes[3].axvline(np.median(slip), color=BLUE, lw=1.2, ls="--",
                    label=f"Median {np.median(slip):.2f}%")
    axes[3].legend(facecolor=SURFACE, edgecolor=BORDER, labelcolor=TEXT, fontsize=7)
    axes[3].set_xlabel("Slippage %", color=MUTED, fontsize=8)

    # ── 4: Rolling success rate ───────────────────────────────────────────
    sax(axes[4], "Rolling success rate (100-attack window)")
    roll_success = df["success"].rolling(100).mean() * 100
    axes[4].plot(blocks_x, roll_success.values, color=GREEN, lw=1.5)
    axes[4].axhline(50, color=MUTED, lw=0.6, ls=":")
    axes[4].set_ylim(0, 100)
    axes[4].yaxis.set_major_formatter(FuncFormatter(lambda v,_: f"{v:.0f}%"))

    # ── 5: Stats table ────────────────────────────────────────────────────
    axes[5].set_facecolor(SURFACE)
    axes[5].axis("off")
    axes[5].set_title("Summary", color=TEXT, fontsize=9, pad=7,
                       fontfamily="monospace")
    for spine in axes[5].spines.values():
        spine.set_edgecolor(BORDER)

    total_victim_usd = session.total_victim_loss_eth * ETH_PRICE_USD
    stats = [
        ("Blocks scanned",         f"{session.blocks_scanned:,}"),
        ("Attacks attempted",      f"{session.attacks_attempted:,}"),
        ("Attacks profitable",     f"{session.attacks_profitable:,}"),
        ("Success rate",           f"{session.success_rate:.1%}"),
        ("Gross profit (ETH)",     f"{session.total_gross_eth:.4f}"),
        ("Gas costs (ETH)",        f"{session.total_gas_eth:.4f}"),
        ("Net profit (ETH)",       f"{session.total_net_eth:.4f}"),
        ("Net profit (USD)",       f"${session.net_usd:,.0f}"),
        ("Total victim loss (ETH)",f"{session.total_victim_loss_eth:.4f}"),
        ("Total victim loss (USD)",f"${total_victim_usd:,.0f}"),
        ("Avg slippage imposed",   f"{df['slippage_pct'].mean():.3f}%"),
        ("Capital deployed",       f"{session.capital_eth:.1f} ETH"),
        ("ROI",                    f"{session.roi_pct:.2f}%"),
        ("Attacker / victim ratio",f"{session.total_net_eth / max(session.total_victim_loss_eth,1e-9):.2f}×"),
    ]

    rows_per_col = 7
    table_data   = []
    for i in range(rows_per_col):
        row = []
        for j in range(2):
            idx = i + j * rows_per_col
            row += list(stats[idx]) if idx < len(stats) else ["", ""]
        table_data.append(row)

    tbl = axes[5].table(
        cellText=table_data,
        colLabels=["Metric", "Value", "Metric", "Value"],
        cellLoc="left", loc="center",
        bbox=[0, -0.1, 1, 1.1]
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    for (r, c), cell in tbl.get_celld().items():
        cell.set_facecolor(SURFACE if r > 0 else "#1c1c20")
        cell.set_edgecolor(BORDER)
        cell.set_text_props(
            color=(MUTED if c % 2 == 0 else TEXT) if r > 0 else TEXT,
            fontfamily="monospace"
        )

    path = os.path.join(output_dir, "mev_report.png")
    plt.savefig(path, dpi=150, bbox_inches="tight", facecolor=BG)
    print(f"[✓] Chart saved → {path}")
    return path


# ──────────────────────────────────────────────────────────────────────────────
#  Dune Data Visualizer
# ──────────────────────────────────────────────────────────────────────────────

def plot_dune_data(dune_data: dict, output_dir: str = "."):
    """Render charts from live Dune data."""
    BG = "#09090b"; TEXT = "#d4d4d8"; MUTED = "#52525b"
    ACCENT = "#f97316"; RED = "#ef4444"; GREEN = "#22c55e"

    fig, axes = plt.subplots(2, 3, figsize=(18, 10), facecolor=BG)
    fig.suptitle("MEV Bot Intelligence — Live Dune Analytics",
                 color=TEXT, fontsize=13, fontweight="bold",
                 fontfamily="monospace")

    def sax(ax, title):
        ax.set_facecolor("#111116")
        ax.tick_params(colors=MUTED, labelsize=8)
        for s in ax.spines.values(): s.set_edgecolor("#27272a")
        ax.set_title(title, color=TEXT, fontsize=9, pad=7,
                     fontfamily="monospace")

    charts = [
        ("daily_profit",        "Daily MEV profit (USD)"),
        ("cumulative_mev",      "Cumulative MEV extracted"),
        ("attack_by_pool",      "Attacks by pool"),
        ("victim_distribution", "Victim loss distribution"),
        ("top_bots",            "Top 10 MEV bots"),
        ("slippage_impact",     "Retail slippage %"),
    ]

    for ax, (key, title) in zip(axes.flat, charts):
        sax(ax, title)
        df = dune_data.get(key, pd.DataFrame())
        if df.empty:
            ax.text(0.5, 0.5, "No data\n(add Dune query ID)",
                    ha="center", va="center",
                    color=MUTED, fontsize=9, transform=ax.transAxes)
            continue

        try:
            if key == "daily_profit":
                df["day"] = pd.to_datetime(df.get("day", []))
                ax.bar(df["day"], df.get("profit_usd", []),
                       color=ACCENT, alpha=0.8, width=0.8)
                ax.yaxis.set_major_formatter(DOLLAR)
            elif key == "cumulative_mev":
                df["day"] = pd.to_datetime(df.get("day", []))
                ax.plot(df["day"], df.get("cumulative_mev_usd", []),
                        color=RED, lw=2)
                ax.fill_between(df["day"], 0, df.get("cumulative_mev_usd", []),
                                color=RED, alpha=0.1)
                ax.yaxis.set_major_formatter(DOLLAR)
            elif key == "attack_by_pool":
                top = df.nlargest(8, "attack_count") if "attack_count" in df else df.head(8)
                ax.barh(top.get("pair", top.index), top.get("attack_count", []),
                        color=ACCENT, alpha=0.8)
            elif key == "victim_distribution":
                ax.bar(df.get("loss_bucket", []), df.get("victim_count", []),
                       color=RED, alpha=0.8)
                ax.tick_params(axis="x", rotation=45)
            elif key == "top_bots":
                top = df.nlargest(10, "estimated_mev_usd") if "estimated_mev_usd" in df else df.head(10)
                labels = [str(a)[:8]+"…" for a in top.get("bot_address", top.index)]
                ax.barh(labels, top.get("estimated_mev_usd", []), color=ACCENT, alpha=0.8)
                ax.xaxis.set_major_formatter(DOLLAR)
            elif key == "slippage_impact":
                vals = df.get("slippage_pct", [])
                ax.hist(vals, bins=20, color=RED, alpha=0.8)
                ax.set_xlabel("Slippage %", color=MUTED, fontsize=8)
        except Exception as e:
            ax.text(0.5, 0.5, f"Plot error:\n{e}",
                    ha="center", va="center",
                    color=RED, fontsize=8, transform=ax.transAxes)

    plt.tight_layout()
    path = os.path.join(output_dir, "dune_report.png")
    plt.savefig(path, dpi=150, bbox_inches="tight", facecolor=BG)
    print(f"[✓] Dune chart saved → {path}")


# ──────────────────────────────────────────────────────────────────────────────
#  Report Generator
# ──────────────────────────────────────────────────────────────────────────────

def export_json(session: BotSession, output_dir: str = "."):
    """Export session summary as JSON for the dashboard."""
    attacks = session.attacks
    df = pd.DataFrame([{
        "block":       a.block,
        "net_eth":     round(a.net_profit, 6),
        "slippage":    round(a.slippage_pct, 4),
        "victim_loss": round(max(0, a.victim_loss), 6),
        "success":     a.success,
    } for a in attacks])

    summary = {
        "generated_at":         datetime.utcnow().isoformat(),
        "blocks_scanned":       session.blocks_scanned,
        "attacks_attempted":    session.attacks_attempted,
        "attacks_profitable":   session.attacks_profitable,
        "success_rate_pct":     round(session.success_rate * 100, 2),
        "total_gross_eth":      round(session.total_gross_eth, 6),
        "total_gas_eth":        round(session.total_gas_eth, 6),
        "total_net_eth":        round(session.total_net_eth, 6),
        "total_net_usd":        round(session.net_usd, 2),
        "total_victim_loss_eth":round(session.total_victim_loss_eth, 6),
        "total_victim_loss_usd":round(session.total_victim_loss_eth * ETH_PRICE_USD, 2),
        "avg_slippage_pct":     round(df["slippage"].mean(), 4),
        "roi_pct":              round(session.roi_pct, 4),
        "capital_eth":          session.capital_eth,
    }

    path = os.path.join(output_dir, "mev_summary.json")
    with open(path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"[✓] JSON summary → {path}")
    return summary


# ──────────────────────────────────────────────────────────────────────────────
#  CLI Entry Point
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="MEV Bot Profitability Analyzer")
    parser.add_argument("--mode",     choices=["sim","live","full"], default="sim")
    parser.add_argument("--capital",  type=float, default=10.0,   help="Bot capital in ETH")
    parser.add_argument("--blocks",   type=int,   default=1000,   help="Blocks to simulate")
    parser.add_argument("--days",     type=int,   default=30,     help="Days for live Dune data")
    parser.add_argument("--gas",      choices=["fixed","adaptive","aggressive"], default="adaptive")
    parser.add_argument("--output",   default=".",               help="Output directory")
    parser.add_argument("--dune-key", default=os.getenv("DUNE_API_KEY", ""), help="Dune API key")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print(f"\n{'─'*60}")
    print(f"  MEV Bot Analyzer | mode={args.mode} | capital={args.capital} ETH")
    print(f"{'─'*60}\n")

    session = None

    if args.mode in ("sim", "full"):
        print(f"[→] Simulating {args.blocks:,} blocks...")
        sim = SandwichSimulator(
            capital_eth  = args.capital,
            gas_strategy = args.gas,
        )
        session = sim.run(args.blocks)
        summary = export_json(session, args.output)

        print(f"\n{'─'*60}")
        print(f"  SIMULATION RESULTS")
        print(f"{'─'*60}")
        print(f"  Attacks attempted:    {session.attacks_attempted:,}")
        print(f"  Success rate:         {session.success_rate:.1%}")
        print(f"  Net profit:           {session.total_net_eth:.5f} ETH  (${session.net_usd:,.0f})")
        print(f"  Total victim loss:    {session.total_victim_loss_eth:.5f} ETH  (${session.total_victim_loss_eth*ETH_PRICE_USD:,.0f})")
        print(f"  ROI on capital:       {session.roi_pct:.2f}%")
        print(f"{'─'*60}")
        plot_simulation(session, args.output)

    if args.mode in ("live", "full"):
        if not args.dune_key:
            print("[warn] No DUNE_API_KEY — skipping live data pull")
            print("[hint] export DUNE_API_KEY=your_key  or pass --dune-key")
        else:
            print(f"\n[→] Fetching Dune Analytics data (last {args.days} days)...")
            client = DuneClient(args.dune_key)
            dune_data = client.fetch_all()
            plot_dune_data(dune_data, args.output)

    print(f"\n[✓] Done — outputs in {args.output}/\n")


if __name__ == "__main__":
    main()
