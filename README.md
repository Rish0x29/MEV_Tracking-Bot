# mev-toolkit

MEV Bot Profitability Dashboard — Sandwich attack analysis with Dune Analytics SQL, Python AMM simulator, and live ops dashboard.

```
mev-toolkit/
├── queries/
│   └── mev_queries.sql     # 10 Dune Analytics SQL queries
├── bot/
│   └── mev_bot.py          # Python: AMM sandwich simulator + Dune client + charts
└── dashboard/
    └── index.html          # Unified ops dashboard (open in browser)
```

---

## 1. SQL Queries (Dune Analytics)

Paste each query into [dune.com/queries/new](https://dune.com/queries/new). All run on Ethereum mainnet.

| Query | What it does |
|-------|-------------|
| `Q1` mev_bot_daily_profit | Daily P&L per tracked bot address |
| `Q2` sandwich_attacks_detected | Self-join detection: frontrun → victim → backrun |
| `Q3` retail_slippage_impact | Victim execution vs 5-block rolling fair price |
| `Q4` top_mev_bots | 90-day leaderboard by estimated MEV extracted |
| `Q5` attack_by_pool | Which pools are sandwiched most (attacks/day, loss rate) |
| `Q6` victim_loss_distribution | Histogram: how much does a typical sandwich cost victims? |
| `Q7` mev_vs_gas_margin | Bot gas efficiency: revenue/gas ratio, wasted gas on reverts |
| `Q8` time_of_day_heatmap | Attack frequency by hour × weekday (UTC) |
| `Q9` flashbots_vs_public | Private relay vs public mempool MEV per block |
| `Q10` cumulative_mev_extracted | Running total MEV since 2023-01-01 |

**After publishing your queries on Dune**, copy the query IDs into `mev_bot.py`:
```python
DUNE_QUERY_IDS = {
    "daily_profit":  YOUR_Q1_ID,
    "sandwich_attacks": YOUR_Q2_ID,
    ...
}
```

---

## 2. Python MEV Bot Analyzer

### Install
```bash
cd bot
pip install numpy pandas matplotlib requests python-dotenv
```

### Run simulation (no API key needed)
```bash
# Simulate 1000 blocks of sandwich attacks
python mev_bot.py --mode sim --capital 10 --blocks 1000

# Aggressive gas strategy
python mev_bot.py --mode sim --capital 20 --blocks 5000 --gas aggressive

# Output to folder
python mev_bot.py --mode sim --blocks 2000 --output ./reports/
```

### Run with live Dune data
```bash
export DUNE_API_KEY=your_key_here
python mev_bot.py --mode live --days 30

# Full: simulate + Dune pull + all charts
python mev_bot.py --mode full --capital 10 --blocks 1000 --days 30
```

### Outputs
- `mev_report.png`   — 6-panel simulation dashboard (dark matplotlib)
- `dune_report.png`  — 6-panel Dune data visualization
- `mev_summary.json` — machine-readable P&L summary for the dashboard

### Key math implemented
| Component | Formula |
|-----------|---------|
| AMM output | `dy = ry·dx·(1−fee) / (rx + dx·(1−fee))` |
| Optimal frontrun | `dx_f = √(rx·(rx + dx_victim)) − rx` |
| Victim slippage | `(fair_output − actual_output) / fair_output × 100` |
| Gas cost | `gas_units × gwei × 1e-9 ETH` |
| Net profit | `backrun_output − frontrun_input − gas_cost` |

---

## 3. Dashboard

Open `dashboard/index.html` directly in any browser. Four tabs:

- **Overview** — MEV leaderboard, cumulative extraction chart, scanner terminal
- **Sandwich Attacks** — live feed, AMM anatomy simulator, slippage histogram, heatmap, pool ranking
- **Profitability** — parametric P&L simulator, 30-day projection, gas strategy comparison
- **SQL Queries** — index of all 10 Dune queries with descriptions and source tables

### Connect to live data
Load `mev_summary.json` from the Python bot into the dashboard:
```js
// In dashboard/index.html, add to <script>:
fetch('./mev_summary.json')
  .then(r => r.json())
  .then(data => {
    document.getElementById('ov-profit').textContent = '$' + data.total_net_usd.toLocaleString();
    document.getElementById('ov-success').textContent = data.success_rate_pct + '%';
    // ... etc
  });
```

---

## Security & Legal Note

MEV bots extract value from ordinary traders. Running a sandwich bot:
- Is technically legal but ethically contested
- May expose you to front-running by other bots
- Requires MEV-Boost / Flashbots integration for competitive success
- All reverted txs still cost gas

This toolkit is for research and analytics purposes.
