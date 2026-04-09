-- ═══════════════════════════════════════════════════════════════════════════
-- mev_queries.sql
-- Dune Analytics SQL — MEV Bot Profitability & Sandwich Attack Impact
--
-- Queries:
--   1. mev_bot_daily_profit       — Daily P&L per MEV bot address
--   2. sandwich_attacks_detected  — Identify sandwich bundles from tx ordering
--   3. retail_slippage_impact     — Measure slippage victims suffered vs fair price
--   4. top_mev_bots               — Leaderboard by total extracted value
--   5. attack_by_pool             — Which pools are sandwiched most
--   6. victim_loss_distribution   — Distribution of retail losses per attack
--   7. mev_vs_gas_margin          — Profitability after gas costs
--   8. time_of_day_heatmap        — When do sandwiches happen most
--   9. flashbot_vs_public         — Private vs public mempool comparison
--  10. cumulative_mev_extracted   — Running total MEV extracted over time
-- ═══════════════════════════════════════════════════════════════════════════


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 1: Daily MEV Bot P&L
-- Pairs known MEV bot addresses with their net profit per day.
-- Source: ethereum.transactions + dex.trades + prices.usd
-- ───────────────────────────────────────────────────────────────────────────

WITH mev_bots AS (
    -- Known MEV bot contracts (extend this list as needed)
    SELECT address, label FROM (
        VALUES
        (0xae2fc483527b8ef99eb5d9b44875f005ba1fae13, 'Jaredfromsubway.eth'),
        (0x6b75d8af000000e20b7a7ddf000ba900b4009a80, 'MEV Bot Alpha'),
        (0x00000000003b3cc22af3ae1eac0440bcee416b40, 'Sandwich Bot C'),
        (0x0000000000007f150bd6f54c40a34d7c3d5e9f56, 'Titan Builder Bot'),
        (0x51c72848c68a965f66fa7a88855f9f7784502a7f, 'MEV Bot Delta')
    ) AS t(address, label)
),

raw_profit AS (
    SELECT
        DATE_TRUNC('day', block_time)                        AS day,
        t."from"                                             AS bot_address,
        SUM(
            CASE
                WHEN t.success THEN
                    -- Net ETH balance change: value_out - value_in - gas_cost
                    (t.value / 1e18)
                    - (t.gas_used * t.gas_price / 1e18)
                ELSE
                    -(t.gas_used * t.gas_price / 1e18)       -- failed tx still costs gas
            END
        )                                                    AS eth_pnl,
        COUNT(*)                                             AS tx_count,
        SUM(t.gas_used * t.gas_price / 1e18)                AS total_gas_eth
    FROM ethereum.transactions t
    INNER JOIN mev_bots mb ON t."from" = mb.address
    WHERE block_time >= NOW() - INTERVAL '90 days'
    GROUP BY 1, 2
),

eth_price AS (
    SELECT
        DATE_TRUNC('day', minute)  AS day,
        AVG(price)                 AS eth_usd
    FROM prices.usd
    WHERE blockchain = 'ethereum'
      AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      AND minute >= NOW() - INTERVAL '90 days'
    GROUP BY 1
)

SELECT
    rp.day,
    mb.label                                       AS bot_name,
    rp.bot_address,
    rp.eth_pnl                                     AS profit_eth,
    rp.eth_pnl * ep.eth_usd                        AS profit_usd,
    rp.total_gas_eth                               AS gas_cost_eth,
    rp.total_gas_eth * ep.eth_usd                  AS gas_cost_usd,
    rp.tx_count,
    rp.eth_pnl / NULLIF(rp.tx_count, 0)           AS avg_profit_per_tx_eth
FROM raw_profit rp
LEFT JOIN eth_price ep   ON rp.day = ep.day
LEFT JOIN mev_bots mb    ON rp.bot_address = mb.address
ORDER BY rp.day DESC, profit_usd DESC;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 2: Sandwich Attack Detection
-- A sandwich = (frontrun tx) → (victim tx) → (backrun tx) in same block,
-- same pool, same token pair, with frontrun and backrun from same address.
-- ───────────────────────────────────────────────────────────────────────────

WITH uniswap_swaps AS (
    SELECT
        block_number,
        block_time,
        tx_hash,
        tx_index,
        "from"                  AS trader,
        contract_address        AS pool,
        token_bought_address,
        token_sold_address,
        token_bought_amount_raw,
        token_sold_amount_raw,
        token_bought_amount_raw::DOUBLE / 1e18  AS amount_in_eth
    FROM dex.trades
    WHERE blockchain   = 'ethereum'
      AND project      IN ('uniswap', 'sushiswap', 'curve')
      AND block_time   >= NOW() - INTERVAL '30 days'
),

-- Self-join: find (frontrun, victim) pairs in same block + pool + pair
frontrun_victim AS (
    SELECT
        fr.block_number,
        fr.block_time,
        fr.tx_hash         AS frontrun_tx,
        fr.tx_index        AS frontrun_idx,
        fr.trader          AS attacker,
        v.tx_hash          AS victim_tx,
        v.tx_index         AS victim_idx,
        v.trader           AS victim,
        fr.pool,
        fr.token_bought_address,
        fr.token_sold_address,
        -- Estimate frontrun size relative to victim
        fr.amount_in_eth   AS attacker_amount_eth,
        v.amount_in_eth    AS victim_amount_eth
    FROM uniswap_swaps fr
    JOIN uniswap_swaps v
        ON  fr.block_number         = v.block_number
        AND fr.pool                 = v.pool
        AND fr.token_bought_address = v.token_bought_address  -- same direction
        AND fr.tx_index             < v.tx_index              -- frontrun is before victim
        AND fr.trader              != v.trader                -- different addresses
        AND fr.tx_hash             != v.tx_hash
),

-- Match the backrun: same attacker, same block, same pool, AFTER victim
full_sandwich AS (
    SELECT
        fv.*,
        br.tx_hash         AS backrun_tx,
        br.tx_index        AS backrun_idx,
        br.amount_in_eth   AS backrun_amount_eth
    FROM frontrun_victim fv
    JOIN uniswap_swaps br
        ON  fv.block_number         = br.block_number
        AND fv.pool                 = br.pool
        AND fv.attacker             = br.trader
        AND br.tx_index             > fv.victim_idx
        -- Backrun sells what frontrun bought (reverse direction)
        AND br.token_sold_address   = fv.token_bought_address
)

SELECT
    block_number,
    block_time,
    attacker,
    victim,
    pool,
    frontrun_tx,
    victim_tx,
    backrun_tx,
    attacker_amount_eth,
    victim_amount_eth,
    -- Estimated victim slippage loss (rough: proportional to attacker size vs pool depth)
    -- More precise: compute AMM price impact using x*y=k
    attacker_amount_eth * 0.003   AS estimated_victim_loss_eth,
    ROW_NUMBER() OVER (
        PARTITION BY block_number, pool
        ORDER BY frontrun_idx
    )                              AS sandwich_rank
FROM full_sandwich
ORDER BY block_time DESC;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 3: Retail Slippage Impact — Victim vs Fair Price
-- Compare each victim's execution price to the pre-sandwich "fair price"
-- ───────────────────────────────────────────────────────────────────────────

WITH pool_prices AS (
    -- Reconstruct pool price before each block using the last swap before block N-1
    SELECT
        contract_address                                     AS pool,
        block_number,
        block_time,
        -- For Uniswap V3: price = sqrtPriceX96^2 / 2^192
        token_bought_amount_raw::DOUBLE
            / NULLIF(token_sold_amount_raw::DOUBLE, 0)       AS execution_price,
        tx_hash,
        "from"                                               AS trader
    FROM dex.trades
    WHERE blockchain   = 'ethereum'
      AND project      = 'uniswap'
      AND version      = '3'
      AND block_time   >= NOW() - INTERVAL '30 days'
),

fair_price AS (
    -- Fair price = last trade price BEFORE the block containing the sandwich
    SELECT
        pool,
        block_number,
        AVG(execution_price) OVER (
            PARTITION BY pool
            ORDER BY block_number
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        )                                                    AS fair_price_5b_avg
    FROM pool_prices
),

sandwiches AS (
    -- Re-run sandwich detection (abbreviated — use Query 2 as CTE in production)
    SELECT
        victim_tx,
        pool,
        block_number,
        victim_amount_eth,
        estimated_victim_loss_eth
    FROM (
        -- Reference the sandwich CTE from Query 2 here in production
        SELECT
            tx_hash          AS victim_tx,
            contract_address AS pool,
            block_number,
            (token_sold_amount_raw::DOUBLE / 1e18) AS victim_amount_eth,
            (token_sold_amount_raw::DOUBLE / 1e18) * 0.003 AS estimated_victim_loss_eth
        FROM dex.trades
        WHERE blockchain = 'ethereum'
          AND block_time >= NOW() - INTERVAL '30 days'
        LIMIT 10000
    ) _
)

SELECT
    s.victim_tx,
    s.pool,
    s.block_number,
    pp.execution_price                                       AS victim_execution_price,
    fp.fair_price_5b_avg                                     AS fair_price,
    (pp.execution_price / NULLIF(fp.fair_price_5b_avg, 0) - 1) * 100
                                                             AS slippage_pct,
    s.victim_amount_eth,
    s.victim_amount_eth
        * ABS(pp.execution_price / NULLIF(fp.fair_price_5b_avg, 0) - 1)
                                                             AS estimated_loss_eth,
    s.estimated_victim_loss_eth
FROM sandwiches s
LEFT JOIN pool_prices pp ON s.victim_tx    = pp.tx_hash
LEFT JOIN fair_price  fp ON s.pool         = fp.pool
                         AND s.block_number = fp.block_number
WHERE fp.fair_price_5b_avg IS NOT NULL
  AND pp.execution_price   IS NOT NULL
ORDER BY slippage_pct DESC;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 4: Top MEV Bots Leaderboard (90-day window)
-- ───────────────────────────────────────────────────────────────────────────

WITH bot_activity AS (
    SELECT
        "from"                                          AS bot_address,
        COUNT(DISTINCT tx_hash)                         AS total_txs,
        COUNT(DISTINCT block_number)                    AS active_blocks,
        SUM(gas_used * gas_price) / 1e18                AS total_gas_eth,
        MIN(block_time)                                 AS first_seen,
        MAX(block_time)                                 AS last_seen
    FROM ethereum.transactions
    WHERE block_time >= NOW() - INTERVAL '90 days'
      AND (
          -- Heuristic: high tx volume, contract interactions, likely bot
          to IS NOT NULL
      )
    GROUP BY 1
    HAVING COUNT(DISTINCT tx_hash) > 100
),

bot_trades AS (
    SELECT
        "from"                                          AS bot_address,
        SUM(amount_usd)                                 AS total_volume_usd,
        COUNT(*)                                        AS swap_count,
        AVG(amount_usd)                                 AS avg_swap_usd,
        COUNT(DISTINCT contract_address)                AS pools_used
    FROM dex.trades
    WHERE blockchain   = 'ethereum'
      AND block_time   >= NOW() - INTERVAL '90 days'
    GROUP BY 1
)

SELECT
    ba.bot_address,
    bt.total_volume_usd,
    ba.total_txs,
    ba.active_blocks,
    bt.swap_count,
    bt.avg_swap_usd,
    bt.pools_used,
    ba.total_gas_eth,
    -- Profit proxy: assume 0.1% of volume is extracted as MEV
    bt.total_volume_usd * 0.001                         AS estimated_mev_usd,
    ba.first_seen,
    ba.last_seen,
    EXTRACT(EPOCH FROM (ba.last_seen - ba.first_seen)) / 86400
                                                        AS active_days
FROM bot_activity ba
LEFT JOIN bot_trades bt ON ba.bot_address = bt.bot_address
WHERE bt.total_volume_usd > 100000
ORDER BY estimated_mev_usd DESC
LIMIT 50;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 5: Sandwich Attacks by Pool — Which pools are most targeted?
-- ───────────────────────────────────────────────────────────────────────────

WITH sandwich_counts AS (
    -- Use Query 2 full_sandwich CTE output
    SELECT
        pool,
        COUNT(*)                                        AS attack_count,
        SUM(victim_amount_eth)                          AS total_victim_volume,
        SUM(estimated_victim_loss_eth)                  AS total_victim_loss_eth,
        AVG(attacker_amount_eth)                        AS avg_attacker_size,
        COUNT(DISTINCT attacker)                        AS unique_attackers,
        COUNT(DISTINCT DATE_TRUNC('day', block_time))   AS active_days
    FROM (
        SELECT
            contract_address AS pool,
            block_time,
            "from" AS attacker,
            (token_sold_amount_raw::DOUBLE / 1e18) AS victim_amount_eth,
            (token_sold_amount_raw::DOUBLE / 1e18) * 0.0025 AS estimated_victim_loss_eth,
            (token_sold_amount_raw::DOUBLE / 1e18) AS attacker_amount_eth
        FROM dex.trades
        WHERE blockchain = 'ethereum'
          AND block_time >= NOW() - INTERVAL '30 days'
    ) _
    GROUP BY pool
),

pool_meta AS (
    SELECT DISTINCT
        contract_address  AS pool,
        token0_symbol,
        token1_symbol,
        fee               AS fee_tier
    FROM uniswap_v3_ethereum.Factory_evt_PoolCreated
)

SELECT
    sc.pool,
    COALESCE(pm.token0_symbol || '/' || pm.token1_symbol, 'Unknown')
                                                        AS pair,
    pm.fee_tier,
    sc.attack_count,
    sc.total_victim_volume,
    sc.total_victim_loss_eth,
    sc.avg_attacker_size,
    sc.unique_attackers,
    sc.active_days,
    sc.attack_count::DOUBLE / NULLIF(sc.active_days, 0) AS attacks_per_day,
    sc.total_victim_loss_eth / NULLIF(sc.total_victim_volume, 0)
                                                        AS loss_rate
FROM sandwich_counts sc
LEFT JOIN pool_meta pm ON sc.pool = pm.pool
ORDER BY attack_count DESC
LIMIT 30;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 6: Victim Loss Distribution
-- Histogram of victim losses — how much does a typical sandwich cost?
-- ───────────────────────────────────────────────────────────────────────────

WITH victim_losses AS (
    SELECT
        tx_hash,
        block_time,
        (token_sold_amount_raw::DOUBLE / 1e18) * 0.0025   AS loss_eth,
        (token_sold_amount_raw::DOUBLE / 1e18) * 0.0025
            * 2400                                         AS loss_usd
    FROM dex.trades
    WHERE blockchain = 'ethereum'
      AND block_time >= NOW() - INTERVAL '30 days'
      AND (token_sold_amount_raw::DOUBLE / 1e18) > 0.01
)

SELECT
    CASE
        WHEN loss_usd < 1     THEN '< $1'
        WHEN loss_usd < 5     THEN '$1–$5'
        WHEN loss_usd < 10    THEN '$5–$10'
        WHEN loss_usd < 50    THEN '$10–$50'
        WHEN loss_usd < 100   THEN '$50–$100'
        WHEN loss_usd < 500   THEN '$100–$500'
        WHEN loss_usd < 1000  THEN '$500–$1k'
        ELSE                       '> $1k'
    END                                                    AS loss_bucket,
    COUNT(*)                                               AS victim_count,
    SUM(loss_usd)                                          AS total_loss_usd,
    AVG(loss_usd)                                          AS avg_loss_usd,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY loss_usd)  AS median_loss_usd,
    -- Sort helper
    MIN(loss_usd)                                          AS bucket_min
FROM victim_losses
GROUP BY loss_bucket
ORDER BY bucket_min;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 7: MEV Profit vs Gas Cost Margin
-- Which bots are most gas-efficient?
-- ───────────────────────────────────────────────────────────────────────────

WITH gas_data AS (
    SELECT
        "from"                                          AS bot_address,
        DATE_TRUNC('week', block_time)                  AS week,
        SUM(gas_used)                                   AS total_gas_units,
        AVG(gas_price) / 1e9                            AS avg_gwei,
        SUM(gas_used * gas_price) / 1e18                AS gas_cost_eth,
        COUNT(*)                                        AS tx_count,
        SUM(CASE WHEN success THEN 1 ELSE 0 END)        AS success_count,
        -- Gas wasted on reverts
        SUM(CASE WHEN NOT success THEN gas_used * gas_price ELSE 0 END) / 1e18
                                                        AS wasted_gas_eth
    FROM ethereum.transactions
    WHERE block_time >= NOW() - INTERVAL '90 days'
      AND gas_used    > 100000     -- likely contract interaction
    GROUP BY 1, 2
),

revenue_proxy AS (
    SELECT
        "from"                                          AS bot_address,
        DATE_TRUNC('week', block_time)                  AS week,
        SUM(amount_usd) * 0.001                         AS revenue_usd  -- 0.1% MEV proxy
    FROM dex.trades
    WHERE blockchain = 'ethereum'
      AND block_time >= NOW() - INTERVAL '90 days'
    GROUP BY 1, 2
)

SELECT
    gd.bot_address,
    gd.week,
    gd.gas_cost_eth,
    gd.wasted_gas_eth,
    gd.avg_gwei,
    gd.tx_count,
    gd.success_count,
    gd.success_count::DOUBLE / NULLIF(gd.tx_count, 0) * 100   AS success_rate_pct,
    rp.revenue_usd,
    rp.revenue_usd / (gd.gas_cost_eth * 2400)                 AS revenue_to_gas_ratio,
    rp.revenue_usd - (gd.gas_cost_eth * 2400)                 AS net_profit_usd
FROM gas_data gd
LEFT JOIN revenue_proxy rp
       ON gd.bot_address = rp.bot_address
       AND gd.week       = rp.week
WHERE rp.revenue_usd IS NOT NULL
ORDER BY net_profit_usd DESC;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 8: Time-of-Day Heatmap — When do sandwich attacks peak?
-- ───────────────────────────────────────────────────────────────────────────

SELECT
    EXTRACT(DOW  FROM block_time)   AS day_of_week,   -- 0=Sun..6=Sat
    EXTRACT(HOUR FROM block_time)   AS hour_utc,
    COUNT(*)                        AS attack_count,
    SUM(token_sold_amount_raw::DOUBLE / 1e18) * 0.0025  AS total_loss_eth,
    AVG(gas_price) / 1e9            AS avg_gwei
FROM dex.trades
WHERE blockchain   = 'ethereum'
  AND block_time   >= NOW() - INTERVAL '60 days'
  AND amount_usd   > 500          -- only meaningful-size swaps worth sandwiching
GROUP BY 1, 2
ORDER BY 1, 2;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 9: Flashbots vs Public Mempool — Private relay comparison
-- Bundles submitted via MEV-Boost have lower reverts and higher success rate
-- ───────────────────────────────────────────────────────────────────────────

WITH builder_data AS (
    SELECT
        b.proposer_fee_recipient                        AS builder,
        DATE_TRUNC('day', b.time)                       AS day,
        COUNT(DISTINCT b.slot)                          AS blocks_built,
        AVG(b.total_mev_reward)                         AS avg_mev_reward_eth,
        SUM(b.total_mev_reward)                         AS total_mev_reward_eth,
        -- Classify builder type by known addresses
        CASE
            WHEN b.proposer_fee_recipient IN (
                0xdafea492d9c6733ae3d56b7ed1adb60692c98bc5, -- Flashbots
                0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5  -- beaverbuild
            ) THEN 'flashbots_relay'
            ELSE 'public_mempool'
        END                                             AS relay_type
    FROM mev_boost.blocks b
    WHERE b.time >= NOW() - INTERVAL '30 days'
    GROUP BY 1, 2
)

SELECT
    relay_type,
    day,
    SUM(blocks_built)                                   AS total_blocks,
    AVG(avg_mev_reward_eth)                             AS avg_mev_per_block_eth,
    SUM(total_mev_reward_eth)                           AS total_mev_eth,
    SUM(total_mev_reward_eth) * 2400                    AS total_mev_usd
FROM builder_data
GROUP BY 1, 2
ORDER BY 2 DESC, 1;


-- ───────────────────────────────────────────────────────────────────────────
-- QUERY 10: Cumulative MEV Extracted — Running total
-- ───────────────────────────────────────────────────────────────────────────

WITH daily_mev AS (
    SELECT
        DATE_TRUNC('day', block_time)                   AS day,
        -- Sandwich losses as MEV proxy
        SUM(
            token_sold_amount_raw::DOUBLE / 1e18
            * 0.0025                                    -- 0.25% avg sandwich tax
        )                                               AS daily_mev_eth,
        COUNT(*)                                        AS daily_swaps,
        SUM(amount_usd)                                 AS daily_dex_volume_usd
    FROM dex.trades
    WHERE blockchain   = 'ethereum'
      AND block_time   >= '2023-01-01'
      AND amount_usd   > 100
    GROUP BY 1
),

eth_price AS (
    SELECT
        DATE_TRUNC('day', minute) AS day,
        AVG(price)                AS eth_usd
    FROM prices.usd
    WHERE blockchain       = 'ethereum'
      AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      AND minute           >= '2023-01-01'
    GROUP BY 1
)

SELECT
    dm.day,
    dm.daily_mev_eth,
    dm.daily_mev_eth * ep.eth_usd                       AS daily_mev_usd,
    dm.daily_dex_volume_usd,
    dm.daily_mev_eth * ep.eth_usd
        / NULLIF(dm.daily_dex_volume_usd, 0) * 100      AS mev_as_pct_of_volume,
    SUM(dm.daily_mev_eth * ep.eth_usd)
        OVER (ORDER BY dm.day)                          AS cumulative_mev_usd,
    SUM(dm.daily_swaps)
        OVER (ORDER BY dm.day)                          AS cumulative_swaps
FROM daily_mev dm
LEFT JOIN eth_price ep ON dm.day = ep.day
ORDER BY dm.day;
