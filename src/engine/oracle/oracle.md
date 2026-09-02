# Capital Oracle Technical Specification

## 1. Variables and Inputs

### 1.1 Per-Asset Market Data and Configured Bounds
- `B = [b_1, b_2, ..., b_T]`: Daily bar history where each bar `b_t` contains high `H_t`, low `L_t`, and close `C_t`.
- `P_current`: Current live market price (top-of-book bid/ask with last bar close fallback).
- `S_target`: Configured target survival percentage (`target_survival`, e.g. 0.85).
- `S_min`: Minimum survival percentage for strategy activation (`min_active_dsurv`, e.g. 0.0).
- `q_min`: Base order quantity (`qty`).
- `m_q`: Order quantity multiplier (`qty_cap_mult`). Upper bound: `q_max = q_min * m_q`.
- `[g_min, g_max]`: Allowed grid interval range in percent (`grid_interval`).
- `f`: Venue maker fee fraction (`maker_fee`).

### 1.2 Per-Venue Account Capital
- `Q_free`: Free, uncommitted quote currency balance on the exchange account.
- `Q_resting_i`: Quote currency currently locked in resting buy orders for strategy `i`.
- `Q_venue`: Total venue quote capital available:
  `Q_venue = Q_free + sum_{i=1}^N Q_resting_i`
- `tasks`: Active strategies `1, 2, ..., N` assigned to the venue account, indexed strictly by configuration presentation order (index 1 is highest priority).

---

## 2. Historical References

Calculated over the complete daily bar series `B`:

1. **All-Time High (ATH)**:
   `ATH = max_{1 <= t <= T} H_t`

2. **All-Time Low (ATL)**:
   `ATL = min_{1 <= t <= T} L_t`

3. **Running Close Peak (`P_close_peak_t`)**:
   `P_close_peak_t = max_{1 <= s <= t} C_s`

4. **Lifetime Maximum Drawdown (MDD)**:
   Worst peak-to-trough decline measured from the running close peak to the bar low:
   `MDD = min(0.999999, max_{1 <= t <= T} (1.0 - L_t / P_close_peak_t))`

---

## 3. Runway, Regimes, and Survival Floor

1. **Target Drawdown to Fund**:
   `TargetDrop = MDD * S_target`

2. **Floor Price**:
   `P_floor = ATH * (1.0 - TargetDrop) = ATH * (1.0 - MDD * S_target)`

3. **Realized Drawdown**:
   `DD_realized = clamp(0.0, 1.0, (ATH - P_current) / ATH)`

4. **Market Aggressiveness Ratio (`alpha`)**:
   - If `MDD > 0.0`:
     `alpha = clamp(0.0, 1.0, DD_realized / MDD)`
   - If `MDD <= 0.0` and `DD_realized > 0.0`:
     `alpha = 1.0`
   - Otherwise:
     `alpha = 0.0`

5. **Regime Classification and Effective Funded Floor (`P_funded_floor`)**:
   - If `P_current > P_floor`:
     `Regime = Normal`
     `P_funded_floor = P_floor`
   - Else if `DD_realized >= MDD` and `P_current <= ATL`:
     `Regime = Unprecedented_lows`
     `P_funded_floor = ATL`
   - Else:
     `Regime = Floor_extension`
     `P_funded_floor = ATL`

---

## 4. Parameter Optimization

Resolves grid interval `g` and buy quantity `q` over `[g_min, g_max] x [q_min, q_max]`.

### 4.1 Single-Asset Geometric Ladder Survival
For candidate `(g, q)`:
- Geometric step multiplier:
  `step = 1.0 - g / 100.0`
- Rung prices:
  `p_k = P_current * step^k` for `k = 1, 2, ...`
- Quote cost per rung:
  `C_k = q * p_k * (1.0 + f)`

Survival metric `d_surv` down to `P_funded_floor` with quote budget `Q`:
- Rungs `k` are funded while `p_k >= P_funded_floor`:
  - If `Q` exhausts at rung `k` before reaching `P_funded_floor`:
    `d_surv = clamp(0.0, 1.0, (P_current - p_k) / (P_current - P_funded_floor))`
  - If all rungs down to `P_funded_floor` are funded with leftover quote:
    `d_surv = 1.0 + min(1.0, remaining_quote / C_{k+1})`

### 4.2 Resolution Branches
1. **Unprecedented Lows**:
   - Emit `(gi_max, q_min)`, `branch = Unreachable`.
2. **Surplus Branch**:
   - Test most aggressive corner: `(g_min, q_max)`.
   - If `d_surv(g_min, q_max) > 1.0`:
     Emit `(g_min, q_max)`, `branch = Surplus`.
3. **Reachable Branch**:
   - Evaluate a 24x24 discrete grid of `(g, q)` pairs.
   - Filter candidates satisfying full floor funding: `d_surv >= 1.0`.
   - Score surviving candidates:
     `q_norm = (q - q_min) / (q_max - q_min)`
     `g_tight = (g_max - g) / (g_max - g_min)`
     `score_agg = (q_norm + g_tight) / 2.0`
     `score_cons = 1.0 - score_agg`
     `score = alpha * score_agg + (1.0 - alpha) * score_cons`
   - Candidate with maximum `score` is selected.
   - Ties broken by: larger `q`, then tighter `g`.
   - If candidate found: `branch = Reachable`.
4. **Unreachable Branch**:
   - If no candidate satisfies `d_surv >= 1.0`:
     Emit `(gi_max, q_min)`, `branch = Unreachable`.

### 4.3 Initial Activity Gate
`active = (d_surv >= S_min or has_resting_buy) and (Q_available >= q * P_current)`

---

## 5. Multi-Strategy Priority Drawdown Simulation

Calculates live `D_surv` across all active strategies on a venue account.

### 5.1 Simulation Rules
- Active strategies sorted by config presentation priority `1, 2, ..., N`.
- In each round, every active strategy attempts to execute 1 order down its geometric ladder.
- Execution within each round proceeds sequentially by priority (strategy 1 funds order before strategy 2).
- When available quote cannot cover the next order in priority sequence, simulation terminates immediately (priority capital exhaustion).
- Boundary limit: rungs terminate if an asset drops past 99.9% of its price (`step^(k+1) <= 0.001`) or exceeds 1,000 rungs.

### 5.2 Algorithm
```
Input:
  total_quote = Q_venue
  strategies = [s_1, s_2, ..., s_N] sorted by priority ascending
  counts = map of strategy_id -> 0
  remaining = total_quote
  keep_running = true

While keep_running and remaining > 1e-9:
  any_funded_this_round = false
  For each strategy s in strategies:
    If keep_running:
      k = counts[s.id]
      If k >= 1000:
        continue
      step = max(1e-6, 1.0 - s.grid_interval / 100.0)
      drop_factor = step^(k + 1)
      If drop_factor <= 0.001:
        continue
      p_next = s.current * drop_factor
      cost = s.buy_qty * p_next * (1.0 + s.maker_fee)
      If cost <= remaining + 1e-9:
        remaining = remaining - cost
        counts[s.id] = k + 1
        any_funded_this_round = true
      Else:
        keep_running = false

  If not any_funded_this_round:
    keep_running = false
```

### 5.3 Output Metric
For each strategy `s`:
`P_funded = s.current * (1.0 - s.grid_interval / 100.0)^k`
`D_surv = (s.current - P_funded) / s.current = 1.0 - (1.0 - s.grid_interval / 100.0)^k`

For inactive strategies:
`D_surv = 0.0`

---

## 6. Capital Allocation and Priority Cascades

### 6.1 Greedy Pass-Down
1. Iterates strategies in priority order `1, 2, ..., N`.
2. Immediate quote required: `Need_i = q_i * P_current_i`.
3. If strategy `i` is active and has no resting buy:
   - If `Need_i <= remaining_quote`:
     `remaining_quote = remaining_quote - Need_i`
   - Else:
     Strategy `i` is starved. `remaining_quote` passes down untouched to junior strategies.

### 6.2 Cancellation Cascade
Triggered when a senior strategy `i` is starved (`Need_i > remaining_quote`) and its resolved sizing satisfied its effective target survival:
1. Identify lower-priority strategies `j > i` with resting buy orders (`Q_resting_j > 0`).
2. Sort candidates by priority descending (least senior first).
3. Accumulate resting buy quote from junior strategies:
   Stop as soon as `sum Q_resting_j + remaining_quote >= Need_i`.
4. If the deficit is covered:
   Flag identified junior orders with `cancel_resting_buys = true`.
5. If cancelling all junior orders still cannot cover `Need_i`:
   Cancel nothing. Junior orders are preserved.
