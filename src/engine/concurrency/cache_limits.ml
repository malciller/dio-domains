(** Named bounds for the engine's caches, with the reasoning behind each.

    These were previously bare literals scattered across a dozen modules — a
    `Hashtbl.create 32` in a venue feed, a `64` in an order executor, a `512` in a UI
    graph — with nothing recording what the number was protecting against or how it was
    arrived at. That makes them impossible to review and impossible to change safely:
    nobody can tell whether a bound is a measured working-set size, a deliberate safety
    margin, or a guess.

    Each constant below therefore carries its justification. When you change one, change
    the comment with it, and check the corresponding row on the dashboard's [caches]
    section afterwards — every entry-count bound is now observable there, which is what
    makes tuning these an informed decision rather than a guess.

    **Deliberately not runtime-configurable.** Making these settable from config.json
    would let an operator shrink a safety cap below the engine's real working set and turn
    a bounded, diagnosable eviction problem into silent data loss — order-to-symbol
    mappings vanishing mid-session, fee lookups silently falling back to venue defaults. A
    bound whose failure mode is wrong behaviour under trading load is not something to
    expose as a tunable. If a deployment genuinely needs different sizes, that is a code
    change with a benchmark behind it, which is the right cost. *)

(* --- copy-on-write table bucket counts ------------------------------------------------

   [Cow_table] bucket counts should sit around 2-8x the expected live key count. Too few
   makes each write copy a large bucket; too many wastes an array of atomics and hurts
   locality. The cost of getting this wrong is mild — a bigger copy per write — so these
   are generous rather than tuned. *)

(** Kraken pair metadata: the venue's full pair universe, several hundred symbols, read on
    every strategy decision and every order encode. *)
let kraken_pair_cache_buckets = 64

(** Kraken venue-level fee table: one entry per configured symbol. *)
let kraken_fee_cache_buckets = 32

(** Oracle-resolved fee table: (exchange, symbol) pairs across configured assets. *)
let oracle_fee_cache_buckets = 16

(** Strategy expression intern table. Strategy files use a small fixed set of fact and
    state keys; 256 buckets means the copy on a miss is a couple of entries even if a
    strategy file grows well beyond anything seen in practice. *)
let strategy_intern_buckets = 256

(* --- bounded index capacities --------------------------------------------------------- *)

(** Startup capacity for the per-venue order-id indexes, applied before any tuning pass.

    This is a ceiling that exists from the first insert, not a tuning knob: it exists so
    that a feed whose startup snapshot errors, is skipped, or races cannot grow the index
    without bound on a key that never stops arriving. The index is retuned to observed
    volume once the snapshot lands. Generous, because no venue should have this many open
    orders — the number is here to be unreachable, not to be reached. *)
let order_index_startup_cap = 65_536

(** Floor for a tuned order-id index cap. Below this a venue's book would thrash. *)
let order_index_min_cap = 32

(** Hyperliquid's floor is higher than the other venues': its open-order payloads are
    larger and the snapshot is known to be bigger, so its observed startup volume is
    expected to be well above the common floor. *)
let order_index_hyperliquid_min_cap = 1024

(* --- TTL / sweep intervals ------------------------------------------------------------ *)

(** Minimum seconds between [Fee_cache] expiry sweeps. The fee keyspace is bounded by
    configuration, so a sweep is memory reclamation rather than correctness — reads
    already reject stale entries — and there is nothing to gain from doing it more often. *)
let fee_cache_sweep_interval = 60.0

(** Default fee entry TTL. Long enough that a strategy cycle never races the refresh,
    short enough that a venue changing its fee schedule is picked up without a restart. *)
let fee_cache_ttl = 600.0
