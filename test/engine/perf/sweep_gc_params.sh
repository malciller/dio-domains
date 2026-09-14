#!/bin/bash

# GC parameter sweep for gc_hammer.exe.
#
# Only two OCAMLRUNPARAM knobs materially affect this runtime:
#
#   s  minor_heap_size, in WORDS (8 bytes per word) per domain.
#      256k words = 2 MB (the deployed effective value, set from config.json's
#      gc block via Gc.set - which overrides OCAMLRUNPARAM).
#      1M words = 8 MB, 8M words = 64 MB. OxCaml default: 1M words (8 MB).
#      NB: config.json's gc.minor_heap_size wins over OCAMLRUNPARAM at runtime,
#      so this sweep's s only mirrors production when it matches that value.
#   o  space_overhead, the major-GC pacing percentage. Higher = the collector
#      tolerates more floating garbage before working harder, i.e. fewer but
#      larger major cycles. OxCaml default: 80.
#
# Deliberately NOT swept / set:
#   a  allocation_policy. This field is unavailable in OCaml 5 (always 0);
#      setting it is silently ignored, so sweeping it measures nothing.
#   h  not an OCaml runtime parameter at all; silently ignored.
#   w  window_size; only affects custom-memory (bigarray) accounting and has
#      no observable effect via Gc.get here.
#
# The uppercase O (max_overhead, compaction trigger) is intentionally left at
# its default. It is not a latency knob; the deployed image sets O=1000000 to
# disable compaction, which this dev sweep does not need to explore.

set -euo pipefail

echo "Building gc_hammer.exe ..."
dune build test/engine/perf/gc_hammer.exe || exit 1

EXE="_build/default/test/engine/perf/gc_hammer.exe"

SIZES=("256k" "512k" "1M" "2M" "4M" "8M")
OVERHEADS=("40" "80" "120" "200")

RESULTS_FILE="gc_sweep_results.md"

{
  echo "# GC Parameter Sweep Results"
  echo ""
  echo "Host: $(uname -a)"
  echo ""
  echo "| Minor Heap (s) | space_overhead (o) | Avg P99 (us) | Max P999 (us) | Minor GCs | Major GCs | Promoted (MB) | Wall (s) |"
  echo "|----------------|--------------------|--------------|---------------|-----------|-----------|---------------|----------|"
} > "$RESULTS_FILE"

echo "Starting GC parameter sweep ..."
echo ""

for s in "${SIZES[@]}"; do
  for o in "${OVERHEADS[@]}"; do
    export OCAMLRUNPARAM="s=$s,o=$o"
    echo "Testing config: s=$s, o=$o"

    OUTPUT=$($EXE)

    P99=$(echo "$OUTPUT" | awk -F: '/SWEEP_METRIC_P99/ {gsub(/ /,"",$2); print $2}')
    MAX_P999=$(echo "$OUTPUT" | awk -F: '/SWEEP_METRIC_MAX_P999/ {gsub(/ /,"",$2); print $2}')
    MINOR_GC=$(echo "$OUTPUT" | awk -F: '/SWEEP_METRIC_MINOR_GC/ {gsub(/ /,"",$2); print $2}')
    MAJOR_GC=$(echo "$OUTPUT" | awk -F: '/SWEEP_METRIC_MAJOR_GC/ {gsub(/ /,"",$2); print $2}')
    PROMOTED=$(echo "$OUTPUT" | awk -F: '/SWEEP_METRIC_PROMOTED_MB/ {gsub(/ /,"",$2); print $2}')
    WALL=$(echo "$OUTPUT" | awk '/Total Wall Time/ {print $4}' | tr -d 's')

    echo "  -> P99: ${P99}us | Max P999: ${MAX_P999}us | minor: ${MINOR_GC} | major: ${MAJOR_GC} | promoted: ${PROMOTED}MB | wall: ${WALL}s"
    echo "| $s | $o | $P99 | $MAX_P999 | $MINOR_GC | $MAJOR_GC | $PROMOTED | $WALL |" >> "$RESULTS_FILE"

    # Let the machine settle so one configuration does not bleed into the next.
    sleep 1
  done
done

echo ""
echo "Sweep complete. Results written to $RESULTS_FILE."
cat "$RESULTS_FILE"
