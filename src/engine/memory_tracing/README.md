# Memory Tracing Module

A comprehensive memory leak detection and profiling system for OCaml applications, designed specifically for high-performance trading engines and multicore systems.

## Overview

The Memory Tracing module provides deep insights into memory usage patterns, automatically detects memory leaks, and monitors domain-specific allocation behavior. It integrates seamlessly with the Dio Trading Engine to help identify performance bottlenecks and memory issues before they impact production systems.

### Key Capabilities

- **Automatic Leak Detection**: Identifies memory leaks through growth pattern analysis
- **Domain Profiling**: Tracks memory usage per OCaml domain in multicore applications
- **Runtime Events**: Captures OCaml garbage collector events and allocation patterns
- **Configurable Instrumentation**: Tracks Hashtbl, Array, List, and custom data structures
- **Comprehensive Reporting**: Generates detailed reports in text, JSON, and summary formats
- **Performance Optimized**: Configurable sampling rates and selective tracking to minimize overhead

## Features

### Leak Detection
- Monitors data structure growth over time
- Calculates leak severity (Low, Medium, High, Critical)
- Provides actionable recommendations for leak mitigation
- Tracks allocation sites with full backtraces

### Domain Profiling
- Per-domain memory usage tracking
- Allocation rate monitoring
- Domain lifecycle management
- Multicore-aware statistics collection

### Runtime Events
- OCaml GC event capture
- Minor/major collection tracking
- Domain spawn/terminate events
- Memory pressure analysis

### Data Structure Tracking
- Hashtbl creation/destruction monitoring
- Array allocation tracking
- Large list monitoring (configurable threshold)
- Custom structure instrumentation API

### Domain Profiling Details

Domain profiling provides comprehensive per-domain memory monitoring:

- **Automatic GC Trigger**: 30-second fallback GC to ensure fresh statistics
- **Allocation History**: Tracks last 100 allocations per domain (auto-cleaned every 5 minutes)
- **Timeout Protection**: 100ms timeouts prevent hanging during mutex contention
- **Memory Pressure Detection**: Monitors heap usage and free ratios
- **Lifecycle Tracking**: Monitors domain spawn/terminate events

Domain profiling automatically manages resources with:
- **Periodic Cleanup**: Removes allocation history older than 1 hour
- **Graceful Degradation**: Continues operation even when mutexes are contended
- **Background Maintenance**: Non-blocking cleanup threads

## Quick Start

### Enable Memory Tracing

Set the environment variable before starting your application:

```bash
export DIO_MEMORY_TRACING=true
./your_application
```

Or pass as command-line argument:

```bash
./your_application DIO_MEMORY_TRACING=true
```

### Expected Output

When enabled, you'll see initialization messages:

```
INFO [memory_tracing] Initializing memory tracing system...
INFO [memory_config] Memory tracing enabled with sample rate 1.00
INFO [memory_tracing] Memory tracing system initialized (reporting every 300 seconds)
INFO [leak_detector] Leak detector initialized
INFO [domain_profiler] Domain profiler initialized
```

### Basic Monitoring

The system automatically starts periodic reporting. You'll see memory statistics in logs:

```
INFO [memory_monitoring] GC Stats: heap=6.2MB live=4.3MB free=1.9MB fragments=2220 compactions=0
INFO [memory_monitoring] Feed telemetry: subs=0/0/0 cache=24 mem=0.024MB growth=0.000000MB/s
```

## Configuration

Memory tracing is configured entirely through environment variables for easy deployment and runtime adjustment.

### Core Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `DIO_MEMORY_TRACING` | `false` | Enable/disable the entire memory tracing system |
| `DIO_MEMORY_SAMPLE_RATE` | `1.0` | Sampling rate (1.0 = 100%, 0.1 = 10% of allocations) |
| `DIO_MEMORY_REPORT_INTERVAL` | `300` | Report interval in seconds (default: 5 minutes) |

### Tracking Options

| Variable | Default | Description |
|----------|---------|-------------|
| `DIO_MEMORY_TRACK_HASHTABLES` | `true` | Track Hashtbl operations |
| `DIO_MEMORY_TRACK_ARRAYS` | `true` | Track Array operations |
| `DIO_MEMORY_TRACK_LISTS` | `false` | Track large lists (>1000 elements) |
| `DIO_MEMORY_TRACK_CUSTOM` | `true` | Track custom structures via API |

### Advanced Features

| Variable | Default | Description |
|----------|---------|-------------|
| `DIO_MEMORY_RUNTIME_EVENTS` | `true` | Enable OCaml runtime event capture |
| `DIO_MEMORY_DOMAIN_PROFILING` | `true` | Enable per-domain memory profiling |
| `DIO_MEMORY_LEAK_DETECTION` | `true` | Enable automatic leak detection |
| `DIO_MEMORY_SENSITIVITY` | `Medium` | Leak detection sensitivity (`Low`, `Medium`, `High`) |
| `DIO_MEMORY_MAX_HISTORY` | `10000` | Maximum allocation history size |
| `DIO_MEMORY_MAX_REPORT_ALLOCATIONS` | `0` | Maximum allocations to show in reports (0 = unlimited) |
| `DIO_MEMORY_REPORT_DIRECTORY` | `"memory_trace"` | Directory for generated report files |
| `DIO_MEMORY_REPORT_PREFIX` | `"memory_trace"` | Prefix for generated report filenames |
| `DIO_MEMORY_RUNTIME_EVENTS_DIRECTORY` | `"memory_trace"` | Directory for runtime events files |

**Note**: Boolean environment variables accept: `true`/`1`/`yes`/`on` and `false`/`0`/`no`/`off`

**Runtime Events**: When enabled, sets `OCAML_RUNTIME_EVENTS_DIR` environment variable automatically.

### Advanced Features

#### Tracking Deferral Mechanism

The system supports temporarily deferring allocation tracking to avoid deadlocks during critical phases:

```ocaml
(* Defer tracking during critical operations *)
Memory_tracing.defer_allocation_tracking ()
(* All allocation tracking is paused *)

(* Perform critical operations that might cause deadlocks *)
perform_critical_operation ()

(* Resume normal tracking *)
Memory_tracing.resume_allocation_tracking ()
```

Use deferral when:
- Performing operations that acquire multiple locks
- During domain initialization/shutdown
- In signal handlers or other critical sections
- When calling external C functions that might block

#### Automatic Limits and Cleanup

The system automatically manages memory usage with built-in limits:

- **Active Allocation Limit**: Maximum 10,000 active allocations tracked
- **Growth Tracker Limit**: Maximum 500 concurrent growth trackers
- **Allocation History**: 100 allocations per domain, 20 samples per growth tracker
- **Stale Cleanup**: Automatic removal of allocations older than 24 hours
- **Tracker Cleanup**: Removal of inactive growth trackers after 1 hour

When limits are reached, the oldest entries are automatically removed to prevent unbounded memory growth.

#### Mutex Contention Handling

All operations use `try_lock` with timeouts to prevent deadlocks:

- **100ms timeout** for domain statistics operations
- **Graceful degradation** when mutexes are contended
- **Non-blocking allocation tracking** during high contention
- **Background cleanup** that doesn't interfere with normal operation

### Example Configuration

For production monitoring with low overhead:

```bash
export DIO_MEMORY_TRACING=true
export DIO_MEMORY_SAMPLE_RATE=0.5          # Sample 50% of allocations
export DIO_MEMORY_REPORT_INTERVAL=600      # Report every 10 minutes
export DIO_MEMORY_TRACK_LISTS=false        # Skip list tracking
export DIO_MEMORY_SENSITIVITY=Medium       # Balanced leak detection
```

For development debugging:

```bash
export DIO_MEMORY_TRACING=true
export DIO_MEMORY_SAMPLE_RATE=1.0          # Sample everything
export DIO_MEMORY_REPORT_INTERVAL=60       # Report every minute
export DIO_MEMORY_TRACK_LISTS=true         # Track all lists
export DIO_MEMORY_SENSITIVITY=High         # Aggressive leak detection
```

## Programmatic API

### Basic Functions

```ocaml
open Dio_memory_tracing.Memory_tracing

(* Initialize tracing (usually done automatically) *)
let tracing_enabled = Memory_tracing.init ()
(* Returns: true if initialized successfully, false if disabled or failed *)

(* Check if tracing is currently enabled *)
let enabled = Memory_tracing.is_enabled ()

(* Shutdown the tracing system cleanly *)
let () = Memory_tracing.shutdown ()

(* Reconfigure tracing from environment variables at runtime *)
let () = Memory_tracing.reconfigure ()

(* Generate an immediate report *)
let report_promise = Memory_tracing.generate_report `Text
(* Returns: Lwt.t string containing the report *)

(* Get current leak status summary *)
match Memory_tracing.get_leak_status () with
| Some status -> print_endline status
| None -> print_endline "Tracing disabled"

(* Force garbage collection and memory analysis *)
let () = Memory_tracing.trigger_gc_analysis ()

(* Get runtime events statistics *)
match Memory_tracing.get_runtime_stats () with
| Some stats -> (* process stats *)
| None -> print_endline "Runtime events disabled"
```

### Advanced Functions

```ocaml
(* Control tracking deferral to avoid deadlocks during critical phases *)
let () = Memory_tracing.defer_allocation_tracking ()
(* All allocation tracking is paused until resumed *)

let () = Memory_tracing.resume_allocation_tracking ()
(* Resume normal allocation tracking *)

let is_deferred = Memory_tracing.is_allocation_tracking_deferred ()
(* Check if tracking is currently deferred *)

(* Manual allocation site capture utility *)
let site = Memory_tracing.get_allocation_site ()
(* Returns: allocation_site record with file, function, line, and backtrace *)

(* Domain memory information *)
let current_mb = Memory_tracing.get_domain_memory_usage (Domain.self ())
(* Returns: (heap_mb, live_mb) tuple for the domain *)

let alloc_rate = Memory_tracing.get_domain_allocation_rate (Domain.self ())
(* Returns: allocations per second since last GC *)

(* Manual instrumentation for custom structures *)
let record = Memory_tracing.track_custom_structure "MyCache" my_cache
(* Track custom data structures manually *)

let () = Memory_tracing.untrack_custom_structure record
(* Remove tracking when structure is cleaned up *)
```

### Internal Access (Advanced)

```ocaml
(* Advanced users can access internal components directly *)
open Dio_memory_tracing.Memory_tracing.Internal

(* Direct access to configuration *)
let enabled = Config.is_memory_tracing_enabled ()
let sample_rate = Config.config.sample_rate  (* Direct field access *)

(* Direct access to allocation tracker *)
let stats = Allocation_tracker.get_allocation_stats ()
let active_count = Allocation_tracker.get_all_active_allocations ()

(* Direct access to leak detector *)
let leak_report = Leak_detector.get_detailed_leak_report ()
let status = Leak_detector.get_leak_status_summary ()

(* Direct access to domain profiler *)
let domain_stats = Domain_profiler.get_all_domain_stats ()
let memory_summary = Domain_profiler.get_memory_summary ()

(* Direct access to reporter *)
let report_promise = Reporter.generate_report_now `Text

(* Direct access to runtime events tracer *)
let is_running = Runtime_events_tracer.is_running ()
let stats = Runtime_events_tracer.get_stats ()
```

### Manual Instrumentation

Use the `Tracked` modules to automatically instrument your data structures:

```ocaml
open Dio_memory_tracing.Memory_tracing.Tracked

(* Use tracked versions of standard data structures *)
let my_table = Hashtbl.create 100                    (* Automatically tracked *)
let my_array = Array.make 1000 0                     (* Automatically tracked *)
let my_list = List.of_seq (Seq.take 1500 (Seq.ints 0))  (* Large lists auto-tracked *)
let my_ring_buffer = RingBuffer.create 500           (* Automatically tracked *)

(* Track custom data structures manually *)
type my_complex_type = { data: int array; metadata: string }

let my_structure = { data = Array.make 1000 0; metadata = "example" }
let record = Memory_tracing.track_custom_structure "MyComplexType" my_structure
(* Later when structure is cleaned up: *)
Memory_tracing.untrack_custom_structure record
```

#### Thread-Safe Hashtables

For multi-threaded applications, use `SafeTrackedHashtbl` which bundles the hashtable with its mutex:

```ocaml
open Dio_memory_tracing.Memory_tracing.Tracked

(* Create a thread-safe tracked hashtable *)
let safe_table = SafeTrackedHashtbl.create 100

(* Use with automatic locking - ensures proper mutex acquisition/release *)
SafeTrackedHashtbl.with_lock safe_table (fun hashtbl ->
  SafeTrackedHashtbl.add hashtbl "key1" "value1";
  SafeTrackedHashtbl.replace hashtbl "key2" "value2";
  let value = SafeTrackedHashtbl.find_opt hashtbl "key1" in
  let length = SafeTrackedHashtbl.length hashtbl in
  (* All operations within this block are thread-safe *)
  (value, length)
)

(* Manual locking (advanced usage) - be careful with deadlock potential *)
SafeTrackedHashtbl.add safe_table "key3" "value3"  (* Thread-safe operation *)
```

#### Tracked Data Structure Operations

All tracked data structures support the same API as their standard library counterparts:

```ocaml
(* Hashtbl operations *)
TrackedHashtbl.add my_table "key" value
TrackedHashtbl.find my_table "key"
TrackedHashtbl.remove my_table "key"
TrackedHashtbl.length my_table

(* Array operations *)
TrackedArray.get my_array 0
TrackedArray.set my_array 0 42
TrackedArray.length my_array

(* Large list tracking (only tracks lists > 1000 elements) *)
let large_list = TrackedList.of_list [1; 2; 3; (* ... 1500 elements ... *)]
let length = TrackedList.length large_list
TrackedList.destroy large_list  (* Required for tracked lists *)

(* Ring buffer operations *)
TrackedRingBuffer.write my_ring_buffer item
let latest = TrackedRingBuffer.read_latest my_ring_buffer
let all_items = TrackedRingBuffer.read_all my_ring_buffer
TrackedRingBuffer.destroy my_ring_buffer  (* Clean up when done *)
```

### Domain Information

```ocaml
(* Get memory usage for current domain *)
let current_mb = Memory_tracing.get_domain_memory_usage (Domain.self ())

(* Get allocation rate for current domain *)
let alloc_rate = Memory_tracing.get_domain_allocation_rate (Domain.self ())
```

## Report Interpretation

### Report Generation and Storage

Memory reports are generated both on-demand and automatically at configured intervals. Reports are saved to the configured report directory with timestamped filenames:

- **Text Reports**: `{prefix}_{timestamp}.txt`
- **JSON Reports**: `{prefix}_{timestamp}.json`
- **Summary Reports**: Logged to console only

Example filenames:
```
memory_trace_1703123456.txt
memory_trace_1703123456.json
```

Reports are automatically generated every `DIO_MEMORY_REPORT_INTERVAL` seconds and saved to `DIO_MEMORY_REPORT_DIRECTORY` (default: "memory_trace").

### Text Reports

Generated reports include comprehensive memory analysis. By default, **all active allocations** are shown to help identify unbounded accumulations. Use `DIO_MEMORY_MAX_REPORT_ALLOCATIONS` to limit the display if reports become too large:

```
=======================================
      MEMORY LEAK DETECTION REPORT
=======================================

Report generated: 1703123456.789
Analysis duration: 2.345 seconds

MEMORY OVERVIEW
---------------
Total heap memory: 45.2 MB
Total allocations: 1254302
Active allocations: 89234
Leaks detected: 3

LEAK ANALYSIS
-------------
CRITICAL: Critical leak detected
  Structure: Hashtbl
  Growth rate: 1250.50 bytes/sec
  Current size: 45670
  Location: main.ml:234 in function process_orders
  Recommendation: Check for missing cleanup in order processing loop

HIGH: High leak risk detected
  Structure: CustomStructure
  Growth rate: 89.30 bytes/sec
  Current size: 1234
  Location: trading_engine.ml:567 in function update_positions
  Recommendation: Verify cleanup in position update logic
```

### JSON Reports

JSON reports are saved to file and can also be used for programmatic processing:

```json
{
  "timestamp": 1703123456.789,
  "duration_seconds": 2.345,
  "total_memory_mb": 45.2,
  "total_allocations": 1254302,
  "active_allocations": 89234,
  "leaks_found": [
    {
      "structure_type": "Hashtbl",
      "allocation_site": {
        "file": "main.ml",
        "function_name": "process_orders",
        "line": 234,
        "backtrace": "..."
      },
      "domain_id": 1,
      "severity": "Critical",
      "growth_rate": 1250.50,
      "current_size": 45670,
      "time_window": 300.0,
      "description": "Critical leak: 1250.50 bytes/sec growth rate",
      "recommendations": [
        "Check for missing cleanup in order processing loop",
        "Consider reducing data retention period"
      ]
    }
  ]
}
```

### Severity Levels

- **CRITICAL**: Immediate action required, rapid memory growth (>1000 bytes/sec)
- **HIGH**: High risk, significant growth (>100 bytes/sec) or large structures
- **MEDIUM**: Moderate concern, steady growth or medium-sized structures
- **LOW**: Minor issue, slow growth or small structures

## Manual Instrumentation

### Custom Structure Tracking

For complex data structures not automatically tracked:

```ocaml
(* Track custom structures *)
let my_cache = create_custom_cache ()
Memory_tracing.track_custom_structure "OrderCache" my_cache

(* Later, when structure is cleaned up *)
Memory_tracing.untrack_custom_structure my_cache
```

### Event Callbacks

Register callbacks for custom analysis:

```ocaml
let my_callback = function
  | Allocation_tracker.Allocation record ->
      (* Custom allocation handling *)
      log_allocation record
  | Allocation_tracker.Deallocation {id; structure_type; timestamp} ->
      (* Custom deallocation handling *)
      log_deallocation id structure_type timestamp

Allocation_tracker.add_callback my_callback
```

### Domain-Specific Monitoring

```ocaml
(* Monitor specific domains *)
let domain_id = Domain.get_id (Domain.self ())
let memory_usage = Domain_profiler.get_domain_memory_mb domain_id
let alloc_rate = Domain_profiler.get_domain_allocation_rate domain_id

(* Check for domain-specific issues *)
let domain_issues = Leak_detector.detect_domain_memory_issues ()
```

## Examples

### Trading Engine Monitoring

```ocaml
(* Enable comprehensive trading engine monitoring *)
let () =
  Unix.putenv "DIO_MEMORY_TRACING" "true";
  Unix.putenv "DIO_MEMORY_TRACK_HASHTABLES" "true";
  Unix.putenv "DIO_MEMORY_TRACK_ARRAYS" "true";
  Unix.putenv "DIO_MEMORY_DOMAIN_PROFILING" "true";
  Unix.putenv "DIO_MEMORY_LEAK_DETECTION" "true";
  Unix.putenv "DIO_MEMORY_REPORT_INTERVAL" "300";

  (* Start trading engine *)
  let engine = Trading_engine.create () in

  (* Monitor order book growth *)
  let order_book = Tracked.Hashtbl.create 1000 in

  (* Monitor position arrays *)
  let positions = Tracked.Array.make 500 {price=0.0; quantity=0} in

  (* Run engine with monitoring *)
  Trading_engine.run engine
```

### Production Configuration

```bash
#!/bin/bash
# Production memory tracing configuration

export DIO_MEMORY_TRACING=true
export DIO_MEMORY_SAMPLE_RATE=0.3          # 30% sampling for performance
export DIO_MEMORY_REPORT_INTERVAL=1800     # 30-minute reports
export DIO_MEMORY_SENSITIVITY=Medium       # Balanced detection
export DIO_MEMORY_TRACK_LISTS=false        # Skip list overhead
export DIO_MEMORY_RUNTIME_EVENTS=true      # Keep runtime monitoring
export DIO_MEMORY_DOMAIN_PROFILING=true    # Essential for multicore

# Start application
./dio_trading_engine
```

### Development Debugging

```bash
#!/bin/bash
# Development memory tracing for debugging

export DIO_MEMORY_TRACING=true
export DIO_MEMORY_SAMPLE_RATE=1.0          # Full sampling
export DIO_MEMORY_REPORT_INTERVAL=60       # 1-minute reports
export DIO_MEMORY_SENSITIVITY=High         # Aggressive detection
export DIO_MEMORY_TRACK_LISTS=true         # Track all lists
export DIO_MEMORY_MAX_HISTORY=50000        # Larger history for debugging

# Start with detailed logging
./dio_trading_engine 2>&1 | tee memory_debug.log
```

## Thread Safety

The memory tracing system is designed for multicore OCaml applications with comprehensive thread safety:

### Mutex Usage
- **Per-component mutexes**: Separate mutexes for allocation tracking, leak detection, domain profiling, and reporting
- **try_lock with timeouts**: Prevents deadlocks by using non-blocking lock attempts with 100ms timeouts
- **Graceful degradation**: Operations continue even when mutexes are contended
- **SafeTrackedHashtbl**: Provides thread-safe hashtable operations with automatic locking

### Safe Operations
- **Non-blocking allocation tracking**: Uses try_lock to avoid blocking during allocation recording
- **Background cleanup**: Automatic cleanup runs in separate threads without blocking normal operation
- **Atomic counters**: Global allocation counters use atomic operations for lock-free access

## Performance Considerations

### Overhead Management
- **Configurable sampling**: Reduce overhead with `DIO_MEMORY_SAMPLE_RATE` (default 100%)
- **Selective tracking**: Disable expensive features like list tracking in production
- **Automatic limits**: Built-in limits prevent unbounded memory growth in the tracing system itself
- **Background processing**: Report generation and cleanup run asynchronously

### Memory Usage
- **Self-limiting design**: Tracing system automatically manages its own memory usage
- **Cleanup mechanisms**: Automatic removal of stale data prevents memory leaks in the tracer
- **Configurable sensitivity**: Adjust leak detection sensitivity to balance accuracy vs overhead

### Optimization Tips
- **Production settings**: Use `DIO_MEMORY_SAMPLE_RATE=0.05` (5% sampling) for production monitoring
- **Disable expensive features**: Set `DIO_MEMORY_TRACK_LISTS=false` to reduce overhead
- **Increase reporting interval**: Use `DIO_MEMORY_REPORT_INTERVAL=3600` (hourly) for long-running applications
- **Use deferral**: Temporarily disable tracking during performance-critical operations

## Error Handling

### Graceful Degradation
- **Non-fatal failures**: Most operations continue even when individual components fail
- **Timeout protection**: Operations timeout rather than hanging indefinitely
- **Fallback behavior**: System continues with reduced functionality when components are unavailable
- **Safe shutdown**: Clean shutdown procedures ensure no resource leaks

### Common Scenarios
- **Mutex timeouts**: Operations return default values when mutexes cannot be acquired
- **File I/O failures**: Report generation continues even if file writing fails
- **Runtime events unavailable**: System continues without runtime event capture
- **Domain failures**: Domain-specific failures don't affect global tracing

### Debugging Failed Operations
- **Debug logging**: Enable detailed logging to diagnose issues
- **Manual testing**: Use `Memory_tracing.generate_report` to test report generation
- **Component isolation**: Test individual components through the `Internal` module

## Troubleshooting

### Common Issues

#### Tracing Not Starting

**Symptoms**: No memory tracing log messages appear.

**Solutions**:
1. Verify `DIO_MEMORY_TRACING=true` is set
2. Check environment variable syntax: `DIO_MEMORY_TRACING=true` (not `1` or `yes`)
3. Ensure module is compiled (check for `dio_memory_tracing` in dune dependencies)

#### High Performance Overhead

**Symptoms**: Application performance degrades significantly.

**Solutions**:
1. Reduce `DIO_MEMORY_SAMPLE_RATE` (try 0.1 for 10% sampling)
2. Disable expensive features: `DIO_MEMORY_TRACK_LISTS=false`
3. Increase `DIO_MEMORY_REPORT_INTERVAL` to reduce reporting frequency
4. Disable runtime events: `DIO_MEMORY_RUNTIME_EVENTS=false`

#### Missing Leak Detections

**Symptoms**: Expected leaks not being detected.

**Solutions**:
1. Increase `DIO_MEMORY_SENSITIVITY` to `High`
2. Reduce `DIO_MEMORY_REPORT_INTERVAL` for more frequent analysis
3. Ensure `DIO_MEMORY_LEAK_DETECTION=true`
4. Check that data structures are properly instrumented

#### Domain Profiling Issues

**Symptoms**: Domain-specific memory data missing.

**Solutions**:
1. Verify `DIO_MEMORY_DOMAIN_PROFILING=true`
2. Ensure OCaml 5.0+ with domain support
3. Check domain creation happens after tracing initialization

### Log Analysis

#### Interpreting Memory Growth

```
INFO [memory_monitoring] Feed balance: subs=0/0/0 cache=155 mem=0.155MB growth=0.000000MB/s
```

- `subs`: subscribers (current/total/max)
- `cache`: cache size in entries
- `mem`: memory usage in MB
- `growth`: growth rate in MB/second

#### Leak Severity Guidelines

- **growth=0.000000MB/s**: Normal, no leak detected
- **growth<0.001MB/s**: Low concern, monitor
- **growth<0.010MB/s**: Medium concern, investigate
- **growth≥0.010MB/s**: High concern, immediate action needed

### Debug Mode

For detailed debugging, enable all features:

```bash
export DIO_MEMORY_TRACING=true
export DIO_MEMORY_SAMPLE_RATE=1.0
export DIO_MEMORY_REPORT_INTERVAL=30
export DIO_MEMORY_TRACK_LISTS=true
export DIO_MEMORY_RUNTIME_EVENTS=true
export DIO_MEMORY_DOMAIN_PROFILING=true
export DIO_MEMORY_LEAK_DETECTION=true
export DIO_MEMORY_SENSITIVITY=High
export OCAMLRUNPARAM=v=0x3FF  # Maximum OCaml debug output
```

### Performance Tuning

#### Low Overhead Production Setup

```bash
export DIO_MEMORY_TRACING=true
export DIO_MEMORY_SAMPLE_RATE=0.05       # 5% sampling
export DIO_MEMORY_REPORT_INTERVAL=3600   # Hourly reports
export DIO_MEMORY_TRACK_LISTS=false
export DIO_MEMORY_RUNTIME_EVENTS=false   # Skip expensive events
export DIO_MEMORY_SENSITIVITY=Low
```

#### Balanced Monitoring Setup

```bash
export DIO_MEMORY_TRACING=true
export DIO_MEMORY_SAMPLE_RATE=0.2        # 20% sampling
export DIO_MEMORY_REPORT_INTERVAL=600    # 10-minute reports
export DIO_MEMORY_TRACK_LISTS=false
export DIO_MEMORY_RUNTIME_EVENTS=true
export DIO_MEMORY_SENSITIVITY=Medium
```

## Architecture

The memory tracing system consists of several interconnected components:

- **Memory_tracing**: Main API and initialization
- **Config**: Environment variable configuration
- **Allocation_tracker**: Data structure instrumentation
- **Leak_detector**: Pattern analysis and leak identification
- **Reporter**: Report generation and formatting
- **Domain_profiler**: Per-domain memory tracking
- **Runtime_events_tracer**: OCaml runtime event capture

All components are thread-safe and designed for multicore OCaml applications.
