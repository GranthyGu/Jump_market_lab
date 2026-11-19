# Persistence queue benchmarks

| Group | Coverage |
| --- | --- |
| Queue correctness | Ordering, completeness, bounded capacity, concurrent shutdown |
| Queue performance | Mutex queue versus two single-producer/single-consumer waiting strategies |
| Engine and recovery | Real matching and persistence, throughput, notification latency, final recovered state |

From the repository root:

```sh
./tests/build.sh
./build/demo-tests/bin/persistence_queue_benchmark --check
./build/demo-tests/bin/persistence_queue_recovery_spsc_park build/queue-check/park
./build/demo-tests/bin/persistence_queue_recovery_spsc_yield build/queue-check/yield

python3 -B tests/recovery/bench/run.py --build-dir build/demo-tests \
  --output build/queue-comparison
```

Alternative queues are used only by these benchmarks; production keeps its existing queue. Results go to the selected output directory. The [full test runner](../../README.md) includes this group.
