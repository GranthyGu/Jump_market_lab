# Planned restart tests

| Group | Coverage |
| --- | --- |
| Replication and recovery | Checkpoint freezing, contiguous replay, duplicate/gap rejection, partial-fill FIFO |
| Shard replacement | Active/standby round trips, request completeness, history continuity, stable market endpoints |
| Failures and load | Failed-switch fallback, ongoing trading, queuing, replacement timings |

From the repository root:

```sh
./tests/build.sh
ctest --test-dir build/demo-tests --output-on-failure \
  -R '^(replication_correctness|planned_restart_process)$'

# Measure replacement during ongoing trading.
python3 -B tests/restart/process.py build/demo-tests/bin build/restart-pressure \
  --resting 20000 --rate 20000 --copy-rate 0 --warmup 2 --switch-gap 2
```

Results and service logs go to the selected output directory. See the [test runner](../README.md) for the full suite.
