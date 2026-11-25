# PSMDB-2346: PSMDB and Community BOLT comparison

Compares Percona Server for MongoDB with MongoDB Community using YCSB workload A and host `perf`. The test checks each `mongod` for a BOLT note with `readelf`, then runs both servers and writes a median summary.

Workload A is 50/50 read/update against database `ali`. Each slot starts a fresh `mongod` with an empty entrypoint and `--dbpath /data/db`. Data stays in the container filesystem and is removed with the container.

## Setup

Docker and Docker Compose v2 are required. The test container uses the host Docker socket to start `mongod` and YCSB.

`perf` must be the host binary that matches the running kernel. The test container does not install `perf`; it enters the host mount namespace and runs `/usr/bin/perf`. On Ubuntu:

```bash
sudo apt install linux-tools-$(uname -r) linux-tools-generic
perf version
```

Counters for other users are allowed when `kernel.perf_event_paranoid` is `1` or lower. The test sets this itself. If that `sysctl` fails, set it on the host:

```bash
sudo sysctl -w kernel.perf_event_paranoid=1
```

To keep it across reboot, put `kernel.perf_event_paranoid=1` in `/etc/sysctl.d/`.

Check that the events used by the test are counted on this machine:

```bash
perf stat -e instructions,cycles,L1-icache-load-misses,branches,branch-misses,iTLB-load-misses,topdown-fe-bound -- sleep 0.2
```

On a hybrid CPU, `perf` prints each event twice, once as `cpu_atom/...` and once as `cpu_core/...`. The summary adds those two counts. `<not counted>` on the core type the process did not run on is normal. `<not supported>` means that event is missing from the results. `iTLB-loads` and `stalled-cycles-frontend` are not supported on the machine this was written for, so they are not collected. `topdown-fe-bound` is the frontend-stall counter that is supported. On the performance cores, `perf` also prints a `slots` count beside it.

## Run

From this directory:

```bash
docker compose build
docker compose run test pytest -s
```

`docker compose build` builds the pytest image and the [mongodb-labs YCSB](https://github.com/mongodb-labs/YCSB) image. `docker compose run` starts only the test container. Pytest then starts one `mongod` at a time and one YCSB container per phase. YCSB is named `ycsb-<phase>-<server>` (for example `ycsb-load-bolt-psmdb-01`) and is removed when the phase exits, so `docker compose ps` does not list it.

Pytest then runs one unmeasured warmup, the same load and run as a measured slot, and discards it. The eight measured slots follow immediately, so none of them is the first work on an idle CPU. The servers alternate so a steady drift does not land on one target.

| Slot | Server |
| --- | --- |
| 01 | psmdb |
| 02 | community |
| 03 | community |
| 04 | psmdb |
| 05 | community |
| 06 | psmdb |
| 07 | psmdb |
| 08 | community |

Each slot loads, then runs. `mongod` containers are named `bolt-<server>-<slot>`, for example `bolt-psmdb-01`. Results are written to `./results/` (gitignored):

```text
results/<target>/mongod-version.txt
results/<target>/bolt-note.txt
results/<target>/buildinfo.json
results/<target>/<slot>/ycsb-load.txt
results/<target>/<slot>/ycsb-run.txt
results/<target>/<slot>/perf-load.txt
results/<target>/<slot>/perf-run.txt
results/<target>/<slot>/mongod.log
results/summary.txt
```

Reprint the summary from results already on disk:

```bash
docker compose run test pytest -s test_bolt.py::test_summary
```

### Environment

Set these in the shell before `docker compose run`. Compose substitutes them into the test container.

| Variable | Default | Effect |
| --- | --- | --- |
| `PSMDB_IMAGE` | `perconalab/percona-server-mongodb:9.0.4` | PSMDB image |
| `COMMUNITY_IMAGE` | `mongodb/mongodb-community-server:9.0.4-ubi9` | Community image. Use the exact `9.0.4` tag; `9.0-ubi9` is an older release |
| `YCSB_RECORDCOUNT` | `800000` | Records inserted by `ycsb load`. Kept at this size so the set stays inside the cache |
| `YCSB_OPERATIONCOUNT` | `6000000` | Operations executed by `ycsb run`. About two to three minutes at the rates measured with 600,000 operations |
| `YCSB_THREADS` | `12` | YCSB client threads |
| `YCSB_CACHE_GB` | `2` | `--wiredTigerCacheSizeGB`. The default workload is about 800 MB plus the index, so 2 GB keeps it in memory without allocating the server default of half of RAM |

`mongod` and YCSB are not pinned. The kernel schedules them on any core.

The record count is unchanged so the working set still fits in the 2 GB cache. The operation count is what lengthens the steady run. More threads raise throughput and shorten the same operation count.

These are fixed by Compose and are not read from the host shell: `YCSB_IMAGE=psmdb-2346-ycsb:mongodb-labs`, `YCSB_RESULTS_DIR=/results`, `YCSB_NETWORK=psmdb-2346`.

## Interpret the results

Pytest prints `results/summary.txt` at the end. The version column comes from `mongod --version`. After the BOLT line, the summary prints the `buildEnvironment` object from `buildInfo`, recorded once per image during inspect. The per-run table is one row per slot. The next two tables are medians of the four PSMDB samples and the four Community samples, one for YCSB and one for `perf`. With an even count, the median is the average of the two middle values, so a single fast or slow slot does not decide the result.

`delta %` is `(community − psmdb) / psmdb`.

- Throughput: a positive delta means Community completed more operations per second.
- Latency (`avg`, `p95`, `p99`, microseconds): a negative delta means Community was faster.
- `perf … seconds`: wall-clock time of that phase. A negative delta means Community finished sooner. This should move in the opposite direction from throughput.
- Instruction, cycle, branch, and miss counters are totals for the phase, not per operation. On a hybrid CPU the `cpu_atom` and `cpu_core` counts are added together. `<not counted>` on the core type the process did not run on is normal.
- `perf … ipc` is instructions / cycles, computed on each sample. Higher means the core retired more of the instruction stream per cycle.
- `L1i misses / 1k instr` and `iTLB misses / 1k instr` are the miss counts divided by retired instructions. These are the layout counters. A negative delta means Community fetched the hot code with fewer misses.
- `branch-miss rate %` is branch misses / branches.
- `frontend bound %` is `topdown-fe-bound / slots`. It is the share of issue slots stalled on instruction fetch. A negative delta means less of the core's time was stuck waiting for the instruction stream. `perf` prints `slots` beside `topdown-fe-bound` for the performance-core PMU.

`psmdb BOLT note` and `community BOLT note` come from `readelf -p .note.bolt_info` on `/usr/bin/mongod`. `yes` means the note contains `BOLT revision`.

BOLT reorders compiled code. The expected counters are fewer L1 instruction-cache misses, fewer iTLB misses, and fewer branch misses. The instruction total can rise because the hot path is laid out with more straight-line code. Throughput usually moves by less than the miss counts. Each operation also looks up data, takes locks, and writes the journal, and BOLT does not shorten that work. A miss drop of about 30% with an ops/s gain of about 10% means instruction fetch was only part of the operation.

The size of the gain depends on whether the data fits in the WiredTiger cache. Workload A stores about 1 KB per record, so 800,000 records are about 800 MB plus the index. When that fits in the cache, reads stay in memory (hundreds of microseconds, not disk latency) and the BOLT difference can show up. When the data is larger than the cache, most of each operation is a disk read and the same miss reduction barely changes ops/s.

These two binaries are not a BOLT on/off pair of one build. PSMDB contains additional code, and Community’s BOLT options are whatever was used to build that image. A result here is not the 52% figure from the Arm Neoverse N2 article; that run used a different CPU and a different BOLT profile.
