#!/usr/bin/env python3
import json
import os
import re
from pathlib import Path


def write_summary(root):
    root = Path(root)
    discovered = [path.name for path in sorted(root.iterdir()) if path.is_dir()]
    names = (
        ["psmdb", "community"]
        if "psmdb" in discovered and "community" in discovered
        else discovered
    )


    def ycsb(path):
        data = {}
        if not path.exists():
            return data
        for line in path.read_text(errors="replace").splitlines():
            parts = [part.strip() for part in line.split(",")]
            if len(parts) >= 3 and parts[0].startswith("[") and parts[0].endswith("]"):
                data[f"{parts[0][1:-1]} {parts[1]}"] = parts[2]
        return data


    def perf(path):
        data = {}
        if not path.exists():
            return data
        for line in path.read_text(errors="replace").splitlines():
            match = re.match(r"\s*([\d,]+)\s+(\S+)", line)
            if match and not match.group(2).startswith("seconds"):
                raw = match.group(2).split(":")[0].strip("/")
                event = raw.split("/")[-1]
                data[event] = data.get(event, 0) + int(match.group(1).replace(",", ""))
            elapsed = re.search(r"([\d.]+)\s+seconds time elapsed", line)
            if elapsed:
                data["seconds"] = float(elapsed.group(1))
        return data


    def bolt(path):
        text = path.read_text(errors="replace") if path.exists() else ""
        return "yes" if "BOLT revision" in text else "no"


    def build_environment(path):
        if not path.exists():
            return "n/a"
        text = path.read_text(errors="replace").strip()
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return text or "n/a"
        if isinstance(data, dict) and "buildEnvironment" in data:
            data = data["buildEnvironment"]
        return json.dumps(data, indent=2)


    def mongod_version(path):
        if not path.exists():
            return "n/a"
        text = path.read_text(errors="replace")
        match = re.search(r'"version":\s*"([^"]+)"', text)
        if match:
            return match.group(1)
        for line in text.splitlines():
            if line.startswith("db version"):
                return line.removeprefix("db version").strip().lstrip("v")
        return "n/a"


    def num(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None


    def _ratio(data, numerator, denominator, scale):
        top = num(data.get(numerator))
        bottom = num(data.get(denominator))
        if top is None or bottom is None or bottom == 0:
            return None
        return top / bottom * scale


    def cell(value):
        got = num(value)
        if got is None:
            return "n/a"
        if abs(got) < 1:
            return f"{got:.4f}"
        if abs(got) >= 100:
            return f"{got:.0f}"
        return f"{got:.2f}"


    def median(values):
        nums = sorted(value for value in (num(item) for item in values) if value is not None)
        if not nums:
            return None
        mid = len(nums) // 2
        if len(nums) % 2:
            return nums[mid]
        return (nums[mid - 1] + nums[mid]) / 2


    def delta(left, right):
        base, other = num(left), num(right)
        if base is None or other is None or base == 0:
            return "n/a"
        return f"{((other - base) / base) * 100:.1f}%"


    def render_table(rows):
        number = re.compile(
            r"(?:n/a|-?\d[\d,]*(?:\.\d+)?%?)"
            r"(?:,(?:n/a|-?\d[\d,]*(?:\.\d+)?%?))*$"
        )
        widths = [max(len(cell) for cell in column) for column in zip(*rows)]
        aligns = []
        for column in zip(*rows):
            body = column[1:]
            numeric = bool(body) and all(number.fullmatch(cell) for cell in body)
            # Sample lists stay at the left edge of their column. Single values
            # stay right-aligned so the digits line up.
            aligns.append(">" if numeric and not any("," in cell for cell in body) else "<")
        rendered = []
        for index, row in enumerate(rows):
            rendered.append(
                "  ".join(f"{cell:{aligns[i]}{widths[i]}}" for i, cell in enumerate(row)).rstrip()
            )
            if index == 0:
                rendered.append(
                    "  ".join(f"{'-' * widths[i]:{aligns[i]}{widths[i]}}" for i in range(len(row))).rstrip()
                )
        return rendered


    def run_dirs(name):
        base = root / name
        if (base / "ycsb-load.txt").exists():
            return [base]
        return sorted(
            path
            for path in base.iterdir()
            if path.is_dir()
            and re.fullmatch(r"\d{2}", path.name)
            and (path / "ycsb-load.txt").exists()
        )


    metrics = [
        ("load", "OVERALL Throughput(ops/sec)", "load throughput (ops/sec)"),
        ("load", "INSERT AverageLatency(us)", "load INSERT avg (us)"),
        ("load", "INSERT 95thPercentileLatency(us)", "load INSERT p95 (us)"),
        ("load", "INSERT 99thPercentileLatency(us)", "load INSERT p99 (us)"),
        ("run", "OVERALL Throughput(ops/sec)", "run throughput (ops/sec)"),
        ("run", "READ AverageLatency(us)", "run READ avg (us)"),
        ("run", "READ 95thPercentileLatency(us)", "run READ p95 (us)"),
        ("run", "READ 99thPercentileLatency(us)", "run READ p99 (us)"),
        ("run", "UPDATE AverageLatency(us)", "run UPDATE avg (us)"),
        ("run", "UPDATE 95thPercentileLatency(us)", "run UPDATE p95 (us)"),
        ("run", "UPDATE 99thPercentileLatency(us)", "run UPDATE p99 (us)"),
    ]
    perf_metrics = [
        "instructions",
        "cycles",
        "L1-icache-load-misses",
        "branches",
        "branch-misses",
        "iTLB-load-misses",
        "topdown-fe-bound",
        "slots",
    ]
    # Rates are taken per sample, then the median of those rates. A rate of the
    # two medians would mix different runs.
    derived_metrics = (
        ("ipc", "instructions", "cycles", 1),
        ("L1i misses / 1k instr", "L1-icache-load-misses", "instructions", 1000),
        ("iTLB misses / 1k instr", "iTLB-load-misses", "instructions", 1000),
        ("branch-miss rate %", "branch-misses", "branches", 100),
        ("frontend bound %", "topdown-fe-bound", "slots", 100),
    )

    parsed = {}
    samples = []
    for name in names:
        runs = []
        for path in run_dirs(name):
            run_id = path.name if path != root / name else "01"
            sample = {
                "run_id": run_id,
                "target": name,
                "version": mongod_version(root / name / "mongod-version.txt"),
                "load": ycsb(path / "ycsb-load.txt"),
                "run": ycsb(path / "ycsb-run.txt"),
                "perf-load": perf(path / "perf-load.txt"),
                "perf-run": perf(path / "perf-run.txt"),
            }
            runs.append(sample)
            samples.append(sample)
        parsed[name] = {
            "runs": runs,
            "bolt": bolt(root / name / "bolt-note.txt"),
            "version": runs[0]["version"] if runs else mongod_version(root / name / "mongod-version.txt"),
            "buildEnvironment": build_environment(root / name / "buildinfo.json"),
        }

    samples.sort(key=lambda sample: sample["run_id"])
    compare = "psmdb" in names and "community" in names
    lines = [
        "aggregate=median",
        "delta=(community-psmdb)/psmdb",
    ]
    for name in names:
        lines.append(f"samples {name}={len(parsed[name]['runs'])}")
        lines.append(f"{name} mongod: {parsed[name]['version']}")

    lines.append("")
    per_run = [[
        "run",
        "target",
        "version",
        "load throughput (ops/sec)",
        "run throughput (ops/sec)",
        "load INSERT avg (us)",
        "run READ avg (us)",
        "run READ p99 (us)",
        "run UPDATE avg (us)",
        "run UPDATE p99 (us)",
    ]]
    for sample in samples:
        per_run.append([
            sample["run_id"],
            sample["target"],
            sample["version"],
            cell(sample["load"].get("OVERALL Throughput(ops/sec)")),
            cell(sample["run"].get("OVERALL Throughput(ops/sec)")),
            cell(sample["load"].get("INSERT AverageLatency(us)")),
            cell(sample["run"].get("READ AverageLatency(us)")),
            cell(sample["run"].get("READ 99thPercentileLatency(us)")),
            cell(sample["run"].get("UPDATE AverageLatency(us)")),
            cell(sample["run"].get("UPDATE 99thPercentileLatency(us)")),
        ])
    lines.extend(render_table(per_run))


    def values_for(name, phase, key):
        return [sample[phase].get(key) for sample in parsed[name]["runs"]]


    def emit(bucket, label, key_of):
        medians = {name: median(key_of(name)) for name in names}
        row = [label, *[cell(medians[name]) for name in names]]
        if compare:
            row.append(delta(medians["psmdb"], medians["community"]))
            row.append(",".join(cell(value) for value in key_of("psmdb")))
            row.append(",".join(cell(value) for value in key_of("community")))
        else:
            row.extend(["n/a", "", ""])
        bucket.append(row)


    def titled(name, kind):
        version = parsed[name]["version"]
        return f"{name} {version} {kind}" if version != "n/a" else f"{name} {kind}"


    header = ["metric", *[titled(name, "median") for name in names], "delta %"]
    if compare:
        header.extend([titled("psmdb", "values"), titled("community", "values")])
    ycsb_rows = [header]
    perf_rows = [list(header)]

    for phase, key, label in metrics:
        emit(ycsb_rows, label, lambda name, phase=phase, key=key: values_for(name, phase, key))

    for phase in ("load", "run"):
        for key in perf_metrics:
            emit(
                perf_rows,
                f"perf {phase} {key}",
                lambda name, phase=phase, key=key: [
                    sample[f"perf-{phase}"].get(key) for sample in parsed[name]["runs"]
                ],
            )
        emit(
            perf_rows,
            f"perf {phase} seconds",
            lambda name, phase=phase: [
                sample[f"perf-{phase}"].get("seconds") for sample in parsed[name]["runs"]
            ],
        )
        for label, numerator, denominator, scale in derived_metrics:
            emit(
                perf_rows,
                f"perf {phase} {label}",
                lambda name, phase=phase, numerator=numerator, denominator=denominator, scale=scale: [
                    _ratio(sample[f"perf-{phase}"], numerator, denominator, scale)
                    for sample in parsed[name]["runs"]
                ],
            )

    lines.append("")
    lines.extend(render_table(ycsb_rows))
    lines.append("")
    lines.extend(render_table(perf_rows))
    lines.append("")
    for name in names:
        lines.append(f"{name} BOLT note: {parsed[name]['bolt']}")
        lines.append(f"{name} buildEnvironment:")
        lines.append(parsed[name]["buildEnvironment"])

    text = "\n".join(lines) + "\n"
    (root / "summary.txt").write_text(text)
    return text


def main():
    print(write_summary(os.environ.get("YCSB_RESULTS_DIR", "/results")), end="")


if __name__ == "__main__":
    main()
