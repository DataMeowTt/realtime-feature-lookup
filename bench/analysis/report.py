import glob
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

STACK_ORDER = ["a", "b"]
STACK_NAME = {"a": "A (Kafka+Redis)", "b": "B (Fluss)"}


def md(df):
    return df.to_markdown(index=False, floatfmt=".2f") if not df.empty else "_(không có dữ liệu)_"


def latency_section(res, out):
    path = os.path.join(res, "summary.csv")
    if not os.path.exists(path):
        return []
    df = pd.read_csv(path)
    df["target_rate"] = pd.to_numeric(df["target_rate"], errors="coerce")
    agg = (df.groupby(["label", "stack", "target_rate"], dropna=False)
             .agg(runs=("run_id", "count"), throughput=("throughput", "mean"),
                  p50_ms=("p50_ms", "mean"), p95_ms=("p95_ms", "mean"), p99_ms=("p99_ms", "mean"),
                  max_ms=("max_ms", "mean"), stale_pct=("stale_rate", lambda s: 100 * s.mean()),
                  dup_pct=("dup_rate", lambda s: 100 * s.mean()), timeouts=("timeouts", "mean"))
             .reset_index().sort_values(["label", "target_rate", "stack"]))
    lines = ["## M1/M2/M4 — Latency, throughput, stale read (trung bình các lần lặp)", "", md(agg), ""]

    # p99 & throughput theo rate cho từng stack
    for metric, ylabel, fname in [("p99_ms", "p99 latency (ms)", "latency_p99.png"),
                                  ("throughput", "throughput đạt được (tx/s)", "throughput.png")]:
        fig, ax = plt.subplots(figsize=(7, 4))
        for st in STACK_ORDER:
            d = agg[(agg["stack"] == st) & agg["target_rate"].notna()].groupby("target_rate")[metric].mean()
            if not d.empty:
                ax.plot(d.index, d.values, marker="o", label=STACK_NAME.get(st, st))
        ax.set_xlabel("tx/s mục tiêu")
        ax.set_ylabel(ylabel)
        ax.set_xscale("log")
        ax.grid(alpha=.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(os.path.join(out, fname), dpi=120)
        plt.close(fig)
        lines += [f"![{metric}]({fname})", ""]
    return lines


def freshness_section(res, _):
    path = os.path.join(res, "freshness_summary.csv")
    if not os.path.exists(path):
        return []
    df = pd.read_csv(path)
    agg = (df.groupby("stack").agg(runs=("run_id", "count"), p50_ms=("p50_ms", "mean"),
                                   p95_ms=("p95_ms", "mean"), p99_ms=("p99_ms", "mean"),
                                   max_ms=("max_ms", "mean"), timeouts=("timeouts", "sum"))
             .reset_index())
    return ["## M3 — Freshness lag (commit Oracle → feature store thấy version mới)", "", md(agg), ""]


def es4_section(res, out):
    path = os.path.join(res, "es4.csv")
    if not os.path.exists(path):
        return []
    df = pd.read_csv(path)
    agg = (df.groupby(["stack", "delta_ms"]).agg(received=("received", "sum"), stale=("stale", "sum"))
             .reset_index())
    agg["stale_pct"] = 100 * agg["stale"] / agg["received"]
    pivot = agg.pivot(index="delta_ms", columns="stack", values="stale_pct").reset_index()
    fig, ax = plt.subplots(figsize=(7, 4))
    for st in STACK_ORDER:
        d = agg[agg["stack"] == st]
        if not d.empty:
            ax.plot(d["delta_ms"], d["stale_pct"], marker="o", label=STACK_NAME.get(st, st))
    ax.set_xlabel("Δ giữa commit và gửi tx (ms)")
    ax.set_ylabel("% bản ghi mang profile cũ")
    ax.set_xscale("symlog", linthresh=10)
    ax.grid(alpha=.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(out, "es4_stale.png"), dpi=120)
    plt.close(fig)
    return ["## M4 / E-S4 — Stale read theo Δ (%)", "", md(pivot), "", "![es4](es4_stale.png)", ""]


def hot_section(res, _):
    rows = []
    for f in glob.glob(os.path.join(res, "*_hot.csv")):
        run = os.path.basename(f)[:-len("_hot.csv")]
        ts_path = os.path.join(res, run + "_ts.csv")
        ev = pd.read_csv(f).set_index("event")["ts_ms"]
        if "on" not in ev or "off" not in ev or not os.path.exists(ts_path):
            continue
        label, stack, rate = run.rsplit("_", 4)[:3]
        ts = pd.read_csv(ts_path)
        ts = ts[ts["count"] > 1]  # bỏ giây mép cửa sổ đo
        t = (pd.to_datetime(ts["time"], utc=True) - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(milliseconds=1)
        ts = ts.assign(phase=["trước" if x < ev["on"] else "trong" if x < ev["off"] else "sau" for x in t])
        g = (ts.groupby("phase").agg(seconds=("second", "count"), tx_per_s=("count", "mean"),
                                     p50_ms=("p50_ms", "median"), p99_ms=("p99_ms", "median"),
                                     p99_worst_ms=("p99_ms", "max"), max_ms=("max_ms", "max"))
               .reset_index())
        g["label"], g["stack"], g["target_rate"] = label, stack, int(rate)
        rows.append(g)
    if not rows:
        return []
    df = pd.concat(rows)
    df["phase"] = pd.Categorical(df["phase"], ["trước", "trong", "sau"], ordered=True)
    agg = (df.groupby(["label", "target_rate", "stack", "phase"], observed=True)
             .agg(runs=("seconds", "count"), tx_per_s=("tx_per_s", "mean"), p50_ms=("p50_ms", "mean"),
                  p99_ms=("p99_ms", "mean"), p99_worst_ms=("p99_worst_ms", "mean"), max_ms=("max_ms", "mean"))
             .reset_index())
    return ["## X3 — Hot key: latency theo từng giây trước / trong / sau đợt hot key",
            "", "p50/p99 = trung vị theo giây trong mỗi pha; p99_worst/max = giây tệ nhất. Trung bình các lần lặp.",
            "", md(agg), ""]


def stats_section(res, _):
    files = glob.glob(os.path.join(res, "*_stats.csv"))
    if not files:
        return []
    rows = []
    for f in files:
        run = os.path.basename(f)[:-len("_stats.csv")]
        parts = run.split("_")
        stack = parts[1] if len(parts) > 1 else "?"
        d = pd.read_csv(f)
        if d.empty:
            continue
        d["cpu"] = d["cpu_pct"].str.rstrip("%").astype(float)
        d["mem_mib"] = d["mem_usage"].str.split("/").str[0].str.strip().apply(to_mib)
        d["service"] = (d["container"].str.replace(r"^rtfl-|-\d+$", "", regex=True)
                        .str.replace(r"^tools-run-.*", "tools", regex=True))
        # mỗi mẫu docker stats: cộng các container cùng service (generator + consumer = tools)
        per_ts = d.groupby(["ts", "service"]).agg(cpu=("cpu", "sum"), mem_mib=("mem_mib", "sum")).reset_index()
        g = per_ts.groupby("service").agg(cpu_pct=("cpu", "mean"), mem_mib=("mem_mib", "mean")).reset_index()
        g["stack"] = stack
        rows.append(g)
    if not rows:
        return []
    df = pd.concat(rows).groupby(["stack", "service"]).mean(numeric_only=True).reset_index()
    # tools = generator + consumer (công cụ đo), không tính vào hạ tầng của stack
    total = df[df["service"] != "tools"].groupby("stack").agg(cpu_pct=("cpu_pct", "sum"), mem_mib=("mem_mib", "sum"),
                                    containers=("service", "count")).reset_index()
    return ["## M6 — Tài nguyên trung bình theo container", "", md(df), "",
            "### Tổng theo stack (không tính tools = generator + consumer)", "", md(total), ""]


def to_mib(s):
    s = s.strip()
    for unit, mul in [("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024), ("GB", 1000), ("MB", 1), ("kB", 1 / 1000), ("B", 1 / 1048576)]:
        if s.endswith(unit):
            return float(s[: -len(unit)]) * mul
    return float("nan")


def main():
    res = sys.argv[1] if len(sys.argv) > 1 else "bench/results"
    lines = ["# Kết quả benchmark", ""]
    for section in (latency_section, freshness_section, es4_section, hot_section, stats_section):
        lines += section(res, res)
    path = os.path.join(res, "report.md")
    with open(path, "w") as f:
        f.write("\n".join(lines))
    print("\n".join(lines))
    print(f"\n-> {path}")


if __name__ == "__main__":
    main()
