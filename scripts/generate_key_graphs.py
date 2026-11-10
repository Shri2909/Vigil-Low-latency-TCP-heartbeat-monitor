#!/usr/bin/env python3
"""Renders the two benchmark charts (epoll vs thread-per-connection CPU, and
heartbeat latency percentiles) from the saved output in bench/results/.
Nothing is hardcoded: run parameters in captions come from the results files'
headers, and durations are unit-parsed.

Usage: python3 scripts/generate_key_graphs.py [--results-dir DIR] [--out-dir DIR]
  reads  latency_bench.txt, throughput_bench_epoll.txt, throughput_bench_naive.txt
  writes throughput_scalability.png, latency_percentiles.png
(defaults: bench/results -> bench/charts; scripts/run_benchmarks_and_plot.sh runs both steps)
"""

from __future__ import annotations

import argparse
import pathlib
import re

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

matplotlib.use("Agg")

# --- Palette: series 1 = blue, series 2 = green, in the same order on both charts. ---
SURFACE = "#fcfcfb"
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
BASELINE = "#c3c2b7"

SERIES_1 = "#2a78d6"  # epoll reactor / tcp_nodelay=true
SERIES_2 = "#008300"  # naive thread-per-connection / tcp_nodelay=false

# DejaVu Sans ships with matplotlib, so charts render the same on any machine.
SANS = ["DejaVu Sans"]

ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT_DIR / "bench" / "results"
OUT_DIR = ROOT_DIR / "bench" / "charts"

_UNIT_TO_US = {"ns": 1e-3, "us": 1.0, "ms": 1e3, "s": 1e6}


def _base_style() -> None:
    plt.rcParams.update(
        {
            "font.family": SANS,
            "text.color": INK_PRIMARY,
            "axes.edgecolor": BASELINE,
            "axes.labelcolor": INK_SECONDARY,
            "xtick.color": INK_MUTED,
            "ytick.color": INK_MUTED,
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "savefig.facecolor": SURFACE,
        }
    )


def _strip_chrome(ax: plt.Axes) -> None:
    """Recessive grid, no top/right/left spines, thin baseline."""
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(BASELINE)
    ax.spines["bottom"].set_linewidth(1)
    ax.tick_params(axis="both", length=0, labelsize=10.5)
    ax.yaxis.grid(True, color=GRIDLINE, linewidth=1, zorder=0)
    ax.set_axisbelow(True)


# --- Parsing saved benchmark output --------------------------------------


def _require_text(path: pathlib.Path) -> str:
    if not path.exists():
        raise SystemExit(
            f"missing {path}\n"
            f"Run the benchmarks and save their output there first -- see "
            f"scripts/run_benchmarks_and_plot.sh for the exact commands, or run it directly:\n"
            f"    bash scripts/run_benchmarks_and_plot.sh"
        )
    return path.read_text()


def parse_duration_us(token: str) -> float:
    """'6.12us' / '850.00ns' / '1.09ms' / '2.30s' -> microseconds."""
    m = re.fullmatch(r"([\d.]+)(ns|us|ms|s)", token.strip())
    if not m:
        raise SystemExit(f"cannot parse duration {token!r} (expected <number>ns|us|ms|s)")
    return float(m.group(1)) * _UNIT_TO_US[m.group(2)]


LATENCY_METRICS = ["p50", "p90", "p99", "p99.9", "max"]


def parse_latency_results(text: str) -> dict:
    """{"params": {"warmup", "measured", "repetitions"},
        "true"/"false": {metric: {"min": us, "median": us, "max": us}}}
    parsed from latency_bench's stdout (durations converted to microseconds)."""
    hdr = re.search(r"warmup=(\d+)\s+measured=(\d+)\s+repetitions=(\d+)", text)
    if not hdr:
        raise SystemExit("latency_bench.txt has no 'warmup=.. measured=.. repetitions=..' header line")
    params = {"warmup": int(hdr.group(1)), "measured": int(hdr.group(2)), "repetitions": int(hdr.group(3))}

    result: dict = {"params": params}
    current: str | None = None
    for line in text.splitlines():
        header = re.match(r"tcp_nodelay=(true|false)\s+across \d+ repetitions", line.strip())
        if header:
            current = header.group(1)
            result[current] = {}
            continue
        if current is not None:
            m = re.match(
                r"\s*(p50|p90|p99\.9|p99|max)\s*:\s+min=(\S+)\s+median=(\S+)\s+max=(\S+)",
                line,
            )
            if m:
                result[current][m.group(1)] = {
                    "min": parse_duration_us(m.group(2)),
                    "median": parse_duration_us(m.group(3)),
                    "max": parse_duration_us(m.group(4)),
                }
    for cfg in ("true", "false"):
        got = result.get(cfg, {})
        if not {"p50", "p90", "p99"} <= set(got):
            raise SystemExit(
                f"could not parse p50/p90/p99 for tcp_nodelay={cfg} out of latency_bench.txt -- "
                "was the file truncated, or does the output format no longer match?"
            )
    return result


def parse_throughput_header(text: str) -> dict:
    """{"interval_ms": int, "repetitions": int, "window_ms": int | None} from
    a throughput_bench results header (window is absent in pre-window files)."""
    hdr = re.search(r"heartbeat_interval=(\d+)ms.*?repetitions=(\d+)", text)
    if not hdr:
        raise SystemExit("throughput results file has no 'heartbeat_interval=..ms .. repetitions=..' header line")
    win = re.search(r"steady_window=(\d+)ms", text)
    return {
        "interval_ms": int(hdr.group(1)),
        "repetitions": int(hdr.group(2)),
        "window_ms": int(win.group(1)) if win else None,
    }


def parse_throughput_epoll_ramp(text: str) -> dict[int, float]:
    """{connection_count: steady_cpu_median_pct} from the connection-count ramp
    only; the epoll_max_events sweep below it reuses the largest N and is cut off."""
    ramp_section = text.split("=== epoll_max_events batch size", 1)[0]
    results: dict[int, float] = {}
    for block in re.finditer(
        r"^N=(\d+)\s+epoll_max_events=256.*?steady_cpu:\s+min=[\d.]+%\s+median=([\d.]+)%",
        ramp_section,
        re.MULTILINE | re.DOTALL,
    ):
        n, cpu = int(block.group(1)), float(block.group(2))
        results.setdefault(n, cpu)
    if not results:
        raise SystemExit("could not find any 'N=... epoll_max_events=256' ramp block in throughput_bench_epoll.txt")
    return results


def parse_throughput_naive(text: str) -> dict[int, float]:
    """{connection_count: steady_cpu_median_pct}, from one or more concatenated
    `--naive-only <N>` runs in the same file."""
    results: dict[int, float] = {}
    for block in re.finditer(
        r"^N=(\d+)\s+\(1 thread/connection\).*?steady_cpu:\s+min=[\d.]+%\s+median=([\d.]+)%",
        text,
        re.MULTILINE | re.DOTALL,
    ):
        n, cpu = int(block.group(1)), float(block.group(2))
        results[n] = cpu
    if not results:
        raise SystemExit("could not find any 'N=... (1 thread/connection)' block in throughput_bench_naive.txt")
    return results


# --- Graph 1: throughput scalability ------------------------------------


def plot_throughput_scalability() -> pathlib.Path:  # noqa: C901
    epoll_text = _require_text(RESULTS_DIR / "throughput_bench_epoll.txt")
    naive_text = _require_text(RESULTS_DIR / "throughput_bench_naive.txt")
    epoll = parse_throughput_epoll_ramp(epoll_text)
    naive = parse_throughput_naive(naive_text)
    hdr = parse_throughput_header(epoll_text)
    naive_hdr = parse_throughput_header(naive_text)
    if (hdr["interval_ms"], hdr["window_ms"]) != (naive_hdr["interval_ms"], naive_hdr["window_ms"]):
        raise SystemExit(
            "epoll and naive results were produced with different heartbeat interval/window "
            f"({hdr} vs {naive_hdr}) -- the comparison would be apples to oranges; re-run both."
        )

    connection_counts = sorted(set(epoll) & set(naive))
    if not connection_counts:
        raise SystemExit("epoll and naive result files share no common connection counts -- re-run both at the same N values")
    epoll_cpu_pct = [epoll[n] for n in connection_counts]
    naive_cpu_pct = [naive[n] for n in connection_counts]

    fig, ax = plt.subplots(figsize=(10, 6.2), dpi=200)
    _strip_chrome(ax)

    x = np.arange(len(connection_counts))
    ax.set_xlim(x[0] - 0.35, x[-1] + 0.85)  # room for the end-of-line labels

    ax.plot(
        x,
        epoll_cpu_pct,
        color=SERIES_1,
        linewidth=2.4,
        marker="o",
        markersize=8,
        markerfacecolor=SURFACE,
        markeredgecolor=SERIES_1,
        markeredgewidth=2.2,
        solid_capstyle="round",
        solid_joinstyle="round",
        zorder=3,
        label="epoll reactor (this project)",
    )
    ax.plot(
        x,
        naive_cpu_pct,
        color=SERIES_2,
        linewidth=2.4,
        marker="o",
        markersize=8,
        markerfacecolor=SURFACE,
        markeredgecolor=SERIES_2,
        markeredgewidth=2.2,
        solid_capstyle="round",
        solid_joinstyle="round",
        zorder=3,
        label="naive thread-per-connection",
    )

    # Label only the last point of each line.
    ax.annotate(
        f"{epoll_cpu_pct[-1]:.1f}%",
        (x[-1], epoll_cpu_pct[-1]),
        xytext=(10, 8 if epoll_cpu_pct[-1] >= naive_cpu_pct[-1] else -14),
        textcoords="offset points",
        color=SERIES_1,
        fontsize=11,
        fontweight="bold",
    )
    ax.annotate(
        f"{naive_cpu_pct[-1]:.1f}%",
        (x[-1], naive_cpu_pct[-1]),
        xytext=(10, -14 if epoll_cpu_pct[-1] >= naive_cpu_pct[-1] else 8),
        textcoords="offset points",
        color=SERIES_2,
        fontsize=11,
        fontweight="bold",
    )

    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels([str(n) for n in connection_counts])
    ax.set_xlabel("concurrent monitored connections", fontsize=11)
    ax.set_ylabel("steady-state CPU (% of one core, log scale)", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:g}%"))

    fig.text(
        0.08,
        0.955,
        "Epoll reactor vs thread-per-connection: steady-state CPU",
        fontsize=17,
        fontweight="bold",
        color=INK_PRIMARY,
    )
    fig.text(
        0.08,
        0.905,
        f"Steady-state CPU at the same connection count and heartbeat cadence ({hdr['interval_ms']}ms)",
        fontsize=11.5,
        color=INK_SECONDARY,
    )

    legend = ax.legend(
        loc="upper left",
        frameon=False,
        fontsize=10.5,
        handlelength=1.6,
        borderaxespad=0,
    )
    for text in legend.get_texts():
        text.set_color(INK_SECONDARY)

    fig.text(
        0.08,
        0.02,
        "Source: bench/results/throughput_bench_epoll.txt, throughput_bench_naive.txt "
        f"(median of {hdr['repetitions']} reps per point"
        + (f", {hdr['window_ms'] / 1000:g}s steady-state window" if hdr["window_ms"] else "")
        + ").\nSingle machine, loopback, echo peer in the same process: read as relative, not absolute.",
        fontsize=8.5,
        color=INK_MUTED,
        linespacing=1.6,
    )

    fig.subplots_adjust(left=0.09, right=0.93, top=0.80, bottom=0.16)
    out_path = OUT_DIR / "throughput_scalability.png"
    fig.savefig(out_path)
    plt.close(fig)
    return out_path


# --- Graph 2: heartbeat round-trip latency -------------------------------


def plot_latency_percentiles() -> pathlib.Path:
    latency = parse_latency_results(_require_text(RESULTS_DIR / "latency_bench.txt"))
    params = latency["params"]
    # Plot every percentile the file contains, in order.
    metrics = [m for m in LATENCY_METRICS if m in latency["true"] and m in latency["false"]]
    med_true = [latency["true"][m]["median"] for m in metrics]
    med_false = [latency["false"][m]["median"] for m in metrics]
    worst_true = [latency["true"][m]["max"] for m in metrics]
    worst_false = [latency["false"][m]["max"] for m in metrics]

    fig, ax = plt.subplots(figsize=(9.6, 6.4), dpi=200)
    _strip_chrome(ax)

    x = np.arange(len(metrics))
    width = 0.34

    bars_true = ax.bar(x - width / 2, med_true, width, color=SERIES_1, label="tcp_nodelay = true (median of reps)", zorder=3)
    bars_false = ax.bar(x + width / 2, med_false, width, color=SERIES_2, label="tcp_nodelay = false (median of reps)", zorder=3)

    # Worst repetition for each metric: the number a median-of-reps bar hides.
    ax.scatter(x - width / 2, worst_true, marker="v", s=46, color=INK_PRIMARY, zorder=4, label="worst repetition")
    ax.scatter(x + width / 2, worst_false, marker="v", s=46, color=INK_PRIMARY, zorder=4)

    def fmt(us: float) -> str:
        return f"{us / 1000:.2f}ms" if us >= 1000 else f"{us:.2f}\u00b5s"

    # Median inside the bar, worst repetition above its marker; rotated to avoid collisions.
    floor = min(med_true + med_false) / 12
    for bars, worst in ((bars_true, worst_true), (bars_false, worst_false)):
        for rect, w in zip(bars, worst):
            cx = rect.get_x() + rect.get_width() / 2
            ax.text(cx, floor * 1.15, fmt(rect.get_height()), rotation=90, ha="center", va="bottom",
                    fontsize=8.5, color="white", fontweight="bold", zorder=5)
            top = max(rect.get_height(), w)
            ax.annotate(fmt(w), (cx, top), xytext=(0, 9), textcoords="offset points", rotation=90,
                        ha="center", va="bottom", fontsize=8.5, color=INK_SECONDARY)

    ax.set_yscale("log")
    ax.set_ylim(floor, max(worst_true + worst_false) * 8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: fmt(v)))
    ax.set_xticks(x)
    ax.set_xticklabels(metrics)
    ax.set_ylabel("PING \u2192 PONG round-trip latency (log scale)", fontsize=11)

    fig.text(0.1, 0.955, "Heartbeat round trip: typical case and worst case", fontsize=16, fontweight="bold", color=INK_PRIMARY)
    fig.text(
        0.1,
        0.905,
        f"{params['measured']:,} measured round trips \u00d7 {params['repetitions']} repetitions per config "
        f"({params['warmup']:,} warmup), real sockets, loopback",
        fontsize=11.5,
        color=INK_SECONDARY,
    )

    legend = ax.legend(loc="upper left", frameon=False, fontsize=10, handlelength=1.6, borderaxespad=0)
    for text in legend.get_texts():
        text.set_color(INK_SECONDARY)

    tail_note = (
        "Bars (value inside): median across repetitions. Markers (value above): worst repetition,\nso the tail is not hidden by the median."
        if "p99.9" in metrics
        else "Bars: median across repetitions (this results file predates p99.9/max reporting -- re-run the benchmark)."
    )
    fig.text(
        0.1,
        0.02,
        f"Source: bench/results/latency_bench.txt, {params['repetitions']} alternating-order repetitions.\n{tail_note}",
        fontsize=8.5,
        color=INK_MUTED,
        linespacing=1.6,
    )

    fig.subplots_adjust(left=0.11, right=0.96, top=0.80, bottom=0.14)
    out_path = OUT_DIR / "latency_percentiles.png"
    fig.savefig(out_path)
    plt.close(fig)
    return out_path


def main() -> None:
    global RESULTS_DIR, OUT_DIR
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--results-dir", type=pathlib.Path, default=RESULTS_DIR)
    ap.add_argument("--out-dir", type=pathlib.Path, default=OUT_DIR)
    args = ap.parse_args()
    RESULTS_DIR, OUT_DIR = args.results_dir.resolve(), args.out_dir.resolve()

    _base_style()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    paths = [plot_throughput_scalability(), plot_latency_percentiles()]
    for p in paths:
        print(f"wrote {p}")


if __name__ == "__main__":
    main()
