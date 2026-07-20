#!/usr/bin/env python3
"""Renders the two graphs that most directly support this project's core
claims: that the epoll reactor scales where a thread-per-connection design
doesn't, and that the heartbeat round trip is genuinely low-latency.

Unlike an earlier version of this script, the numbers are NOT hardcoded here.
They're parsed straight out of the saved benchmark output in bench/results/,
so there's no manual transcription step to go stale -- refreshing the charts
is "re-run the benchmarks, re-run this script," not "re-run the benchmarks,
read the numbers, hand-edit some constants" (see scripts/run_benchmarks_and_plot.sh
for the full, one-shot version of that first step).

Usage:
    python3 scripts/generate_key_graphs.py
    # reads:
    #   bench/results/latency_bench.txt
    #   bench/results/throughput_bench_epoll.txt
    #   bench/results/throughput_bench_naive.txt
    # writes:
    #   bench/charts/throughput_scalability.png
    #   bench/charts/latency_percentiles.png
"""

from __future__ import annotations

import pathlib
import re

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

matplotlib.use("Agg")

# --- Palette -----------------------------------------------------------
# Fixed categorical order (never reassigned per-chart): slot 1 = blue,
# slot 2 = green. Both graphs use the same two entities in the same order
# so color means the same thing across the whole pair.
SURFACE = "#fcfcfb"
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
BASELINE = "#c3c2b7"

SERIES_1 = "#2a78d6"  # epoll reactor / tcp_nodelay=true
SERIES_2 = "#008300"  # naive thread-per-connection / tcp_nodelay=false

# matplotlib's font manager resolves real installed font names, not CSS
# aliases like "system-ui" -- DejaVu Sans ships with matplotlib itself, so
# this renders identically regardless of what's installed on the machine,
# with no fallback-chain warnings for names that aren't actually present.
SANS = ["DejaVu Sans"]

ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT_DIR / "bench" / "results"
OUT_DIR = ROOT_DIR / "bench" / "charts"


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
    """Recessive grid, no top/right spines, thin baseline -- the chart earns
    attention through the data marks, not through frame decoration."""
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


def parse_latency_results(text: str) -> dict[str, list[float]]:
    """{"true": [p50, p90, p99], "false": [p50, p90, p99]} in microseconds --
    the median of each percentile's reported min/median/max spread, from
    `./bin/latency_bench`'s own stdout."""
    result: dict[str, list[float]] = {}
    current: str | None = None
    for line in text.splitlines():
        header = re.match(r"tcp_nodelay=(true|false)\s+across \d+ repetitions:", line.strip())
        if header:
            current = header.group(1)
            result[current] = []
            continue
        if current is not None:
            m = re.search(r"p\d+:\s+min=[\d.]+us\s+median=([\d.]+)us", line)
            if m:
                result[current].append(float(m.group(1)))
    if set(result) != {"true", "false"} or any(len(v) != 3 for v in result.values()):
        raise SystemExit(
            "could not parse latency_bench.txt into two configs x three percentiles -- "
            "was the file truncated, or does the output format no longer match?"
        )
    return result


def parse_throughput_epoll_ramp(text: str) -> dict[int, float]:
    """{connection_count: steady_cpu_median_pct}, from the 'connection count
    ramp' section only. The epoll_max_events batch-size sweep further down
    the same file reuses N=5000 with different results and must not bleed
    into this -- the text is truncated at that section's header first."""
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


def plot_throughput_scalability() -> pathlib.Path:
    epoll = parse_throughput_epoll_ramp(_require_text(RESULTS_DIR / "throughput_bench_epoll.txt"))
    naive = parse_throughput_naive(_require_text(RESULTS_DIR / "throughput_bench_naive.txt"))

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

    # Direct end labels at the last point only -- selective, not a number
    # crowding every marker.
    ax.annotate(
        f"{epoll_cpu_pct[-1]:.1f}%",
        (x[-1], epoll_cpu_pct[-1]),
        xytext=(10, 4),
        textcoords="offset points",
        color=SERIES_1,
        fontsize=11,
        fontweight="bold",
    )
    ax.annotate(
        f"{naive_cpu_pct[-1]:.1f}%",
        (x[-1], naive_cpu_pct[-1]),
        xytext=(10, -2),
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
        "The epoll reactor scales; a thread per connection doesn't",
        fontsize=17,
        fontweight="bold",
        color=INK_PRIMARY,
    )
    fig.text(
        0.08,
        0.905,
        "Steady-state CPU at the same connection count and heartbeat cadence (1000ms)",
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
        "(median of 3 reps per point).\nSee PROJECT_PLAN.md §19 for this sandbox's run-to-run variance on absolute CPU%.",
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
    percentiles = ["p50", "p90", "p99"]
    nodelay_true_us = latency["true"]
    nodelay_false_us = latency["false"]

    fig, ax = plt.subplots(figsize=(8.6, 6.2), dpi=200)
    _strip_chrome(ax)

    x = np.arange(len(percentiles))
    width = 0.32

    bars_true = ax.bar(
        x - width / 2,
        nodelay_true_us,
        width,
        color=SERIES_1,
        label="tcp_nodelay = true",
        zorder=3,
    )
    bars_false = ax.bar(
        x + width / 2,
        nodelay_false_us,
        width,
        color=SERIES_2,
        label="tcp_nodelay = false",
        zorder=3,
    )

    for bars in (bars_true, bars_false):
        for rect in bars:
            height = rect.get_height()
            ax.annotate(
                f"{height:.2f}µs",
                (rect.get_x() + rect.get_width() / 2, height),
                xytext=(0, 5),
                textcoords="offset points",
                ha="center",
                fontsize=10,
                color=INK_SECONDARY,
                fontweight="normal",
            )

    ax.set_xticks(x)
    ax.set_xticklabels(percentiles)
    ax.set_ylabel("PING → PONG round-trip latency (µs)", fontsize=11)
    ax.set_ylim(0, max(nodelay_true_us + nodelay_false_us) * 1.22)

    fig.text(
        0.1,
        0.955,
        "The heartbeat round trip stays single-digit microseconds",
        fontsize=16,
        fontweight="bold",
        color=INK_PRIMARY,
    )
    fig.text(
        0.1,
        0.905,
        "20,000 measured round trips × 5 repetitions per config, real sockets, loopback",
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
        0.1,
        0.02,
        "Source: bench/results/latency_bench.txt, median across 5 alternating-order repetitions.",
        fontsize=8.5,
        color=INK_MUTED,
    )

    fig.subplots_adjust(left=0.11, right=0.96, top=0.80, bottom=0.13)
    out_path = OUT_DIR / "latency_percentiles.png"
    fig.savefig(out_path)
    plt.close(fig)
    return out_path


def main() -> None:
    _base_style()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    paths = [plot_throughput_scalability(), plot_latency_percentiles()]
    for p in paths:
        print(f"wrote {p}")


if __name__ == "__main__":
    main()
