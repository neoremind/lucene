#!/usr/bin/env python3
"""
Visualize BytesRefHash term frequency metrics as a heatmap.

Reads the CSV produced by BytesRefHash instrumentation
(enabled via -Dlucene.bytesRefHash.metrics=true) and generates a heatmap where:

  - X-axis: term addition span (buckets of add() calls over time)
  - Y-axis: two rows — "New Term" (cold, first seen) and "Seen Term" (warm/hot, repeated)
  - Color:  proportion of new vs seen terms per bucket (hit ratio)

The heatmap shows the Pareto pattern: as indexing progresses, most terms are
repeated (hot) while new terms become increasingly rare (cold).

Usage:
    python3 bytesrefhash_heatmap.py [--input FILE] [--output FILE] [--downsample N]

System properties for the Java instrumentation:
    -Dlucene.bytesRefHash.metrics=true
    -Dlucene.bytesRefHash.metricsFile=/tmp/bytesrefhash_metrics.csv
    -Dlucene.bytesRefHash.metricsBucketSize=1000
"""

import argparse

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def load_metrics(path: str) -> pd.DataFrame:
    """Load the CSV metrics file. Falls back to line-by-line parsing if the CSV has corrupted
    rows (e.g., from earlier runs that used per-instance writers)."""
    try:
        df = pd.read_csv(path)
        expected = {"bucket_index", "bucket_start", "bucket_end", "new_terms", "seen_terms"}
        missing = expected - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns in CSV: {missing}")
        return df
    except Exception:
        # Fallback: parse line-by-line skipping bad rows
        print("  (falling back to line-by-line parsing due to malformed rows)")
        records = []
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("bucket_index"):
                    continue
                parts = line.split(",")
                # Accept both old 7-column format and new 5-column format
                if len(parts) < 5:
                    continue
                try:
                    records.append(
                        {
                            "bucket_index": int(parts[0]),
                            "bucket_start": int(parts[1]),
                            "bucket_end": int(parts[2]),
                            "new_terms": int(parts[3]),
                            "seen_terms": int(parts[4]),
                        }
                    )
                except (ValueError, IndexError):
                    continue
        if not records:
            raise ValueError(f"No valid data rows found in {path}")
        return pd.DataFrame(records)


def downsample(df: pd.DataFrame, factor: int) -> pd.DataFrame:
    """Downsample by grouping every `factor` consecutive buckets."""
    if factor <= 1:
        return df

    n = len(df)
    group_ids = np.arange(n) // factor
    grouped = df.groupby(group_ids).agg(
        bucket_index=("bucket_index", "first"),
        bucket_start=("bucket_start", "first"),
        bucket_end=("bucket_end", "last"),
        new_terms=("new_terms", "sum"),
        seen_terms=("seen_terms", "sum"),
    )
    return grouped.reset_index(drop=True)


def format_number(n):
    """Format large numbers with K/M/B suffixes."""
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    elif n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


def plot_heatmap(df: pd.DataFrame, output: str):
    """Generate a single heatmap showing hit ratio (seen / total) per bucket.
    Blue = cold (many new terms), Red = hot (all seen terms)."""
    total = df["new_terms"].values + df["seen_terms"].values
    total = np.where(total == 0, 1, total)  # avoid div by zero
    hit_ratio = df["seen_terms"].values.astype(float) / total  # 0 = all new, 1 = all seen

    # Shape as a single-row heatmap
    matrix = hit_ratio.reshape(1, -1)

    fig, ax = plt.subplots(figsize=(16, 3))
    fig.suptitle(
        "BytesRefHash Term Hit Ratio Heatmap\n"
        "(Blue = new terms / cold, Red = seen terms / hot)",
        fontsize=13,
        fontweight="bold",
    )

    norm = mcolors.Normalize(vmin=0, vmax=1)
    im = ax.imshow(
        matrix,
        aspect="auto",
        cmap="RdYlBu_r",  # Red=hot (high hit ratio), Blue=cold (low hit ratio)
        norm=norm,
        interpolation="nearest",
    )
    ax.set_yticks([0])
    ax.set_yticklabels(["Hit Ratio\n(seen/total)"])
    ax.set_xlabel("Term Addition Span (cumulative add() calls)")

    cbar = plt.colorbar(im, ax=ax, orientation="vertical", fraction=0.02, pad=0.01)
    cbar.set_label("Hit Ratio (1.0 = all seen, 0.0 = all new)")

    # X ticks
    x_labels = df["bucket_start"].values
    n_ticks = min(20, len(x_labels))
    tick_positions = np.linspace(0, len(x_labels) - 1, n_ticks, dtype=int)
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(
        [format_number(x_labels[i]) for i in tick_positions], rotation=45, ha="right", fontsize=8
    )

    plt.tight_layout()
    plt.savefig(output, dpi=150, bbox_inches="tight")
    print(f"Heatmap saved to: {output}")
    plt.close()


def plot_stacked_area(df: pd.DataFrame, output: str):
    """Generate a stacked area chart: new terms (blue) vs seen terms (red) over time."""
    fig, ax = plt.subplots(figsize=(14, 5))

    x = df["bucket_start"].values
    total = df["new_terms"].values + df["seen_terms"].values
    total = np.where(total == 0, 1, total)

    new_ratio = df["new_terms"].values.astype(float) / total
    seen_ratio = df["seen_terms"].values.astype(float) / total

    ax.fill_between(x, 0, seen_ratio, alpha=0.7, color="red", label="Seen terms (hit ratio)")
    ax.fill_between(
        x, seen_ratio, 1, alpha=0.7, color="blue", label="New terms (miss ratio)"
    )

    ax.set_xlabel("Cumulative add() calls")
    ax.set_ylabel("Ratio per bucket")
    ax.set_ylim(0, 1)
    ax.set_title("BytesRefHash: Hit Ratio Over Time (Pareto Pattern)")
    ax.legend(loc="center right")

    # Add annotation for overall stats
    total_new = df["new_terms"].sum()
    total_seen = df["seen_terms"].sum()
    overall_ratio = total_seen / (total_new + total_seen) * 100
    ax.annotate(
        f"Overall: {overall_ratio:.1f}% seen terms\n"
        f"({format_number(total_seen)} seen / {format_number(total_new)} new)",
        xy=(0.02, 0.5),
        xycoords="axes fraction",
        fontsize=10,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="wheat", alpha=0.8),
    )

    plt.tight_layout()
    area_output = output.replace(".png", "_area.png")
    plt.savefig(area_output, dpi=150, bbox_inches="tight")
    print(f"Stacked area chart saved to: {area_output}")
    plt.close()


def load_term_length_metrics(path: str) -> pd.DataFrame:
    """Load the term length histogram CSV.

    The last row may be the overflow bucket (">=256") covering all terms with length
    >= MAX_TERM_LENGTH. It is parsed into a numeric length with is_overflow=True so
    callers can plot it separately and exclude it from avg/median statistics."""
    df = pd.read_csv(path)
    expected = {"term_utf8_length", "seen_occurrences", "new_occurrences"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in term length CSV: {missing}")
    length_str = df["term_utf8_length"].astype(str)
    df["is_overflow"] = length_str.str.startswith(">=")
    df["term_utf8_length"] = length_str.str.replace(">=", "", regex=False).astype(int)
    return df


def plot_term_length_distribution(length_df: pd.DataFrame, output: str):
    """Generate a bar chart showing seen-term occurrences by UTF-8 byte length."""
    fig, ax = plt.subplots(figsize=(14, 5))

    numeric_df = length_df[~length_df["is_overflow"]]
    overflow_df = length_df[length_df["is_overflow"]]

    lengths = numeric_df["term_utf8_length"].values
    seen = numeric_df["seen_occurrences"].values
    new = numeric_df["new_occurrences"].values

    bar_width = 0.4
    ax.bar(lengths - bar_width / 2, seen, width=bar_width, color="red", alpha=0.7, label="Seen term occurrences")
    ax.bar(lengths + bar_width / 2, new, width=bar_width, color="blue", alpha=0.7, label="New term occurrences")

    # Overflow bucket (length >= MAX_TERM_LENGTH), drawn hatched at its boundary position
    if len(overflow_df) > 0:
        of_len = overflow_df["term_utf8_length"].values
        of_seen = overflow_df["seen_occurrences"].values
        of_new = overflow_df["new_occurrences"].values
        ax.bar(of_len - bar_width / 2, of_seen, width=bar_width, color="red", alpha=0.4, hatch="//")
        ax.bar(of_len + bar_width / 2, of_new, width=bar_width, color="blue", alpha=0.4, hatch="//")
        if (of_seen.sum() + of_new.sum()) > 0:
            ax.annotate(
                f">={of_len[0]}: {format_number(of_seen[0])} seen / {format_number(of_new[0])} new",
                xy=(of_len[0], max(of_seen[0], of_new[0], 1)),
                xytext=(of_len[0] - 60, max(of_seen[0], of_new[0], 1) * 8),
                fontsize=9,
                arrowprops=dict(arrowstyle="->", color="gray", lw=0.8),
                color="gray",
                ha="left",
            )

    ax.set_xlabel("Term UTF-8 byte length (hatched bar = overflow bucket)")
    ax.set_ylabel("Number of occurrences")
    ax.set_yscale("log")
    ax.set_title("BytesRefHash: Term Occurrences by UTF-8 Length")
    ax.legend()

    # Compute totals first for percentage calculations (include overflow so pcts are of the
    # complete distribution)
    total_seen = length_df["seen_occurrences"].sum()

    # Summarize top-4 seen lengths, always including len=1, as a text block — the hot
    # lengths are adjacent (1..5) so individual arrow annotations would overlap.
    top_indices = list(np.argsort(seen)[-4:][::-1])
    len1_matches = np.where(lengths == 1)[0]
    if len(len1_matches) > 0 and len1_matches[0] not in top_indices:
        top_indices.append(len1_matches[0])
    top_indices.sort(key=lambda idx: lengths[idx])
    summary_lines = ["Seen occurrences:"]
    for idx in top_indices:
        pct = seen[idx] / total_seen * 100
        summary_lines.append(f"len={lengths[idx]}: {format_number(seen[idx])} ({pct:.1f}%)")
    ax.text(
        0.98,
        0.72,
        "\n".join(summary_lines),
        transform=ax.transAxes,
        fontsize=9,
        verticalalignment="top",
        horizontalalignment="right",
        color="darkred",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="wheat", alpha=0.85),
    )

    # Compute weighted average length for seen terms (overflow excluded: its exact lengths
    # are unknown, only a lower bound)
    numeric_total_seen = seen.sum()
    if numeric_total_seen > 0:
        avg_len = np.average(lengths, weights=seen)
        median_idx = np.searchsorted(np.cumsum(seen), numeric_total_seen / 2)
        median_len = lengths[min(median_idx, len(lengths) - 1)]
        ax.axvline(avg_len, color="darkred", linestyle="--", alpha=0.6, label=f"Avg seen length: {avg_len:.1f}")
        ax.axvline(median_len, color="darkred", linestyle=":", alpha=0.6, label=f"Median seen length: {median_len}")
        ax.legend()

    plt.tight_layout()
    len_output = output.replace(".png", "_term_length.png")
    plt.savefig(len_output, dpi=150, bbox_inches="tight")
    print(f"Term length distribution chart saved to: {len_output}")
    plt.close()


def load_top_terms(path: str) -> pd.DataFrame:
    """Load the top-terms CSV. A trailing '# WARNING: ...' line (emitted when the
    tracked-terms cap was hit) is skipped via the comment marker."""
    df = pd.read_csv(path, comment="#")
    expected = {"category", "rank", "term", "term_utf8_length", "occurrences"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in top terms CSV: {missing}")
    return df


def plot_top_terms(top_df: pd.DataFrame, output: str, top_n: int = 25):
    """Plot the top-N terms overall as a horizontal bar chart, highlighting short terms."""
    all_terms = top_df[top_df["category"] == "all"].nsmallest(top_n, "rank")
    short_max_len = top_df[top_df["category"] == "short"]["term_utf8_length"].max()

    fig, ax = plt.subplots(figsize=(10, max(5, top_n * 0.3)))
    colors = [
        "red" if length <= short_max_len else "steelblue"
        for length in all_terms["term_utf8_length"]
    ]
    y = np.arange(len(all_terms))
    ax.barh(y, all_terms["occurrences"], color=colors, alpha=0.8)
    ax.set_yticks(y)
    ax.set_yticklabels(
        [f'{t} ({l}B)' for t, l in zip(all_terms["term"], all_terms["term_utf8_length"])],
        fontsize=8,
    )
    ax.invert_yaxis()
    ax.set_xlabel("Occurrences (new + seen)")
    ax.set_xscale("log")
    ax.set_title(
        f"BytesRefHash: Top {top_n} Terms by Occurrences\n"
        f"(red = short terms <= {short_max_len} bytes, the inline-optimization candidates)"
    )

    plt.tight_layout()
    top_output = output.replace(".png", "_top_terms.png")
    plt.savefig(top_output, dpi=150, bbox_inches="tight")
    print(f"Top terms chart saved to: {top_output}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Generate heatmap from BytesRefHash metrics CSV"
    )
    parser.add_argument(
        "--input",
        "-i",
        default="/tmp/bytesrefhash_metrics.csv",
        help="Path to the metrics CSV file (default: /tmp/bytesrefhash_metrics.csv)",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="/tmp/bytesrefhash_heatmap.png",
        help="Output image path (default: /tmp/bytesrefhash_heatmap.png)",
    )
    parser.add_argument(
        "--downsample",
        "-d",
        type=int,
        default=0,
        help="Downsample factor: group N consecutive buckets into one "
        "(0 = auto based on total buckets, default: 0)",
    )

    args = parser.parse_args()

    print(f"Loading metrics from: {args.input}")
    df = load_metrics(args.input)
    print(f"  Total buckets: {len(df):,}")
    total_adds = df["new_terms"].sum() + df["seen_terms"].sum()
    print(f"  Total add() calls: {total_adds:,.0f}")
    print(
        f"  Total new terms: {df['new_terms'].sum():,.0f}, "
        f"Total seen terms: {df['seen_terms'].sum():,.0f}"
    )

    if total_adds > 0:
        seen_pct = df["seen_terms"].sum() / total_adds * 100
        print(f"  Hit ratio (seen/total): {seen_pct:.1f}%")

        # Auto-downsample if too many buckets for display
        ds_factor = args.downsample
        if ds_factor == 0:
            target_cols = 2000
            if len(df) > target_cols:
                ds_factor = len(df) // target_cols
                print(f"  Auto-downsampling by factor {ds_factor} -> ~{len(df) // ds_factor} columns")
            else:
                ds_factor = 1

        df_ds = downsample(df, ds_factor)
        print(f"  Plotting {len(df_ds)} columns...")

        plot_heatmap(df_ds, args.output)
        plot_stacked_area(df_ds, args.output)
    else:
        print("  (no bucket rows — run too small to fill a bucket; skipping heatmap/area charts)")

    # Plot term length distribution if the file exists
    length_file = args.input.replace(".csv", "_term_length.csv")
    import os

    if os.path.exists(length_file):
        print(f"\n  Loading term length histogram from: {length_file}")
        length_df = load_term_length_metrics(length_file)
        total_seen = length_df["seen_occurrences"].sum()
        total_new = length_df["new_occurrences"].sum()
        # Exclude the overflow bucket from the average: exact lengths are unknown there.
        numeric_df = length_df[~length_df["is_overflow"]]
        avg_len = np.average(
            numeric_df["term_utf8_length"].values,
            weights=numeric_df["seen_occurrences"].values,
        )
        print(f"  Seen term occurrences by length: total={total_seen:,.0f}, avg_length={avg_len:.1f} bytes")
        print(f"  New term occurrences by length: total={total_new:,.0f}")
        overflow = length_df[length_df["is_overflow"]]
        if len(overflow) > 0:
            of_total = overflow["seen_occurrences"].sum() + overflow["new_occurrences"].sum()
            print(f"  Overflow bucket (length >= {overflow['term_utf8_length'].iloc[0]}): {of_total:,.0f} occurrences")
        plot_term_length_distribution(length_df, args.output)
    else:
        print(f"\n  (term length file not found: {length_file}, skipping length distribution chart)")
        print(f"  Re-run benchmark with latest code to generate it.")

    # Plot top terms if the file exists
    top_terms_file = args.input.replace(".csv", "_top_terms.csv")
    if os.path.exists(top_terms_file):
        print(f"\n  Loading top terms from: {top_terms_file}")
        top_df = load_top_terms(top_terms_file)
        short_df = top_df[top_df["category"] == "short"]
        all_df = top_df[top_df["category"] == "all"]
        print(f"  Short terms tracked: {len(short_df):,}, all terms tracked: {len(all_df):,}")
        if len(short_df) > 0:
            total_all_occ = all_df["occurrences"].sum()
            short_in_top = all_df[
                all_df["term_utf8_length"] <= short_df["term_utf8_length"].max()
            ]["occurrences"].sum()
            print(
                f"  Short-term share of top-{len(all_df)} occurrences: "
                f"{short_in_top / total_all_occ * 100:.1f}%"
            )
        plot_top_terms(top_df, args.output)
    else:
        print(f"\n  (top terms file not found: {top_terms_file}, skipping top terms chart)")

    print(f"\nDone! To view:\n  open {args.output}")


if __name__ == "__main__":
    main()
