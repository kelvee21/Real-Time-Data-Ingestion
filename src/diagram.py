"""Draw reports/system_architecture.png (the diagram deliverable) with matplotlib.

    python -m src.diagram
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from src.config import ROOT

OUT = ROOT / "reports" / "system_architecture.png"

# name: (x, y, w, h, title, detail, colour)
Box = tuple[float, float, float, float, str, str, str]
BOXES: dict[str, Box] = {
    "gen": (0.2, 3.2, 2.6, 1.5, "src/data_generator.py",
            "fake events, 1 CSV every N s\nwrites _tmp then renames", "#dbeafe"),
    "dir": (3.6, 3.2, 2.4, 1.5, "data/incoming/",
            "events_*.csv\n(the stream source)", "#fef3c7"),
    "spark": (6.8, 2.2, 3.6, 3.5, "Spark Structured Streaming",
              "readStream.csv (schema, maxFilesPerTrigger)\n"
              "try_cast + validate\nwatermark + dropDuplicates\nforeachBatch(write_batch)",
              "#dcfce7"),
    "ckpt": (6.8, 0.2, 3.6, 1.3, "checkpoint/events/",
             "offsets: files per batch\ncommits + dedup state", "#f3f4f6"),
    "stage": (11.2, 3.9, 2.9, 1.3, "events_staging",
              "JDBC bulk append", "#fce7f3"),
    "events": (11.2, 1.7, 2.9, 1.5, "events",
               "INSERT ... ON CONFLICT\n(event_id) DO NOTHING", "#fce7f3"),
    "metrics": (11.2, 0.0, 2.9, 1.1, "metrics/*.jsonl",
                "listener + per-batch log", "#f3f4f6"),
}
ARROWS = [("gen", "dir", "CSV files"), ("dir", "spark", "new files"),
          ("spark", "stage", "1. append"), ("stage", "events", "2. merge (SQL)"),
          ("spark", "ckpt", "progress"), ("spark", "metrics", "")]
SIDES = {("gen", "dir"): ("r", "l"), ("dir", "spark"): ("r", "l"), ("spark", "stage"): ("r", "l"),
         ("stage", "events"): ("b", "t"), ("spark", "ckpt"): ("b", "t"),
         ("spark", "metrics"): ("r", "l")}


def centre(name: str, side: str) -> tuple[float, float]:
    x, y, w, h = BOXES[name][:4]
    return {"r": (x + w, y + h / 2), "l": (x, y + h / 2), "b": (x + w / 2, y),
            "t": (x + w / 2, y + h)}[side]


def draw(out: Path = OUT) -> Path:
    fig, ax = plt.subplots(figsize=(14.5, 6.2))
    ax.set_xlim(0, 14.4)
    ax.set_ylim(-0.3, 6.4)
    ax.axis("off")
    ax.add_patch(FancyBboxPatch((11.0, 1.5), 3.3, 3.9, boxstyle="round,pad=0.05", fill=False,
                                ls="--", ec="#9ca3af"))
    ax.text(12.65, 5.55, "PostgreSQL (ecommerce)", ha="center", fontsize=10, color="#4b5563")
    for x, y, w, h, title, detail, colour in BOXES.values():
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.05", fc=colour, ec="#374151"))
        ax.text(x + w / 2, y + h - 0.3, title, ha="center", va="top", fontsize=10, weight="bold")
        ax.text(x + w / 2, y + h - 0.65, detail, ha="center", va="top", fontsize=8.5)
    for a, b, label in ARROWS:
        sa, sb = SIDES[(a, b)]
        p, q = centre(a, sa), centre(b, sb)
        if (a, b) == ("spark", "metrics"):
            p = (p[0], 2.6)
        ax.add_patch(FancyArrowPatch(p, q, arrowstyle="-|>", mutation_scale=14, color="#374151"))
        if label:
            ax.text((p[0] + q[0]) / 2, (p[1] + q[1]) / 2 + 0.15, label, ha="center", fontsize=8.5,
                    color="#1f2937")
    ax.set_title("Real-time e-commerce event ingestion: CSV files -> Spark Structured Streaming -> PostgreSQL",
                 fontsize=12)
    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=130)
    plt.close(fig)
    return out


if __name__ == "__main__":
    print(f"wrote {draw()}")
