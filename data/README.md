# data/ (gitignored except this note)

made by the code, never committed:
- `incoming/` - the stream source. `python data_generator.py` writes
  `events_<time>.csv` here, the streaming job watches it. files whose name
  starts with `_` are still being written, spark skips them.
- `perf_P2/`, `perf_P3/` - the input folders of the performance runs in
  `reports/performance_metrics.md` (each run had its own folder and checkpoint).
- `notebook/` - `analysis.ipynb` makes its own `incoming/`, `checkpoint/` and
  `metrics/` here and wipes them at the start, so it never touches the real ones.

the checkpoint of the real job is `checkpoint/events/` in the lab root (which
files are done + the dedup state). delete it together with `incoming/` for a
clean start, see `reports/user_guide.md` §8.

the tests do not need any of this. they build fake rows in `tests/helpers.py`.
