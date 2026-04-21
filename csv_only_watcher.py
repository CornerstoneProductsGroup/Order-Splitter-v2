"""
Standalone Depot CSV watcher (Depot input folder only).

Lowe's FedEx CSV automation lives only in ``watcher.py`` with the CSV watcher enabled.

For the intended split (PDF watcher = packing slips + labels; CSV watcher = Depot + Lowe's),
use ``watcher.py`` with ``ORDER_SPLITTER_DISABLE_CSV_WATCH`` unset and launch via ``run_watcher.cmd``,
or CSV-only via ``run_csv_orders_only.cmd``. This script exists for Depot-only scheduled tasks.

Behavior:
- Polls Depot CSV input folder every few seconds.
- Baselines files already present at startup (does not process backlog).
- Processes only files that are new or changed after startup.
- Keeps raw input files in place (archive is copy-only via process module).
- Reloads rules workbook before each file (with last-good fallback if locked).

Run:
  python csv_only_watcher.py
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import time
from pathlib import Path

import process_depot_csv_orders as depot_csv

STATE_FILENAME = ".depot_csv_watch_state.json"


def _resolve_rules_path(configured_path: Path) -> Path:
    candidates: list[Path] = []
    script_dir = Path(__file__).resolve().parent

    if configured_path.is_absolute():
        candidates.append(configured_path)

    candidates.extend(
        [
            script_dir / configured_path,
            Path.cwd() / configured_path,
            Path(r"C:\OrderSplitter") / depot_csv.RULES_FILENAME,
            configured_path,
        ]
    )

    for p in candidates:
        if p.exists():
            return p

    return candidates[0] if candidates else configured_path


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {"files": {}}

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict) and isinstance(raw.get("files"), dict):
            return raw
    except Exception:
        pass

    return {"files": {}}


def _save_state(path: Path, state: dict) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _wait_for_file_ready(path: Path, stable_secs: float = 1.0, timeout_secs: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout_secs
    prev_size = -1

    while time.monotonic() < deadline:
        try:
            size = path.stat().st_size
        except OSError:
            time.sleep(0.2)
            continue

        if size == prev_size and size > 0:
            return True

        prev_size = size
        time.sleep(stable_secs)

    return False


def _fingerprint(path: Path) -> tuple[int, int]:
    st = path.stat()
    return st.st_mtime_ns, st.st_size


def _needs_processing(path: Path, state_files: dict) -> bool:
    key = str(path).lower()
    current_mtime_ns, current_size = _fingerprint(path)
    entry = state_files.get(key)

    if not isinstance(entry, dict):
        return True

    return (
        int(entry.get("mtime_ns", -1)) != current_mtime_ns
        or int(entry.get("size", -1)) != current_size
    )


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Standalone Depot CSV watcher")
    parser.add_argument("--input", type=Path, default=depot_csv.INPUT_DIR)
    parser.add_argument("--output", type=Path, default=depot_csv.OUTPUT_DIR)
    parser.add_argument("--archive", type=Path, default=depot_csv.ARCHIVE_DIR)
    parser.add_argument("--rules", type=Path, default=Path(depot_csv.RULES_FILENAME))
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--state-file", type=Path, default=script_dir / STATE_FILENAME)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("csv-only-watcher")

    args = parse_args()

    input_dir: Path = args.input
    output_dir: Path = args.output
    archive_dir: Path = args.archive
    state_file: Path = args.state_file
    poll_seconds: float = max(1.0, float(args.poll_seconds))

    rules_path = _resolve_rules_path(args.rules)

    logger.info("Input folder   : %s", input_dir)
    logger.info("Output folder  : %s", output_dir)
    logger.info("Archive folder : %s", archive_dir)
    logger.info("Rules workbook : %s", rules_path)
    logger.info("State file     : %s", state_file)
    logger.info("Dry run        : %s", bool(args.dry_run))

    if not input_dir.exists():
        raise FileNotFoundError(f"Input folder not found: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    state_file.parent.mkdir(parents=True, exist_ok=True)

    state = _load_state(state_file)
    state_files = state.setdefault("files", {})

    # Baseline any CSVs already in the folder at startup so we do not process
    # historical backlog when the watcher starts/restarts.
    startup_pending = sorted(input_dir.glob("*.csv"), key=lambda p: p.name.lower())
    startup_baselined = 0
    for csv_path in startup_pending:
        try:
            mtime_ns, size = _fingerprint(csv_path)
        except FileNotFoundError:
            continue
        except OSError as e:
            logger.error("Could not stat %s at startup baseline: %s", csv_path.name, e)
            continue

        state_files[str(csv_path).lower()] = {
            "mtime_ns": mtime_ns,
            "size": size,
            "last_processed": "startup-baseline",
        }
        startup_baselined += 1

    _save_state(state_file, state)
    logger.info("Startup baseline recorded for %d existing CSV file(s)", startup_baselined)

    rules: dict[str, depot_csv.SkuRule] = {}
    next_heartbeat = 0.0

    logger.info("CSV-only watcher is running. Press Ctrl+C to stop.")

    try:
        while True:
            now = time.monotonic()
            pending = sorted(input_dir.glob("*.csv"), key=lambda p: p.name.lower())

            if now >= next_heartbeat:
                next_heartbeat = now + 60.0
                logger.info("Heartbeat: %d CSV file(s) visible in %s", len(pending), input_dir)

            for csv_path in pending:
                try:
                    if not _needs_processing(csv_path, state_files):
                        continue
                except FileNotFoundError:
                    continue
                except OSError as e:
                    logger.error("Could not stat %s: %s", csv_path.name, e)
                    continue

                if not _wait_for_file_ready(csv_path):
                    logger.error("Timed out waiting for %s to finish writing; skipping this cycle", csv_path.name)
                    continue

                try:
                    loaded_rules = depot_csv.load_sku_rules(rules_path)
                    rules = loaded_rules
                except Exception as e:
                    if rules:
                        logger.error("Could not reload rules from %s: %s (keeping %d cached rules)", rules_path, e, len(rules))
                    else:
                        logger.error("Could not load rules from %s: %s", rules_path, e)
                        continue

                try:
                    out_path, out_rows, unknown_skus, archived_to = depot_csv.process_one_csv(
                        raw_csv=csv_path,
                        rules=rules,
                        output_dir=output_dir,
                        archive_dir=archive_dir,
                        dry_run=bool(args.dry_run),
                    )

                    if args.dry_run:
                        logger.info("DRY RUN: %s -> would create %d output row(s)", csv_path.name, out_rows)
                    else:
                        assert out_path is not None
                        assert archived_to is not None
                        logger.info("Processed: %s -> %s (%d rows)", csv_path.name, out_path.name, out_rows)
                        logger.info("Archived copy: %s", archived_to)

                    if unknown_skus:
                        logger.warning("%d unknown SKU row(s) skipped in %s", unknown_skus, csv_path.name)

                    mtime_ns, size = _fingerprint(csv_path)
                    state_files[str(csv_path).lower()] = {
                        "mtime_ns": mtime_ns,
                        "size": size,
                        "last_processed": dt.datetime.now().isoformat(timespec="seconds"),
                    }
                    _save_state(state_file, state)

                except Exception as e:
                    logger.exception("Unhandled error processing %s: %s", csv_path.name, e)

            time.sleep(poll_seconds)

    except KeyboardInterrupt:
        logger.info("CSV-only watcher stopped.")


if __name__ == "__main__":
    main()
