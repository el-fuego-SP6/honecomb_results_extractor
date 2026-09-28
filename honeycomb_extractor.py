#!/usr/bin/env python3
"""Export Honeycomb triggers and the time-series results of their queries.

For every dataset listed in a datasets file, this script:
  1. fetches all triggers and writes them to <output>/triggers.json
  2. creates one directory per trigger
  3. runs each trigger's query over the requested time range as a time series
     (split into windows of at most 7 days, the Query Data API maximum)
  4. writes the results to that trigger's directory

The API key is read from the HONEYCOMB_API_KEY environment variable, a local
.env file, or an interactive prompt. It is never accepted as a command-line
argument so it does not end up in shell history.
"""

from __future__ import annotations

import argparse
import copy
import getpass
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

US_API_URL = "https://api.honeycomb.io"
MAX_WINDOW = timedelta(days=7)  # Query Data API rejects longer queries with a 400.
MAX_SERIES_BUCKETS = 1000  # Keep granularity coarse enough for long windows.
QUERY_RESULT_INTERVAL_SECONDS = 6.5  # Create Query Result is limited to 10/minute.
POLL_TIMEOUT_SECONDS = 300
# Fields that describe a saved query rather than its definition, or that this
# script sets itself for each window.
SPEC_FIELDS_TO_DROP = ("id", "time_range", "start_time", "end_time", "granularity")


class HoneycombError(Exception):
    pass


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

def load_api_key(env_file: Path) -> str:
    key = os.environ.get("HONEYCOMB_API_KEY")
    if not key and env_file.is_file():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith("HONEYCOMB_API_KEY="):
                key = line.split("=", 1)[1].strip().strip("'\"")
                break
    if not key and sys.stdin.isatty():
        key = getpass.getpass("Honeycomb API key: ").strip()
    if not key:
        sys.exit("No API key found. Set HONEYCOMB_API_KEY or add it to .env.")
    return key


def load_datasets(path: Path) -> list[str]:
    """One dataset slug per line. Blank lines and # comments are ignored."""
    slugs = []
    for line in path.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line and line not in slugs:
            slugs.append(line)
    if not slugs:
        sys.exit(f"No dataset slugs found in {path}")
    return slugs


def parse_utc(value: str, is_end: bool) -> datetime:
    """Parse an ISO date or datetime. Values without a time zone are UTC.

    A bare end date (e.g. 2026-09-28) means the end of that day, so the range
    includes the whole day.
    """
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an ISO 8601 date or datetime: {value!r}")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    if is_end and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value.strip()):
        parsed += timedelta(days=1)
    return parsed


def split_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    windows = []
    cursor = start
    while cursor < end:
        window_end = min(cursor + MAX_WINDOW, end)
        windows.append((cursor, window_end))
        cursor = window_end
    return windows


def granularity_for(frequency: int | None, window_seconds: int) -> int:
    """One bucket per trigger evaluation, coarsened if a window would have too many."""
    minimum = math.ceil(window_seconds / MAX_SERIES_BUCKETS)
    return max(int(frequency or 0), minimum, 1)


def safe_dir_name(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-.")
    return cleaned[:80] or "trigger"


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=False) + "\n")
    tmp.replace(path)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------
# API client
# --------------------------------------------------------------------------

class Honeycomb:
    def __init__(self, api_key: str, api_url: str):
        self.base = api_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "X-Honeycomb-Team": api_key,
            "Content-Type": "application/json",
            "User-Agent": "honeycomb-query-results-extractor",
        })
        self._last_query_result_at = 0.0

    def request(self, method: str, path: str, body=None, max_attempts: int = 6):
        url = f"{self.base}{path}"
        for attempt in range(1, max_attempts + 1):
            try:
                resp = self.session.request(method, url, json=body, timeout=60)
            except requests.RequestException as exc:
                if attempt == max_attempts:
                    raise HoneycombError(f"{method} {path}: {exc}") from exc
                time.sleep(2 ** attempt)
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt == max_attempts:
                    break
                wait = _retry_after(resp) or min(60, 2 ** attempt * 2)
                print(f"    {resp.status_code} from {path}; retrying in {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue
            if not resp.ok:
                break
            return resp.json() if resp.content else None
        raise HoneycombError(f"{method} {path}: HTTP {resp.status_code}: {resp.text[:500]}")

    def auth(self):
        return self.request("GET", "/1/auth")

    def triggers(self, dataset: str):
        return self.request("GET", f"/1/triggers/{dataset}")

    def get_query(self, dataset: str, query_id: str):
        return self.request("GET", f"/1/queries/{dataset}/{query_id}")

    def create_query(self, dataset: str, spec: dict):
        return self.request("POST", f"/1/queries/{dataset}", spec)

    def run_query(self, dataset: str, query_id: str) -> dict:
        wait = QUERY_RESULT_INTERVAL_SECONDS - (time.monotonic() - self._last_query_result_at)
        if wait > 0:
            time.sleep(wait)
        created = self.request(
            "POST", f"/1/query_results/{dataset}",
            {"query_id": query_id, "disable_series": False},
        )
        self._last_query_result_at = time.monotonic()

        result_id = created["id"]
        deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
        delay = 1.0
        result = created
        while not result.get("complete"):
            if time.monotonic() > deadline:
                raise HoneycombError(f"query result {result_id} did not complete in {POLL_TIMEOUT_SECONDS}s")
            time.sleep(delay)
            delay = min(delay * 1.5, 5.0)
            result = self.request("GET", f"/1/query_results/{dataset}/{result_id}")
        return result


def _retry_after(resp) -> int | None:
    value = resp.headers.get("Retry-After") or resp.headers.get("RateLimit-Reset")
    try:
        return max(1, int(float(value)))
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------
# Export
# --------------------------------------------------------------------------

def resolve_spec(client: Honeycomb, dataset: str, trigger: dict) -> dict:
    if trigger.get("query"):
        spec = copy.deepcopy(trigger["query"])
    elif trigger.get("query_id"):
        spec = client.get_query(dataset, trigger["query_id"])
    else:
        raise HoneycombError("trigger has neither 'query' nor 'query_id'")
    for field in SPEC_FIELDS_TO_DROP:
        spec.pop(field, None)
    return spec


def export_trigger(client, dataset, trigger, trigger_dir, windows, force) -> bool:
    results_path = trigger_dir / "results.json"
    if results_path.exists() and not force:
        existing = json.loads(results_path.read_text())
        if not any(w.get("error") for w in existing.get("windows", [])):
            print("    already exported; skipping (use --force to re-run)")
            return True

    write_json(trigger_dir / "trigger.json", trigger)
    try:
        spec = resolve_spec(client, dataset, trigger)
    except HoneycombError as exc:
        write_json(results_path, {"trigger_id": trigger.get("id"), "error": str(exc), "windows": []})
        print(f"    ERROR: {exc}", file=sys.stderr)
        return False
    write_json(trigger_dir / "query.json", spec)

    out = {
        "trigger_id": trigger.get("id"),
        "trigger_name": trigger.get("name"),
        "dataset": dataset,
        "frequency_seconds": trigger.get("frequency"),
        "threshold": trigger.get("threshold"),
        "exported_at": iso(datetime.now(timezone.utc)),
        "windows": [],
    }
    ok = True
    for start, end in windows:
        span = int((end - start).total_seconds())
        window_spec = dict(spec,
                           start_time=int(start.timestamp()),
                           end_time=int(end.timestamp()),
                           granularity=granularity_for(trigger.get("frequency"), span))
        entry = {"start": iso(start), "end": iso(end), "granularity": window_spec["granularity"]}
        print(f"    window {entry['start']} -> {entry['end']} (granularity {entry['granularity']}s)")
        try:
            query = client.create_query(dataset, window_spec)
            result = client.run_query(dataset, query["id"])
            entry.update({
                "query_id": query["id"],
                "query_result_id": result.get("id"),
                "data": result.get("data"),
                "links": result.get("links"),
            })
        except HoneycombError as exc:
            ok = False
            entry["error"] = str(exc)
            print(f"      ERROR: {exc}", file=sys.stderr)
        out["windows"].append(entry)
        write_json(results_path, out)  # Save progress after every window.
    return ok


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--datasets-file", type=Path, default=Path("datasets.txt"),
                        help="file with one dataset slug per line (default: datasets.txt)")
    parser.add_argument("--start", required=True, type=lambda v: parse_utc(v, False),
                        help="start of the range, ISO 8601, UTC unless a zone is given")
    parser.add_argument("--end", type=lambda v: parse_utc(v, True),
                        help="end of the range, ISO 8601; a bare date includes that whole day (default: now)")
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--api-url", default=US_API_URL)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--skip-disabled", action="store_true", help="don't run queries for disabled triggers")
    parser.add_argument("--triggers-only", action="store_true", help="export triggers without running queries")
    parser.add_argument("--force", action="store_true", help="re-run triggers that were already exported")
    args = parser.parse_args(argv)

    end = args.end or datetime.now(timezone.utc).replace(microsecond=0)
    if args.start >= end:
        parser.error("--start must be before --end")
    windows = split_windows(args.start, end)

    datasets = load_datasets(args.datasets_file)
    client = Honeycomb(load_api_key(args.env_file), args.api_url)

    auth = client.auth()
    team = (auth.get("team") or {}).get("slug")
    env = (auth.get("environment") or {}).get("slug")
    print(f"Team: {team}  Environment: {env or '(none: Honeycomb Classic)'}")
    print(f"Range: {iso(args.start)} -> {iso(end)} in {len(windows)} window(s)")

    all_triggers = {}
    for dataset in datasets:
        try:
            all_triggers[dataset] = client.triggers(dataset)
            print(f"{dataset}: {len(all_triggers[dataset])} trigger(s)")
        except HoneycombError as exc:
            all_triggers[dataset] = {"error": str(exc)}
            print(f"{dataset}: ERROR {exc}", file=sys.stderr)
    write_json(args.output_dir / "triggers.json", all_triggers)

    failures = [d for d, t in all_triggers.items() if isinstance(t, dict)]
    if args.triggers_only:
        return 1 if failures else 0

    for dataset, triggers in all_triggers.items():
        if isinstance(triggers, dict):
            continue
        for trigger in triggers:
            name = trigger.get("name") or "unnamed"
            if args.skip_disabled and trigger.get("disabled"):
                print(f"  [{dataset}] {name}: disabled, skipped")
                continue
            print(f"  [{dataset}] {name}")
            trigger_dir = args.output_dir / dataset / f"{safe_dir_name(name)}__{trigger.get('id')}"
            if not export_trigger(client, dataset, trigger, trigger_dir, windows, args.force):
                failures.append(f"{dataset}/{name}")

    if failures:
        print(f"\nFinished with {len(failures)} failure(s):", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        print("Re-run the same command to retry only the failed triggers.", file=sys.stderr)
        return 1
    print(f"\nDone. Results are in {args.output_dir}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
