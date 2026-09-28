# Honeycomb query results extractor

Exports Honeycomb triggers and the time-series results of each trigger's query,
to help migrate triggers to Splunk.

For each dataset in `datasets.txt` the script:

1. Fetches every trigger and saves them all to `output/triggers.json`.
2. Creates a directory per trigger.
3. Runs the trigger's query over your time range as a time series, with one
   data point per trigger evaluation interval (`frequency`).
4. Saves the results in that trigger's directory.

## Setup

Requires Python 3.9+.

```powershell
python -m venv .venv
.venv\Scripts\activate          # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
copy datasets.example.txt datasets.txt   # then list your dataset slugs
```

### API key

The key is never stored in the repo. The script looks for it in this order:

1. The `HONEYCOMB_API_KEY` environment variable
   (PowerShell: `$env:HONEYCOMB_API_KEY = "..."`).
2. A `.env` file (copy `.env.example`). `.env` is gitignored.
3. A hidden prompt when you run the script.

The key needs the **Manage Queries and Columns** and **Run Queries** permissions.
The Query Data API is an Enterprise feature.

## Usage

```powershell
python honeycomb_extractor.py --start 2026-09-07 --end 2026-09-27
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--start` | required | Start of the range. ISO 8601, UTC unless a zone is given. |
| `--end` | now | End of the range. A bare date includes that whole day. |
| `--datasets-file` | `datasets.txt` | One dataset slug per line. `#` comments allowed. Use `__all__` for environment-wide triggers. |
| `--output-dir` | `output` | Where results are written. Gitignored. |
| `--skip-disabled` | off | Don't run queries for disabled triggers. |
| `--triggers-only` | off | Only export `triggers.json`. |
| `--force` | off | Re-run triggers that were already exported. |
| `--api-url` | `https://api.honeycomb.io` | Use `https://api.eu1.honeycomb.io` for the EU region. |

Re-running the same command resumes: triggers whose results were saved
without errors are skipped.

## Output

```
output/
  triggers.json                          # {dataset slug: [triggers]}
  <dataset>/<trigger-name>__<trigger-id>/
    trigger.json                         # trigger definition
    query.json                           # query spec that was run
    results.json                         # one entry per window, with data.series
```

## Honeycomb API limits handled

- **7-day maximum per query.** Longer ranges are split into 7-day windows.
  Each window appears separately in `results.json`.
- **10 query results per minute.** Requests are spaced out and 429 responses
  are retried, so a large run takes a while (about 6.5 seconds per window).
- **Series size.** Granularity is the trigger's frequency, raised if needed so a
  window has at most 1,000 buckets. For a 7-day window that is at least 605 seconds,
  so triggers that run every minute get ~10-minute points.
