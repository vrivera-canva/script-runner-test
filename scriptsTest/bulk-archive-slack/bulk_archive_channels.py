#!/usr/bin/env python3
"""
bulk_archive_channels.py — Bulk-archive stale Slack channels.

Reads Slack channel IDs from channels_to_archive.txt, skips channels already
recorded as complete in archive_results.csv, and archives the remaining IDs with
Slack's admin.conversations.bulkArchive API. A successful real run writes
per-channel CSV results and, when SCRIPT_OUTPUT_FILE is set, a JSON summary for
Script Runner.

Required env vars (injected automatically by Script Runner):
  SLACK_TOKEN        — Slack admin user token used to call admin.conversations.bulkArchive.

Dry-run behaviour (SCRIPT_DRY_RUN=true, the default):
  Reads the channel list, computes the archive batches, and logs each channel
  that would be archived. It does not call Slack and does not write
  archive_results.csv; it only writes SCRIPT_OUTPUT_FILE when provided.

Local dry-run (no side effects):
  SCRIPT_DRY_RUN=true SLACK_TOKEN=xoxp-... python3 bulk_archive_channels.py

Local real run:
  SCRIPT_DRY_RUN=false SLACK_TOKEN=xoxp-... python3 bulk_archive_channels.py
"""

import csv
import datetime
import json
import logging
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DRY_RUN = os.environ.get("SCRIPT_DRY_RUN", "true").lower() == "true"
SLACK_TOKEN = os.environ.get("SLACK_TOKEN", "").strip()

_BASE_DIR = pathlib.Path(__file__).resolve().parent
_BULK_ARCHIVE_URL = "https://slack.com/api/admin.conversations.bulkArchive"
_MAX_BACKOFF_SECONDS = 60

_DONE_OUTCOMES = frozenset({"archived", "already_archived", "skipped"})

_FATAL_ERRORS = frozenset(
    {
        "not_authed",
        "invalid_auth",
        "missing_scope",
        "not_allowed_token_type",
        "access_denied",
        "enterprise_is_restricted",
        "team_access_not_granted",
        "account_inactive",
        "token_expired",
        "token_revoked",
        "two_factor_setup_required",
        "restricted_action",
    }
)


def _env_int(name: str, default: int, *, minimum: int, maximum: int | None = None) -> int:
    """Read an integer environment variable with bounds checking."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be at most {maximum}, got {value}")
    return value


def _env_path(name: str, default_filename: str) -> pathlib.Path:
    """Read a path environment variable, resolving relative paths beside this script."""
    raw = os.environ.get(name, default_filename)
    path = pathlib.Path(raw)
    if path.is_absolute():
        return path
    return _BASE_DIR / path


def _backoff_seconds(error_attempt: int) -> int:
    """Exponential backoff (5, 10, 20, ...) capped at _MAX_BACKOFF_SECONDS."""
    return min(_MAX_BACKOFF_SECONDS, 5 * 2 ** (error_attempt - 1))


def _retry_after_seconds(raw_value: str | None, default: int) -> int:
    """Parse Slack's Retry-After value, falling back to a safe default."""
    if not raw_value:
        return default
    try:
        return max(1, int(float(raw_value)))
    except ValueError:
        return default


def _chunk(items: list[str], size: int) -> list[list[str]]:
    """Split a list into consecutive chunks of at most size items."""
    return [items[start : start + size] for start in range(0, len(items), size)]


def _load_channel_ids(channels_file: pathlib.Path) -> list[str]:
    """Read channel IDs from a text/CSV-ish file, skipping blanks, comments, and duplicates."""
    if not channels_file.exists():
        raise FileNotFoundError(f"Channel list not found: {channels_file}")

    channel_ids: list[str] = []
    for line in channels_file.read_text(encoding="utf-8").splitlines():
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#"):
            continue
        channel_id = cleaned.split(",", 1)[0].strip()
        if channel_id.lower() in {"channel_id", "channel id", "id"}:
            continue
        channel_ids.append(channel_id)

    return list(dict.fromkeys(channel_ids))


def _load_done(results_file: pathlib.Path) -> set[str]:
    """Load channel IDs already completed in a previous real run."""
    if not results_file.exists():
        return set()

    with results_file.open(newline="", encoding="utf-8") as existing_results:
        reader = csv.DictReader(existing_results)
        return {
            row["channel_id"]
            for row in reader
            if row.get("channel_id") and row.get("outcome") in _DONE_OUTCOMES
        }


def _results_from_response(
    channel_ids: list[str], body: dict[str, Any]
) -> list[tuple[str, str, str]]:
    """Map a successful bulkArchive response to per-channel results."""
    not_added: dict[str, str] = {}
    for item in body.get("not_added", []):
        if isinstance(item, dict) and item.get("channel_id"):
            not_added[item["channel_id"]] = item.get("error", "not_added")

    results: list[tuple[str, str, str]] = []
    for channel_id in channel_ids:
        if channel_id not in not_added:
            results.append((channel_id, "archived", ""))
            continue
        detail = not_added[channel_id]
        outcome = "already_archived" if detail == "already_archived" else "skipped"
        results.append((channel_id, outcome, detail))
    return results


def _archive_batch(
    channel_ids: list[str],
    token: str,
    *,
    max_retries: int,
    in_progress_wait_seconds: int,
) -> list[tuple[str, str, str]]:
    """Archive up to 100 channels in one admin.conversations.bulkArchive call."""
    payload = json.dumps({"channel_ids": channel_ids}).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    error_attempts = 0
    while True:
        request = urllib.request.Request(url=_BULK_ARCHIVE_URL, data=payload, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                body = json.loads(response.read())
        except urllib.error.HTTPError as error:
            if error.code == 429:
                wait = _retry_after_seconds(error.headers.get("Retry-After"), default=5)
                logger.warning("Batch rate-limited (HTTP 429); waiting %ss.", wait)
                time.sleep(wait)
                continue

            error_detail = error.read().decode("utf-8", errors="replace")
            error_attempts += 1
            if error_attempts >= max_retries:
                detail = f"http_{error.code}:{error_detail[:200]}"
                return [(channel_id, "failed", detail) for channel_id in channel_ids]
            wait = _backoff_seconds(error_attempts)
            logger.warning(
                "Slack HTTP %s; retry %s/%s after %ss.",
                error.code,
                error_attempts,
                max_retries,
                wait,
            )
            time.sleep(wait)
            continue
        except urllib.error.URLError as error:
            error_attempts += 1
            if error_attempts >= max_retries:
                return [
                    (channel_id, "failed", f"network_error:{error.reason}")
                    for channel_id in channel_ids
                ]
            wait = _backoff_seconds(error_attempts)
            logger.warning(
                "Network error; retry %s/%s after %ss.",
                error_attempts,
                max_retries,
                wait,
            )
            time.sleep(wait)
            continue
        except json.JSONDecodeError as error:
            error_attempts += 1
            if error_attempts >= max_retries:
                detail = f"invalid_json:{error}"
                return [(channel_id, "failed", detail) for channel_id in channel_ids]
            wait = _backoff_seconds(error_attempts)
            logger.warning(
                "Slack returned invalid JSON; retry %s/%s after %ss.",
                error_attempts,
                max_retries,
                wait,
            )
            time.sleep(wait)
            continue

        if body.get("ok"):
            return _results_from_response(channel_ids=channel_ids, body=body)

        error_code = body.get("error", "unknown")
        if error_code == "ratelimited":
            wait = _retry_after_seconds(str(body.get("retry_after", "")), default=30)
            logger.warning("Batch rate-limited (body); waiting %ss.", wait)
            time.sleep(wait)
            continue
        if error_code == "action_already_in_progress":
            logger.info(
                "Previous bulk action still running; waiting %ss.",
                in_progress_wait_seconds,
            )
            time.sleep(in_progress_wait_seconds)
            continue
        if error_code in _FATAL_ERRORS:
            raise RuntimeError(
                f"Fatal Slack error '{error_code}'. Check that SLACK_TOKEN is an admin user "
                "token with admin.conversations:write, installed org-wide by an admin who "
                "holds the Channel Management role for public and private channels."
            )
        if error_code == "no_valid_channels":
            return [(channel_id, "skipped", "no_valid_channels") for channel_id in channel_ids]

        error_attempts += 1
        if error_attempts >= max_retries:
            detail = f"gave_up:{error_code}"
            return [(channel_id, "failed", detail) for channel_id in channel_ids]
        wait = _backoff_seconds(error_attempts)
        logger.warning(
            "Slack returned %r; retry %s/%s after %ss.",
            error_code,
            error_attempts,
            max_retries,
            wait,
        )
        time.sleep(wait)


def _append_results(
    results_file: pathlib.Path, results: list[tuple[str, str, str]]
) -> None:
    """Append per-channel results so later runs can skip completed channels."""
    results_file.parent.mkdir(parents=True, exist_ok=True)
    write_header = not results_file.exists()
    timestamp = datetime.datetime.now(datetime.UTC).isoformat()

    with results_file.open("a", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        if write_header:
            writer.writerow(["timestamp", "channel_id", "outcome", "detail"])
        for channel_id, outcome, detail in results:
            writer.writerow([timestamp, channel_id, outcome, detail])


def _count_outcomes(results: list[tuple[str, str, str]]) -> dict[str, int]:
    """Count result outcomes."""
    counts: dict[str, int] = {}
    for _, outcome, _ in results:
        counts[outcome] = counts.get(outcome, 0) + 1
    return counts


def _merge_counts(target: dict[str, int], source: dict[str, int]) -> None:
    """Add source counts into target counts in place."""
    for key, value in source.items():
        target[key] = target.get(key, 0) + value


def _log_progress(
    batch_index: int,
    batch_total: int,
    processed: int,
    total: int,
    counts: dict[str, int],
    started: float,
) -> None:
    """Emit a running summary with a rough ETA based on throughput so far."""
    elapsed = time.monotonic() - started
    rate = processed / elapsed if elapsed > 0 else 0
    remaining = (total - processed) / rate if rate > 0 else 0
    eta = datetime.timedelta(seconds=round(remaining))
    logger.info(
        "[batch %s/%s] %s/%s channels, counts=%s, ETA ~%s",
        batch_index,
        batch_total,
        processed,
        total,
        counts,
        eta,
    )


def _write_script_output(summary: dict[str, Any]) -> None:
    """Write Script Runner structured output when requested."""
    output_file = os.environ.get("SCRIPT_OUTPUT_FILE")
    if not output_file:
        return
    with open(output_file, "w", encoding="utf-8") as output:
        json.dump(summary, output, indent=2, sort_keys=True)


def main() -> None:
    logger.info("Starting script. DRY_RUN=%s", DRY_RUN)

    if not SLACK_TOKEN:
        logger.error("SLACK_TOKEN is not set.")
        sys.exit(1)

    try:
        channels_file = _env_path("CHANNELS_FILE", "channels_to_archive.txt")
        results_file = _env_path("RESULTS_FILE", "archive_results.csv")
        batch_size = _env_int("BATCH_SIZE", 100, minimum=1, maximum=100)
        requests_per_minute = _env_int("REQUESTS_PER_MINUTE", 20, minimum=1)
        max_retries = _env_int("MAX_RETRIES", 5, minimum=1)
        in_progress_wait_seconds = _env_int(
            "IN_PROGRESS_WAIT_SECONDS", 15, minimum=1
        )
        test_limit = _env_int("TEST_LIMIT", 0, minimum=0)
        channel_ids = _load_channel_ids(channels_file)
        if test_limit:
            channel_ids = channel_ids[:test_limit]
        done = _load_done(results_file)
    except (FileNotFoundError, ValueError):
        logger.exception("Failed to load configuration or channel state.")
        sys.exit(1)

    todo = [channel_id for channel_id in channel_ids if channel_id not in done]
    batches = _chunk(todo, batch_size)

    summary: dict[str, Any] = {
        "dry_run": DRY_RUN,
        "channels_file": str(channels_file),
        "results_file": str(results_file),
        "loaded_count": len(channel_ids),
        "already_done_count": len(channel_ids) - len(todo),
        "todo_count": len(todo),
        "batch_count": len(batches),
        "batch_size": batch_size,
    }

    logger.info(
        "%s IDs loaded, %s already done, %s to process in %s batch(es) of up to %s.",
        len(channel_ids),
        len(channel_ids) - len(todo),
        len(todo),
        len(batches),
        batch_size,
    )

    if DRY_RUN:
        for batch_index, batch in enumerate(batches, start=1):
            logger.info(
                "[DRY RUN] Batch %s/%s: %s channel(s).",
                batch_index,
                len(batches),
                len(batch),
            )
            for channel_id in batch:
                logger.info("[DRY RUN] Would archive channel %s", channel_id)
        summary["outcomes"] = {"would_archive": len(todo)}
        _write_script_output(summary)
        logger.info("Done.")
        return

    if not todo:
        summary["outcomes"] = {}
        _write_script_output(summary)
        logger.info("Done.")
        return

    delay = 60 / requests_per_minute
    counts: dict[str, int] = {}
    failed_channels: list[str] = []
    started = time.monotonic()
    processed = 0

    try:
        for batch_index, batch in enumerate(batches, start=1):
            results = _archive_batch(
                batch,
                SLACK_TOKEN,
                max_retries=max_retries,
                in_progress_wait_seconds=in_progress_wait_seconds,
            )
            _append_results(results_file, results)
            _merge_counts(counts, _count_outcomes(results))
            failed_channels.extend(
                channel_id for channel_id, outcome, _ in results if outcome == "failed"
            )

            processed += len(batch)
            _log_progress(
                batch_index=batch_index,
                batch_total=len(batches),
                processed=processed,
                total=len(todo),
                counts=counts,
                started=started,
            )
            if batch_index < len(batches):
                time.sleep(delay)
    except RuntimeError as error:
        summary["error"] = str(error)
        summary["outcomes"] = counts
        _write_script_output(summary)
        logger.exception("Unrecoverable Slack error.")
        sys.exit(1)

    summary["outcomes"] = counts
    summary["failed_channels"] = failed_channels
    _write_script_output(summary)
    logger.info("Done. %s", counts)

    if failed_channels:
        logger.error(
            "%s channel(s) failed after retries.",
            len(failed_channels),
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
