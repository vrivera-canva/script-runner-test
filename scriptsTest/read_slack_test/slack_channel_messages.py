#!/usr/bin/env python3
"""
slack_channel_messages.py — Fetch recent messages from a Slack channel.

Reads recent messages from the configured Slack channel, resolves message
authors through the Slack API, and produces a structured JSON result. The
script uses only read-only Slack endpoints and never posts or modifies data.

Required env vars (injected automatically by Script Runner):
  SLACK_TOKEN — Slack user token with channels:history and users:read access.

Optional env vars:
  TARGET_CHANNEL_ID    — Slack channel ID (default: C0B9RLFKMD0).
  TARGET_CHANNEL_NAME  — Display name used in output (default: fifa-world-cup-2026).
  MESSAGE_COUNT        — Number of recent messages to fetch (default: 3).
  SCRIPT_OUTPUT_FILE   — File to receive the structured JSON result.

Dry-run behaviour (SCRIPT_DRY_RUN=true, the default):
  Performs the same read-only Slack API calls and produces the same output.
  No Slack writes occur in either dry-run or real-run mode.

Local dry-run (no side effects):
  SCRIPT_DRY_RUN=true SLACK_TOKEN=xoxp-... python3 slack_channel_messages.py

Local real run:
  SCRIPT_DRY_RUN=false SLACK_TOKEN=xoxp-... python3 slack_channel_messages.py
"""

import json
import logging
import os
import random
import sys
import time
from typing import Any, Callable

import requests


SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]
logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [%(levelname)s] [{SCRIPT_NAME}] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DRY_RUN = os.environ.get("SCRIPT_DRY_RUN", "true").lower() == "true"
SLACK_TOKEN = os.environ.get("SLACK_TOKEN", "")
SLACK_API = "https://slack.com/api"
CHANNEL_ID = os.environ.get("TARGET_CHANNEL_ID", "C0B9RLFKMD0")
CHANNEL_NAME = os.environ.get("TARGET_CHANNEL_NAME", "fifa-world-cup-2026")
MESSAGE_COUNT_TEXT = os.environ.get("MESSAGE_COUNT", "3")
MAX_ATTEMPTS = 3
BASE_DELAY = 1.0


class SlackRequestError(RuntimeError):
    """A failed Slack HTTP or API response."""

    def __init__(
        self,
        *,
        method: str,
        status: int | None = None,
        body: str = "",
        error_code: str = "",
    ) -> None:
        self.method = method
        self.status = status
        self.body = body
        self.error_code = error_code
        detail = error_code or body[:500] or "request failed"
        super().__init__(f"Slack API {method} failed: status={status} error={detail}")


def retry_delay(
    attempt: int,
    *,
    retry_after: str | None = None,
    base_delay: float = BASE_DELAY,
) -> float:
    """Return Retry-After or exponential backoff with random jitter."""
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            logger.warning("Ignoring invalid Retry-After header: %r", retry_after)
    return base_delay * (2 ** (attempt - 1)) + random.uniform(0, base_delay)


def call_with_retry(
    fn: Callable[[], requests.Response],
    *,
    method: str,
    max_attempts: int = MAX_ATTEMPTS,
    base_delay: float = BASE_DELAY,
) -> dict[str, Any]:
    """Call a Slack endpoint with exponential backoff and random jitter."""
    for attempt in range(1, max_attempts + 1):
        try:
            response = fn()
            body = response.text

            if response.status_code == 429:
                error = SlackRequestError(
                    method=method,
                    status=response.status_code,
                    body=body,
                    error_code="rate_limited",
                )
                if attempt == max_attempts:
                    raise error
                delay = retry_delay(
                    attempt,
                    retry_after=response.headers.get("Retry-After"),
                    base_delay=base_delay,
                )
                logger.warning(
                    "Slack API %s rate limited attempt %d/%d: status=%d body=%s. "
                    "Retrying in %.1fs.",
                    method,
                    attempt,
                    max_attempts,
                    response.status_code,
                    body[:500],
                    delay,
                )
                time.sleep(delay)
                continue

            if not 200 <= response.status_code < 300:
                error = SlackRequestError(
                    method=method,
                    status=response.status_code,
                    body=body,
                )
                if attempt == max_attempts:
                    raise error
                delay = retry_delay(attempt, base_delay=base_delay)
                logger.warning(
                    "Slack API %s attempt %d/%d failed: status=%d body=%s. "
                    "Retrying in %.1fs.",
                    method,
                    attempt,
                    max_attempts,
                    response.status_code,
                    body[:500],
                    delay,
                )
                time.sleep(delay)
                continue

            try:
                data = response.json()
            except requests.exceptions.JSONDecodeError as error:
                if attempt == max_attempts:
                    raise SlackRequestError(
                        method=method,
                        status=response.status_code,
                        body=f"Invalid JSON response: {body[:500]}",
                    ) from error
                delay = retry_delay(attempt, base_delay=base_delay)
                logger.warning(
                    "Slack API %s returned invalid JSON on attempt %d/%d. "
                    "Retrying in %.1fs.",
                    method,
                    attempt,
                    max_attempts,
                    delay,
                )
                time.sleep(delay)
                continue

            if not data.get("ok"):
                error_code = str(data.get("error", "unknown_error"))
                error = SlackRequestError(
                    method=method,
                    status=response.status_code,
                    body=body,
                    error_code=error_code,
                )
                if attempt == max_attempts:
                    raise error
                delay = retry_delay(attempt, base_delay=base_delay)
                logger.warning(
                    "Slack API %s attempt %d/%d failed: error=%s. "
                    "Retrying in %.1fs.",
                    method,
                    attempt,
                    max_attempts,
                    error_code,
                    delay,
                )
                time.sleep(delay)
                continue

            return data
        except requests.RequestException as error:
            if attempt == max_attempts:
                raise
            delay = retry_delay(attempt, base_delay=base_delay)
            logger.warning(
                "Slack API %s attempt %d/%d failed: %s. Retrying in %.1fs.",
                method,
                attempt,
                max_attempts,
                error,
                delay,
            )
            time.sleep(delay)

    raise RuntimeError(f"Retry loop ended unexpectedly for Slack API {method}")


def slack_get(token: str, method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Perform a retried, authenticated Slack GET request."""
    url = f"{SLACK_API}/{method}"
    return call_with_retry(
        lambda: requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=10,
        ),
        method=method,
    )


def fetch_messages(
    token: str,
    channel_id: str,
    count: int,
) -> list[dict[str, Any]]:
    """Fetch recent messages from the configured channel."""
    data = slack_get(
        token,
        "conversations.history",
        {"channel": channel_id, "limit": count},
    )
    return data.get("messages", [])


def resolve_username(token: str, user_id: str) -> tuple[str, bool]:
    """Resolve a Slack user ID, returning whether the lookup succeeded."""
    if not user_id or user_id == "unknown":
        return user_id or "unknown", True

    try:
        data = slack_get(token, "users.info", {"user": user_id})
        user = data.get("user", {})
        username = user.get("real_name") or user.get("name") or user_id
        return str(username), True
    except SlackRequestError as error:
        logger.error(
            "Slack user lookup failed for user_id=%s: status=%s "
            "method=%s error=%s body=%s. Continuing.",
            user_id,
            error.status,
            error.method,
            error.error_code or "http_error",
            error.body[:500],
        )
        return user_id, False
    except requests.RequestException:
        logger.exception(
            "Slack user lookup transport failure for user_id=%s. Continuing.",
            user_id,
        )
        return user_id, False
    except Exception:
        logger.exception(
            "Unexpected Slack user lookup failure for user_id=%s. Continuing.",
            user_id,
        )
        return user_id, False


def write_structured_output(output: dict[str, Any]) -> None:
    """Write JSON to Script Runner's configured result file or the job log."""
    output_file = os.environ.get("SCRIPT_OUTPUT_FILE")
    if output_file:
        with open(output_file, "w", encoding="utf-8") as destination:
            json.dump(output, destination, indent=2)
            destination.write("\n")
        logger.info("Structured output written to %s.", output_file)
        return

    logger.info(
        "SCRIPT_OUTPUT_FILE is not set; structured output follows:\n%s",
        json.dumps(output, indent=2),
    )


def parse_message_count() -> int:
    """Validate MESSAGE_COUNT from the environment."""
    try:
        count = int(MESSAGE_COUNT_TEXT)
    except ValueError as error:
        raise ValueError(
            f"MESSAGE_COUNT must be an integer, got {MESSAGE_COUNT_TEXT!r}"
        ) from error
    if count < 1 or count > 100:
        raise ValueError("MESSAGE_COUNT must be between 1 and 100")
    return count


def main() -> int:
    """Fetch recent Slack messages and produce structured output."""
    logger.info("Starting script. DRY_RUN=%s", DRY_RUN)

    if not SLACK_TOKEN:
        logger.error("SLACK_TOKEN is not set.")
        return 1

    try:
        message_count = parse_message_count()
    except ValueError as error:
        logger.error("Invalid configuration: %s", error)
        return 1

    logger.info(
        "Fetching %d recent message(s) from #%s (%s).",
        message_count,
        CHANNEL_NAME,
        CHANNEL_ID,
    )

    try:
        raw_messages = fetch_messages(SLACK_TOKEN, CHANNEL_ID, message_count)
    except SlackRequestError as error:
        logger.error(
            "Failed to fetch Slack messages: status=%s method=%s error=%s body=%s",
            error.status,
            error.method,
            error.error_code or "http_error",
            error.body[:500],
        )
        return 1
    except requests.RequestException:
        logger.exception(
            "Slack transport failure fetching channel_id=%s.",
            CHANNEL_ID,
        )
        return 1
    except Exception:
        logger.exception(
            "Unexpected failure fetching messages from channel_id=%s.",
            CHANNEL_ID,
        )
        return 1

    messages_output: list[dict[str, Any]] = []
    failed_user_lookups = 0
    for message in raw_messages:
        user_id = str(message.get("user", "unknown"))
        username, lookup_succeeded = resolve_username(SLACK_TOKEN, user_id)
        if not lookup_succeeded:
            failed_user_lookups += 1

        messages_output.append(
            {
                "user": username,
                "user_id": user_id,
                "text": str(message.get("text", "")),
                "ts": str(message.get("ts", "")),
            }
        )
        logger.info(
            "Processed message ts=%s user_id=%s.",
            message.get("ts", "unknown"),
            user_id,
        )

    output = {
        "dry_run": DRY_RUN,
        "channel": CHANNEL_NAME,
        "channel_id": CHANNEL_ID,
        "requested_message_count": message_count,
        "messages_returned": len(messages_output),
        "failed_user_lookups": failed_user_lookups,
        "messages": messages_output,
    }

    try:
        write_structured_output(output)
    except OSError:
        logger.exception("Failed to write structured output.")
        return 1

    if failed_user_lookups:
        logger.warning(
            "Completed with %d failed user lookup(s). See above for details.",
            failed_user_lookups,
        )
    logger.info(
        "Completed successfully with %d message(s).",
        len(messages_output),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
