#!/usr/bin/env python3
"""
Fetches the last 3 messages from #fifa-world-cup-2026.

Required env vars:
  SLACK_TOKEN        - Slack user token (xoxp-...)

Optional env vars:
  SCRIPT_OUTPUT_FILE - path to write structured JSON output (set by Script Runner worker)
  TARGET_CHANNEL_ID  - Slack channel ID (default: C0B9RLFKMD0 = #fifa-world-cup-2026)
"""

import json
import os
import sys
import time

import requests

SLACK_API = "https://slack.com/api"
CHANNEL_NAME = "fifa-world-cup-2026"
CHANNEL_ID = os.environ.get("TARGET_CHANNEL_ID", "C0B9RLFKMD0")


def slack_get(token: str, method: str, params: dict, max_retries: int = 3) -> dict:
    for attempt in range(max_retries):
        response = requests.get(
            f"{SLACK_API}/{method}",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=10,
        )
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limited on {method}, retrying in {retry_after}s...")
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            raise RuntimeError(f"Slack API error ({method}): {data.get('error')}")
        return data
    raise RuntimeError(f"Slack API {method} rate limited after {max_retries} retries")


def fetch_messages(token: str, channel_id: str, count: int = 3) -> list[dict]:
    data = slack_get(token, "conversations.history", {"channel": channel_id, "limit": count})
    return data.get("messages", [])


def resolve_username(token: str, user_id: str) -> str:
    try:
        data = slack_get(token, "users.info", {"user": user_id})
        return data["user"].get("real_name") or data["user"].get("name", user_id)
    except Exception:
        return user_id


def main():
    token = os.environ.get("SLACK_TOKEN")
    if not token:
        print("ERROR: SLACK_TOKEN env var is not set", file=sys.stderr)
        sys.exit(1)

    print(f"Fetching last 3 messages from #{CHANNEL_NAME} ({CHANNEL_ID})...")
    raw_messages = fetch_messages(token, CHANNEL_ID, count=3)

    messages_output = []
    for msg in raw_messages:
        user_id = msg.get("user", "unknown")
        messages_output.append(
            {
                "user": resolve_username(token, user_id),
                "text": msg.get("text", ""),
                "ts": msg.get("ts", ""),
            }
        )
        print(f"  [{user_id}] {msg.get('text', '')[:80]}")

    output = {
        "channel": CHANNEL_NAME,
        "channel_id": CHANNEL_ID,
        "messages": messages_output,
    }

    output_file = os.environ.get("SCRIPT_OUTPUT_FILE")
    if output_file:
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)
        print(f"Structured output written to {output_file}")
    else:
        print("\n[SCRIPT_OUTPUT_FILE not set — printing output to stdout]")
        print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
