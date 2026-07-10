#!/usr/bin/env python3
"""
slack_ping.py — Script Runner smoke test.

Posts a "starting" message to a Slack channel, sleeps 20 seconds
(exercises the worker heartbeat), then posts a "done" message.

Required env var: SLACK_TOKEN (injected by the worker from the
script-runner-slack Platy secret).
"""

import os
import time

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

CHANNEL = "C0BEQP2RDED"
TOKEN = os.environ["SLACK_TOKEN"]


def post(client: WebClient, text: str) -> None:
    try:
        client.chat_postMessage(channel=CHANNEL, text=text)
        print(f"Posted: {text}")
    except SlackApiError as e:
        print(f"Slack error: {e.response['error']}")
        raise


def main() -> None:
    print("Sleeping 20 seconds...")
    time.sleep(20)
    client = WebClient(token=TOKEN)

    post(client, ":rocket: Script Runner ping — starting (will complete in 20 s)")

    print("Sleeping 20 seconds...")
    time.sleep(20)

    post(client, ":white_check_mark: Script Runner ping — done")
    print("Finished.")


if __name__ == "__main__":
    main()
