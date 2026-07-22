#!/usr/bin/env python3
"""
demo.py — Refresh the Confluence space administrator catalogue.

Fetches global Confluence spaces, looks up each space's active administrators,
builds XHTML for the catalogue, and updates the configured Confluence page.
A successful run reports processed/failed space counts and the page update result.

Required env vars (injected automatically by Script Runner):
  USERNAME  — Atlassian account email used for Confluence Basic authentication.
  API_TOKEN — Atlassian API token paired with USERNAME.

Optional env vars:
  ATLASSIAN_DOMAIN    — Confluence Cloud domain (default: canvadev.atlassian.net).
  CONFLUENCE_PAGE_ID  — Catalogue page ID (default: 3484322217).
  SCRIPT_OUTPUT_FILE  — File to receive a JSON execution summary.

Dry-run behaviour (SCRIPT_DRY_RUN=true, the default):
  Fetches Confluence spaces and administrators and builds the catalogue, but
  only logs the intended page update; it does not call the Confluence PUT API.

Local dry-run (no side effects):
  SCRIPT_DRY_RUN=true USERNAME=user@canva.com API_TOKEN=... python3 demo.py

Local real run:
  SCRIPT_DRY_RUN=false USERNAME=user@canva.com API_TOKEN=... python3 demo.py
"""

import asyncio
from datetime import datetime, timezone
from html import escape
import json
import logging
import os
import random
import sys
from typing import Any

import aiohttp
from lxml import html as lxml_html


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DRY_RUN = os.environ.get("SCRIPT_DRY_RUN", "true").lower() == "true"
USERNAME = os.environ.get("USERNAME", "")
API_TOKEN = os.environ.get("API_TOKEN", "")
ATLASSIAN_DOMAIN = os.environ.get("ATLASSIAN_DOMAIN", "canvadev.atlassian.net")
CONFLUENCE_PAGE_ID = os.environ.get("CONFLUENCE_PAGE_ID", "3484322217")
PAGE_TITLE = "Confluence Space Catalogue"

HEADERS = {"Accept": "application/json"}
MAX_ATTEMPTS = 3
BASE_DELAY = 1.0
MAX_BACKOFF = 60.0
USER_CACHE: dict[str, dict[str, Any]] = {}

CONFLUENCE_EXCLUSION_LIST = {
    "Microsoft Teams for Confluence Cloud",
    "Chat Notifications",
    "Refined for Atlassian Cloud",
    "Aura",
    "Refined Spaces for Confluence Cloud",
    "Scio Search Crawler for Confluence",
    "Jira Ops Confluence integration",
    "Macro Toolbox (HTML, Tabs, Expand)",
    "Confluence Analytics (System)",
}


class APIRequestError(RuntimeError):
    """A non-success response from the Confluence API."""

    def __init__(self, status: int, url: str, body: str) -> None:
        self.status = status
        self.url = url
        self.body = body
        super().__init__(f"HTTP {status} from {url}: {body[:500]}")


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
    return min(MAX_BACKOFF, base_delay * (2 ** (attempt - 1))) + random.uniform(
        0, base_delay
    )


async def request_json(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    *,
    max_attempts: int = MAX_ATTEMPTS,
    **kwargs: Any,
) -> dict[str, Any]:
    """Call a Confluence API endpoint with bounded backoff and jitter."""
    for attempt in range(1, max_attempts + 1):
        try:
            async with session.request(method, url, **kwargs) as response:
                body = await response.text()
                if 200 <= response.status < 300:
                    if not body:
                        return {}
                    try:
                        return json.loads(body)
                    except json.JSONDecodeError as exc:
                        raise APIRequestError(
                            response.status,
                            str(response.url),
                            f"Invalid JSON response: {body[:500]}",
                        ) from exc

                error = APIRequestError(response.status, str(response.url), body)
                if attempt == max_attempts:
                    raise error

                delay = retry_delay(
                    attempt,
                    retry_after=response.headers.get("Retry-After")
                    if response.status == 429
                    else None,
                )
                logger.warning(
                    "Attempt %d/%d failed: status=%s url=%s body=%s. "
                    "Retrying in %.1fs.",
                    attempt,
                    max_attempts,
                    response.status,
                    response.url,
                    body[:500],
                    delay,
                )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == max_attempts:
                raise
            delay = retry_delay(attempt)
            logger.warning(
                "Attempt %d/%d failed for %s %s: %s. Retrying in %.1fs.",
                attempt,
                max_attempts,
                method,
                url,
                exc,
                delay,
            )

        await asyncio.sleep(delay)

    raise RuntimeError(f"Retry loop ended unexpectedly for {method} {url}")


async def get_confluence_spaces(
    session: aiohttp.ClientSession,
) -> list[dict[str, Any]]:
    """Fetch all current global Confluence spaces."""
    next_url: str | None = (
        f"https://{ATLASSIAN_DOMAIN}/wiki/api/v2/spaces"
        "?type=global&status=current&sort=name"
    )
    results: list[dict[str, Any]] = []

    while next_url:
        data = await request_json(session, "GET", next_url)
        results.extend(data.get("results", []))
        next_path = data.get("_links", {}).get("next")
        next_url = f"https://{ATLASSIAN_DOMAIN}{next_path}" if next_path else None

    return results


async def get_space_permissions(
    session: aiohttp.ClientSession, space_id: str
) -> list[dict[str, Any]]:
    """Fetch every permission entry for a Confluence space."""
    next_url: str | None = (
        f"https://{ATLASSIAN_DOMAIN}/wiki/api/v2/spaces/{space_id}/permissions"
    )
    results: list[dict[str, Any]] = []

    while next_url:
        data = await request_json(session, "GET", next_url)
        results.extend(data.get("results", []))
        next_path = data.get("_links", {}).get("next")
        next_url = f"https://{ATLASSIAN_DOMAIN}{next_path}" if next_path else None

    return results


async def get_user_details(
    session: aiohttp.ClientSession, account_id: str
) -> dict[str, Any]:
    """Fetch and cache a Confluence user's details."""
    if account_id not in USER_CACHE:
        url = (
            f"https://{ATLASSIAN_DOMAIN}/rest/api/2/user"
            f"?accountId={account_id}"
        )
        USER_CACHE[account_id] = await request_json(session, "GET", url)
    return USER_CACHE[account_id]


async def handle_space(
    session: aiohttp.ClientSession, space: dict[str, Any]
) -> str:
    """Build one XHTML table row for a Confluence space."""
    space_key = str(space["key"])
    space_id = str(space["id"])
    space_name = str(space["name"])

    permissions = await get_space_permissions(session, space_id)
    admin_ids = [
        str(permission["principal"]["id"])
        for permission in permissions
        if permission.get("operation", {}).get("key") == "administer"
        and permission.get("principal", {}).get("type") == "user"
        and permission.get("principal", {}).get("id")
    ]

    admin_names: list[str] = []
    for account_id in admin_ids:
        user = await get_user_details(session, account_id)
        display_name = user.get("displayName")
        if (
            display_name
            and display_name not in CONFLUENCE_EXCLUSION_LIST
            and user.get("active", False)
        ):
            admin_names.append(str(display_name))

    admins = ", ".join(admin_names) if admin_names else "No admin users found."
    space_link = f"https://{ATLASSIAN_DOMAIN}/wiki/spaces/{space_key}"
    return (
        f"<tr><td>{escape(space_key)}</td>"
        f"<td>{escape(space_name)}</td>"
        f'<td><a href="{escape(space_link)}">{escape(space_link)}</a></td>'
        f"<td>{escape(admins)}</td></tr>"
    )


def create_confluence_html_content(space_rows: list[str]) -> str:
    """Create the complete Confluence catalogue HTML."""
    introduction = (
        '<p style="text-align: center;">This is a catalogue of all Confluence '
        "spaces and their respective administrators. For access concerns, please "
        "reach out to the administrators in this list. For licensing, please raise "
        'a ticket at <a href="http://canv.am/it-request">canv.am/it-request</a></p>'
        '<p style="text-align: center;"><em><strong>Please do not edit this page, '
        "it is automatically refreshed with information from Atlassian on a daily "
        "basis.</strong></em></p>"
    )
    table_start = (
        '<table data-table-width="1800" data-layout="default" '
        'ac:local-id="5351e901-f65e-45bb-aad9-d3025ebdd539">'
        '<colgroup><col style="width: 49.0px;" />'
        '<col style="width: 200.0px;" />'
        '<col style="width: 250.0px;" />'
        '<col style="width: 317.0px;" /></colgroup><tbody>'
        "<tr><th>Space Key</th><th>Space Name</th><th>Space Link</th>"
        "<th>Administrators</th></tr>"
    )
    return introduction + table_start + "".join(space_rows) + "</tbody></table>"


def html_to_xhtml(html_content: str) -> str:
    """Convert generated HTML to Confluence storage XHTML."""
    document = lxml_html.fromstring(html_content)
    return lxml_html.tostring(
        document,
        pretty_print=True,
        method="xml",
        encoding="unicode",
    )


async def update_confluence_page(
    session: aiohttp.ClientSession,
    page_id: str,
    xhtml_content: str,
    title: str,
) -> bool:
    """Update the catalogue page, or log the intended write in dry-run mode."""
    if DRY_RUN:
        logger.info(
            "[DRY RUN] Would update Confluence page %s (%s) with %d XHTML characters.",
            page_id,
            title,
            len(xhtml_content),
        )
        return False

    version_url = (
        f"https://{ATLASSIAN_DOMAIN}/wiki/rest/api/content/{page_id}"
        "?expand=version"
    )
    version_data = await request_json(session, "GET", version_url)
    current_version = int(version_data["version"]["number"])
    logger.info("Current page version: %d", current_version)

    update_url = f"https://{ATLASSIAN_DOMAIN}/wiki/rest/api/content/{page_id}"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    payload = {
        "id": page_id,
        "type": "page",
        "title": title,
        "body": {
            "storage": {
                "value": xhtml_content,
                "representation": "storage",
            }
        },
        "version": {
            "number": current_version + 1,
            "minorEdit": False,
            "message": f"Updated by Script Runner on {now}",
        },
    }
    await request_json(
        session,
        "PUT",
        update_url,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        json=payload,
    )
    logger.info("Updated Confluence page %s to version %d.", page_id, current_version + 1)
    return True


def log_api_error(context: str, error: APIRequestError) -> None:
    """Log a Confluence API failure with actionable response context."""
    logger.error(
        "%s: status=%s url=%s body=%s",
        context,
        error.status,
        error.url,
        error.body[:500],
    )


def write_structured_output(summary: dict[str, Any]) -> None:
    """Write the Script Runner JSON result when an output path is configured."""
    output_file = os.environ.get("SCRIPT_OUTPUT_FILE")
    if not output_file:
        return
    with open(output_file, "w", encoding="utf-8") as output:
        json.dump(summary, output, indent=2)
        output.write("\n")
    logger.info("Wrote structured output to %s.", output_file)


async def main() -> int:
    """Run the catalogue refresh."""
    logger.info("Starting script. DRY_RUN=%s", DRY_RUN)

    missing = [name for name, value in (("USERNAME", USERNAME), ("API_TOKEN", API_TOKEN)) if not value]
    if missing:
        logger.error("Missing required environment variable(s): %s", ", ".join(missing))
        return 1

    timeout = aiohttp.ClientTimeout(total=60)
    auth = aiohttp.BasicAuth(USERNAME, API_TOKEN)
    summary: dict[str, Any] = {
        "dry_run": DRY_RUN,
        "spaces_found": 0,
        "spaces_processed": 0,
        "spaces_failed": 0,
        "page_updated": False,
    }

    try:
        async with aiohttp.ClientSession(
            auth=auth,
            headers=HEADERS,
            timeout=timeout,
        ) as session:
            logger.info("Fetching Confluence spaces.")
            spaces = await get_confluence_spaces(session)
            summary["spaces_found"] = len(spaces)
            if not spaces:
                logger.warning("No current global Confluence spaces were returned.")
                write_structured_output(summary)
                return 0

            results = await asyncio.gather(
                *(handle_space(session, space) for space in spaces),
                return_exceptions=True,
            )
            rows: list[str] = []
            for space, result in zip(spaces, results):
                if isinstance(result, Exception):
                    summary["spaces_failed"] += 1
                    space_id = space.get("id", "unknown")
                    space_key = space.get("key", "unknown")
                    if isinstance(result, APIRequestError):
                        log_api_error(
                            f"Failed to process space key={space_key} id={space_id}",
                            result,
                        )
                    else:
                        logger.error(
                            "Failed to process space key=%s id=%s. Continuing.",
                            space_key,
                            space_id,
                            exc_info=(type(result), result, result.__traceback__),
                        )
                    continue
                rows.append(result)
                summary["spaces_processed"] += 1

            if summary["spaces_failed"]:
                logger.warning(
                    "Processed %d space(s) with %d failure(s).",
                    summary["spaces_processed"],
                    summary["spaces_failed"],
                )
            if not rows:
                logger.error("No catalogue rows were generated; page update skipped.")
                write_structured_output(summary)
                return 1

            xhtml_content = html_to_xhtml(create_confluence_html_content(rows))
            summary["page_updated"] = await update_confluence_page(
                session,
                CONFLUENCE_PAGE_ID,
                xhtml_content,
                PAGE_TITLE,
            )
    except APIRequestError as error:
        log_api_error("Confluence API call failed", error)
        write_structured_output(summary)
        return 1
    except Exception:
        logger.exception("Unexpected failure refreshing the Confluence catalogue.")
        write_structured_output(summary)
        return 1

    write_structured_output(summary)
    logger.info("Completed catalogue refresh: %s", summary)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
