import asyncio
import aiohttp
from lxml import html
import datetime
import random

import os


USERNAME = os.getenv('username')
API_TOKEN = os.getenv('api_token')

# Common configurations and constants
ATLASSIAN_DOMAIN = 'canvadev.atlassian.net'
CONFLUENCE_PAGE_ID = '3484322217'

HEADERS = {'Accept': 'application/json'}

# Longest we will ever wait between retries (seconds). We back off up to this,
# then keep trying at this interval. We never give up on a rate limit.
MAX_BACKOFF = 60

# Cache user lookups so the same admin isn't re-fetched for every space they administer
USER_CACHE = {}


def rate_limit_wait(response, attempt):
    """Work out how long to wait after a 429. Honour Retry-After if present,
    otherwise back off exponentially (capped), with jitter so simultaneous
    retries don't all fire at the same instant."""
    retry_after = response.headers.get('Retry-After')
    if retry_after is not None:
        wait = int(retry_after)
    else:
        wait = min(MAX_BACKOFF, 2 ** attempt)
    return wait + random.uniform(0, 1)


# Define the exclusion list for Confluence
confluence_exclusion_list = [
    "Microsoft Teams for Confluence Cloud",
    "Chat Notifications",
    "Refined for Atlassian Cloud",
    "Aura",
    "Refined Spaces for Confluence Cloud",
    "Scio Search Crawler for Confluence",
    "Jira Ops Confluence integration",
    "Macro Toolbox (HTML, Tabs, Expand)",
    "Confluence Analytics (System)"
]


# Common fetch function (GET). Retries indefinitely while rate limited so a 429
# can never end the run early.
async def fetch(session, url):
    attempt = 0
    while True:
        async with session.get(url) as response:
            if response.status != 429:
                if response.status != 200:
                    print(f"Error fetching data: {response.status}")
                response.raise_for_status()
                return await response.json()
            attempt += 1
            wait = rate_limit_wait(response, attempt)
        print(f"Rate limited on {url}. Waiting {wait:.1f}s before retry {attempt}...")
        await asyncio.sleep(wait)


# Common safe request function. Retries indefinitely while rate limited; returns
# None for other client errors.
async def safe_request(session, url, method='get', **kwargs):
    attempt = 0
    while True:
        try:
            request = session.get(url, **kwargs) if method == 'get' else session.put(url, **kwargs)
            async with request as response:
                if response.status != 429:
                    response.raise_for_status()
                    return await response.json()
                attempt += 1
                wait = rate_limit_wait(response, attempt)
        except aiohttp.ClientError as e:
            print(f"HTTP error occurred: {e}")
            return None
        print(f"Rate limited on {url}. Waiting {wait:.1f}s before retry {attempt}...")
        await asyncio.sleep(wait)


# Get Confluence page version
async def get_confluence_page_version(session, page_id):
    url = f"https://{ATLASSIAN_DOMAIN}/wiki/rest/api/content/{page_id}?expand=version"
    data = await safe_request(session, url)
    if data:
        print(f"Current page version: {data['version']['number']}")
        return data['version']['number']
    return -1  # Return -1 as a flag value to indicate error


# Update Confluence page. The PUT also retries indefinitely while rate limited.
async def update_confluence_page(session, page_id, xhtml_content, new_title):
    current_version = await get_confluence_page_version(session, page_id)
    if current_version == -1:
        print("Failed to get current version. Skipping update.")
        return

    now = datetime.datetime.now()
    formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")

    url = f"https://{ATLASSIAN_DOMAIN}/wiki/rest/api/content/{page_id}"
    headers = {'Content-Type': 'application/json'}

    payload = {
        "id": page_id,
        "type": "page",
        "title": new_title,
        "body": {
            "storage": {
                "value": xhtml_content,
                "representation": "storage"
            }
        },
        "version": {
            "number": current_version + 1,
            "minorEdit": False,
            "message": f"Updated by the API Script on {formatted_now}"
        }
    }

    attempt = 0
    while True:
        async with session.put(url, headers=headers, json=payload) as response:
            if response.status == 200:
                print("Page updated successfully.")
                return
            if response.status != 429:
                body = await response.text()
                print(f"Failed to update page {page_id}. Status {response.status}: {body}")
                return
            attempt += 1
            wait = rate_limit_wait(response, attempt)
        print(f"Rate limited updating page {page_id}. Waiting {wait:.1f}s before retry {attempt}...")
        await asyncio.sleep(wait)


# Convert HTML to XHTML
def html_to_xhtml(html_content):
    try:
        document = html.fromstring(html_content)
        xhtml_content = html.tostring(document, pretty_print=True, method="xml", encoding='unicode')
        return xhtml_content
    except Exception as e:
        print(f"Error converting HTML to XHTML: {e}")
        return None


# Confluence specific functions
async def get_confluence_spaces(session):
    base_url = f'https://{ATLASSIAN_DOMAIN}/wiki/api/v2/spaces?type=global&status=current&sort=name'
    all_results = []
    next_url = base_url

    while next_url:
        data = await fetch(session, next_url)
        all_results.extend(data.get("results", []))
        next_url = data.get("_links", {}).get("next")
        if next_url:
            next_url = f"https://{ATLASSIAN_DOMAIN}{next_url}"
        else:
            break
    return all_results


async def get_space_permissions(session, space_id):
    base_url = f'https://{ATLASSIAN_DOMAIN}/wiki/api/v2/spaces/{space_id}/permissions'
    all_results = []
    next_url = base_url

    while next_url:
        data = await fetch(session, next_url)
        all_results.extend(data.get("results", []))
        next_url = data.get("_links", {}).get("next")
        if next_url:
            next_url = f"https://{ATLASSIAN_DOMAIN}{next_url}"
        else:
            break
    return all_results


async def get_user_details(session, account_id):
    if account_id in USER_CACHE:
        return USER_CACHE[account_id]
    url = f'https://{ATLASSIAN_DOMAIN}/rest/api/2/user?accountId={account_id}'
    data = await fetch(session, url)
    USER_CACHE[account_id] = data
    return data


async def handle_space(session, space):
    space_key = space['key']
    space_id = space['id']
    space_name = space['name']

    permissions = await get_space_permissions(session, space_id)
    admin_users = [perm['principal']['id'] for perm in permissions if 'operation' in perm and 'principal' in perm and perm['operation'].get('key') == 'administer' and perm['principal'].get('type') == 'user']

    admin_names = []
    if admin_users:
        for admin_user in admin_users:
            user_details = await get_user_details(session, admin_user)
            if user_details:
                display_name = user_details.get('displayName')
                is_active = user_details.get('active', False)
                if display_name not in confluence_exclusion_list and is_active:
                    admin_names.append(display_name)

    admin_names_str = ', '.join(admin_names) if admin_names else 'No admin users found.'

    space_link = f"https://{ATLASSIAN_DOMAIN}/wiki/spaces/{space_key}"
    return f'<tr><td>{space_key}</td><td>{space_name}</td><td><a href="{space_link}">{space_link}</a></td><td>{admin_names_str}</td></tr>'


async def create_confluence_html_content(space_rows):
    html_content = """
    <p style="text-align: center;">This is a catalogue of all Confluence spaces and their respective administrators. For access concerns, please reach out to the administrators in this list. For licensing, please raise a ticket at <a href="http://canv.am/it-request">canv.am/it-request</a></p>
    <p style="text-align: center;"><em><strong>Please do not edit this page, it is automatically refreshed with information from Atlassian on a daily basis.</strong></em></p>
    <table data-table-width="1800" data-layout="default" ac:local-id="5351e901-f65e-45bb-aad9-d3025ebdd539">
    <colgroup><col style="width: 49.0px;" /><col style="width: 200.0px;" /><col style="width: 250.0px;" /><col style="width: 317.0px;" /></colgroup>
    <tbody>
    <tr><th>Space Key</th><th>Space Name</th><th>Space Link</th><th>Administrators</th></tr>
    """
    html_content += "".join(space_rows)
    html_content += "</tbody></table>"
    return html_content


# Main function
async def main():
    auth = aiohttp.BasicAuth(USERNAME, API_TOKEN)
    async with aiohttp.ClientSession(auth=auth, headers=HEADERS) as session:

        # Fetch Confluence data
        print("Starting Confluence data fetch...")
        confluence_spaces = await get_confluence_spaces(session)
        if not confluence_spaces:
            print("No Confluence spaces retrieved; exiting.")
            return

        confluence_tasks = [handle_space(session, space) for space in confluence_spaces]
        confluence_space_rows = await asyncio.gather(*confluence_tasks)
        confluence_html_content = await create_confluence_html_content(confluence_space_rows)
        confluence_xhtml_content = html_to_xhtml(confluence_html_content)

        if confluence_xhtml_content:
            await update_confluence_page(session, CONFLUENCE_PAGE_ID, confluence_xhtml_content, "Confluence Space Catalogue")
        else:
            print("Error converting Confluence HTML to XHTML.")


# Run the script
if __name__ == "__main__":
    asyncio.run(main())
