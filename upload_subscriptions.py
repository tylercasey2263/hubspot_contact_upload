import argparse
import csv
import os
import time
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

REQUIRED_CSV_COLUMNS = {
    "user_email", "user_first_name", "user_last_name",
    "player_first_name", "player_last_name", "player_id",
    "subscription_id", "subscription_purchase_date",
    "program_id", "program_name", "package_id", "package_name",
}

load_dotenv()

HUBSPOT_API_KEY = os.getenv("HUBSPOT_ACCESS_TOKEN")
BASE_URL = "https://api.hubapi.com"
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json",
}
DEFAULT_CSV = "SubscriptionsExport (2).csv"
DEFAULT_LIST_NAME = "ID_20260420"
BATCH_SIZE = 100
RATE_LIMIT_PAUSE = 10


def check_csv(filepath):
    """Detect encoding and validate required columns. Returns (encoding, fieldnames) or raises SystemExit."""
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            with open(filepath, newline="", encoding=encoding) as f:
                reader = csv.DictReader(f)
                raw_fields = reader.fieldnames or []
                # Strip whitespace from headers in case of copy/paste artifacts
                fields = [h.strip() for h in raw_fields]
                if fields != raw_fields:
                    print(f"  Whitespace stripped from {sum(h != h.strip() for h in raw_fields)} column header(s)")
            detected = encoding
            break
        except (UnicodeDecodeError, LookupError):
            continue
    else:
        print("ERROR: Could not decode CSV — tried utf-8, utf-8-bom, and cp1252.")
        raise SystemExit(1)

    print(f"  Encoding: {detected}")

    missing = REQUIRED_CSV_COLUMNS - set(fields)
    if missing:
        print("ERROR: CSV is missing required column(s):")
        for col in sorted(missing):
            print(f"    - {col}")
        print("  Present columns:", fields)
        raise SystemExit(1)

    extra = set(fields) - REQUIRED_CSV_COLUMNS - {"user_id", "birth_date", "street", "city", "state", "zip"}
    if extra:
        print(f"  Note: {len(extra)} unrecognised column(s) will be ignored: {sorted(extra)}")

    return detected, fields


def to_hubspot_date(date_str):
    """Convert M/D/YYYY string to HubSpot date (midnight UTC, ms timestamp)."""
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str.strip(), "%m/%d/%Y").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return ""


def read_contacts_from_csv(filepath, encoding="utf-8-sig"):
    """Read subscription CSV and return (contacts dict, multi_player_warnings list).

    Contacts are keyed by email. When a parent appears on multiple rows (multiple
    players), their player fields are overwritten by the last row — a warning is
    returned for each such parent so the operator is aware.
    """
    contacts = {}
    email_counts = {}

    with open(filepath, newline="", encoding=encoding) as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = row.get("user_email", "").strip().lower()
            if not email:
                continue

            email_counts[email] = email_counts.get(email, 0) + 1

            contacts[email] = {
                "email": email,
                "firstname": row.get("user_first_name", "").strip(),
                "lastname": row.get("user_last_name", "").strip(),
                "user_id": row.get("user_id", "").strip(),
                "address": row.get("street", "").strip(),
                "city": row.get("city", "").strip(),
                "state": row.get("state", "").strip(),
                "zip": row.get("zip", "").strip(),
                "player_id": row.get("player_id", "").strip(),
                "player_first_name": row.get("player_first_name", "").strip(),
                "player_last_name": row.get("player_last_name", "").strip(),
                "player_date_of_birth": to_hubspot_date(row.get("birth_date", "")),
                "subscription_id": row.get("subscription_id", "").strip(),
                "subscription_purchase_date": to_hubspot_date(row.get("subscription_purchase_date", "")),
                "program_id": row.get("program_id", "").strip(),
                "program_name": row.get("program_name", "").strip(),
                "package_id": row.get("package_id", "").strip(),
                "package_name": row.get("package_name", "").strip(),
                "hs_marketable_status": "true",
            }

    multi_player_warnings = [
        f"  {email} has {count} players — only the last player's data will be stored"
        for email, count in email_counts.items()
        if count > 1
    ]

    return contacts, multi_player_warnings


def fetch_valid_properties():
    """Return the set of all valid contact property names from HubSpot."""
    resp = requests.get(f"{BASE_URL}/crm/v3/properties/contacts", headers=HEADERS)
    if resp.status_code != 200:
        print(f"  WARNING: Could not fetch HubSpot properties ({resp.status_code}) — skipping validation")
        return None
    return {p["name"] for p in resp.json().get("results", [])}


def validate_and_filter(contacts_list, valid_props):
    """Check every property key we plan to send against the HubSpot property list.

    Removes invalid keys from each contact dict in-place and returns a summary
    of any dropped fields.
    """
    # Collect all unique property names across all contacts (excluding 'email'
    # which is used as the upsert key, not sent as a regular property)
    all_keys = {k for c in contacts_list for k in c if k != "email"}
    invalid = sorted(all_keys - valid_props)
    valid = sorted(all_keys & valid_props)

    if invalid:
        print(f"  WARNING: {len(invalid)} field(s) not found in HubSpot — they will be skipped:")
        for k in invalid:
            print(f"    - {k}")
        # Strip invalid keys from every contact
        for c in contacts_list:
            for k in invalid:
                c.pop(k, None)
    else:
        print(f"  All {len(valid)} field(s) validated OK")

    return valid, invalid


def upsert_contacts_batch(contacts_list, dryrun=False):
    """Upsert contacts via batch API using email as the unique key.

    Returns (contact_ids, failed_contacts).
    """
    contact_ids = []
    failed_contacts = []

    for i in range(0, len(contacts_list), BATCH_SIZE):
        batch = contacts_list[i : i + BATCH_SIZE]
        payload = {
            "inputs": [
                {
                    "id": c["email"],
                    "idProperty": "email",
                    "properties": {k: v for k, v in c.items() if v != ""},
                }
                for c in batch
            ]
        }

        if dryrun:
            print(f"  [DRY RUN] Batch {i // BATCH_SIZE + 1}: would upsert {len(batch)} contacts")
            for c in batch:
                print(f"    {c['email']} — {c.get('firstname')} {c.get('lastname')}")
            continue

        resp = _post_with_retry(f"{BASE_URL}/crm/v3/objects/contacts/batch/upsert", payload)

        if resp.status_code in (200, 201):
            results = resp.json().get("results", [])
            ids = [r["id"] for r in results if "id" in r]
            contact_ids.extend(ids)
            print(f"  Batch {i // BATCH_SIZE + 1}: upserted {len(ids)} contacts")
        else:
            failed_contacts.extend(batch)
            print(f"  Batch {i // BATCH_SIZE + 1} FAILED: {resp.status_code}")
            print(f"    {resp.text[:500]}")

        if i + BATCH_SIZE < len(contacts_list):
            time.sleep(1)

    return contact_ids, failed_contacts


def write_error_log(failed_contacts):
    """Write failed contacts to a timestamped CSV for review/retry."""
    if not failed_contacts:
        return
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"upload_errors_{timestamp}.csv"
    fieldnames = list(failed_contacts[0].keys())
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(failed_contacts)
    print(f"  Error log written to: {filename}")


def get_or_create_list(list_name, dryrun=False):
    """Return the listId for list_name, creating it (v1 static) if it doesn't exist."""
    existing_id = find_list_by_name(list_name)
    if existing_id:
        print(f"  Found existing list '{list_name}' with ID {existing_id}")
        return existing_id

    if dryrun:
        print(f"  [DRY RUN] Would create new list '{list_name}'")
        return None

    payload = {"name": list_name, "dynamic": False}
    resp = requests.post(f"{BASE_URL}/contacts/v1/lists", headers=HEADERS, json=payload)
    if resp.status_code == 200:
        list_id = resp.json().get("listId")
        print(f"  Created new list '{list_name}' with ID {list_id}")
        return list_id

    print(f"  Failed to create list: {resp.status_code} — {resp.text[:500]}")
    return None


def find_list_by_name(list_name):
    """Search all lists via v1 API and return listId for a matching name."""
    offset = 0
    while True:
        resp = requests.get(
            f"{BASE_URL}/contacts/v1/lists",
            headers=HEADERS,
            params={"count": 250, "offset": offset},
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        for lst in data.get("lists", []):
            if lst.get("name") == list_name:
                return lst.get("listId")
        if not data.get("has-more"):
            return None
        offset += 250


def add_contacts_to_list(list_id, contact_ids):
    """Add HubSpot contact record IDs (vids) to a static list via v1 API."""
    added = 0
    for i in range(0, len(contact_ids), BATCH_SIZE):
        batch = contact_ids[i : i + BATCH_SIZE]
        resp = requests.post(
            f"{BASE_URL}/contacts/v1/lists/{list_id}/add",
            headers=HEADERS,
            json={"vids": batch},
        )
        if resp.status_code in (200, 204):
            added += len(batch)
            print(f"  Added {len(batch)} contacts to list")
        elif resp.status_code == 429:
            print("  Rate limited — pausing 10s and retrying...")
            time.sleep(RATE_LIMIT_PAUSE)
            resp = requests.post(
                f"{BASE_URL}/contacts/v1/lists/{list_id}/add",
                headers=HEADERS,
                json={"vids": batch},
            )
            if resp.status_code in (200, 204):
                added += len(batch)
            else:
                print(f"  FAILED to add batch after retry: {resp.status_code} — {resp.text[:300]}")
        else:
            print(f"  FAILED to add batch: {resp.status_code} — {resp.text[:300]}")

        if i + BATCH_SIZE < len(contact_ids):
            time.sleep(1)

    return added


def _post_with_retry(url, payload):
    resp = requests.post(url, headers=HEADERS, json=payload)
    if resp.status_code == 429:
        print("  Rate limited — pausing 10s and retrying...")
        time.sleep(RATE_LIMIT_PAUSE)
        resp = requests.post(url, headers=HEADERS, json=payload)
    return resp


def main():
    parser = argparse.ArgumentParser(
        description="Upload subscription contacts to HubSpot with field validation and list membership.",
        epilog=(
            "Examples:\n"
            "  Dry run (preview only, no changes):\n"
            "    python upload_subscriptions.py --dryrun\n\n"
            "  Upload default CSV to default list:\n"
            "    python upload_subscriptions.py\n\n"
            "  Upload a specific CSV to a named list:\n"
            '    python upload_subscriptions.py --uploadcsv "my_export.csv" --listname "MY_LIST"\n'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--uploadcsv", default=DEFAULT_CSV, help=f"CSV file path (default: {DEFAULT_CSV})")
    parser.add_argument("--listname", default=DEFAULT_LIST_NAME, help=f"HubSpot list name to create/use (default: {DEFAULT_LIST_NAME})")
    parser.add_argument("--dryrun", action="store_true", help="Preview contacts and field validation without writing to HubSpot")
    args = parser.parse_args()

    if not HUBSPOT_API_KEY:
        print("ERROR: HUBSPOT_ACCESS_TOKEN not set in .env file")
        return

    mode = "DRY RUN" if args.dryrun else "LIVE"
    print(f"=== upload_subscriptions.py — {mode} ===\n")

    # 1. Read and validate CSV
    print(f"Reading CSV: {args.uploadcsv}")
    encoding, _ = check_csv(args.uploadcsv)
    contacts, multi_warnings = read_contacts_from_csv(args.uploadcsv, encoding=encoding)
    print(f"Found {len(contacts)} unique contact(s) to process")

    if multi_warnings:
        print(f"\nWARNING: {len(multi_warnings)} parent(s) have multiple players in the CSV.")
        print("HubSpot stores one set of player fields per contact — only the last row is kept:")
        for w in multi_warnings:
            print(w)

    if not contacts:
        print("No contacts found. Nothing to do.")
        return

    contacts_list = list(contacts.values())

    # 2. Validate field mappings against HubSpot
    print("\nValidating field mappings against HubSpot...")
    valid_props = fetch_valid_properties()
    if valid_props is not None:
        validate_and_filter(contacts_list, valid_props)

    # 3. Upsert contacts
    print(f"\n{'[DRY RUN] ' if args.dryrun else ''}Upserting contacts in HubSpot...")
    contact_ids, failed_contacts = upsert_contacts_batch(contacts_list, dryrun=args.dryrun)

    if failed_contacts:
        print(f"  {len(failed_contacts)} contact(s) failed — writing error log...")
        write_error_log(failed_contacts)

    if args.dryrun:
        print("\n[DRY RUN] No changes written. Remove --dryrun to upload.")
        return

    print(f"  Upserted: {len(contact_ids)} | Failed: {len(failed_contacts)}\n")

    if not contact_ids:
        print("No contacts were successfully upserted — skipping list step.")
        return

    # 4. Add to list
    print(f"Getting or creating list '{args.listname}'...")
    list_id = get_or_create_list(args.listname, dryrun=args.dryrun)
    if not list_id:
        print("Could not obtain a list ID — skipping list membership step.")
        return
    print()

    print(f"Adding {len(contact_ids)} contact(s) to list ID {list_id}...")
    added = add_contacts_to_list(list_id, contact_ids)

    print(f"\n=== Done ===")
    print(f"  Contacts upserted : {len(contact_ids)}")
    print(f"  Contacts in list  : {added}")
    print(f"  List name         : {args.listname}")
    print(f"  List ID           : {list_id}")
    if failed_contacts:
        print(f"  Failed upserts    : {len(failed_contacts)} (see error log)")


if __name__ == "__main__":
    main()
