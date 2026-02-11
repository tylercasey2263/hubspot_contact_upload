import csv
import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

HUBSPOT_API_KEY = os.getenv("HUBSPOT_ACCESS_TOKEN")
BASE_URL = "https://api.hubapi.com"
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json",
}
CSV_FILE = "playmetrics_players_20260210_181136.csv"

# HubSpot rate limit: 100 requests per 10 seconds for private apps
BATCH_SIZE = 100
RATE_LIMIT_PAUSE = 10


def split_name(full_name):
    """Split a full name into first and last name."""
    parts = full_name.strip().split()
    if len(parts) == 0:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def extract_parents_from_csv(filepath):
    """Read CSV and extract unique parent contacts keyed by email."""
    contacts = {}
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for i in range(1, 5):
                name = row.get(f"Parent {i} Name", "").strip()
                email = row.get(f"Parent {i} Email", "").strip().lower()
                phone = row.get(f"Parent {i} Phone", "").strip()

                # Skip if no email or test/example addresses
                if not email or email.endswith("@example.com"):
                    continue

                # Only keep the first occurrence per email (dedup within CSV)
                if email not in contacts:
                    first, last = split_name(name)
                    contacts[email] = {
                        "name": name,
                        "firstname": first,
                        "lastname": last,
                        "email": email,
                        "phone": phone,
                    }

    return contacts


def fetch_existing_emails():
    """Download all contacts from HubSpot and return a set of their emails.

    Uses the list contacts endpoint with pagination (100 per page).
    """
    existing = set()
    url = f"{BASE_URL}/crm/v3/objects/contacts"
    params = {"limit": 100, "properties": "email"}
    page = 0

    while True:
        page += 1
        resp = requests.get(url, headers=HEADERS, params=params)

        if resp.status_code == 429:
            print("  Rate limited — pausing 10s...")
            time.sleep(RATE_LIMIT_PAUSE)
            continue

        if resp.status_code != 200:
            print(f"  Error fetching contacts: {resp.status_code}")
            print(f"    {resp.text[:500]}")
            break

        data = resp.json()
        results = data.get("results", [])
        for contact in results:
            email = contact.get("properties", {}).get("email", "")
            if email:
                existing.add(email.strip().lower())

        print(f"  Page {page}: fetched {len(results)} contacts ({len(existing)} total)")

        # Check for next page
        paging = data.get("paging")
        if paging and paging.get("next"):
            params["after"] = paging["next"]["after"]
        else:
            break

    return existing


def create_contacts_batch(contacts_list):
    """Create contacts in HubSpot using the batch API (up to 100 per call)."""
    created = 0
    failed = 0

    for i in range(0, len(contacts_list), BATCH_SIZE):
        batch = contacts_list[i : i + BATCH_SIZE]
        payload = {
            "inputs": [
                {
                    "properties": {
                        "email": c["email"],
                        "firstname": c["firstname"],
                        "lastname": c["lastname"],
                        "phone": c["phone"],
                    }
                }
                for c in batch
            ]
        }

        resp = requests.post(
            f"{BASE_URL}/crm/v3/objects/contacts/batch/create",
            headers=HEADERS,
            json=payload,
        )

        if resp.status_code == 201:
            results = resp.json().get("results", [])
            created += len(results)
            print(f"  Batch {i // BATCH_SIZE + 1}: created {len(results)} contacts")
        elif resp.status_code == 429:
            print("  Rate limited — pausing 10s and retrying batch...")
            time.sleep(RATE_LIMIT_PAUSE)
            resp = requests.post(
                f"{BASE_URL}/crm/v3/objects/contacts/batch/create",
                headers=HEADERS,
                json=payload,
            )
            if resp.status_code == 201:
                results = resp.json().get("results", [])
                created += len(results)
                print(f"  Batch {i // BATCH_SIZE + 1} (retry): created {len(results)} contacts")
            else:
                failed += len(batch)
                print(f"  Batch {i // BATCH_SIZE + 1} FAILED after retry: {resp.status_code}")
                print(f"    {resp.text[:500]}")
        else:
            failed += len(batch)
            print(f"  Batch {i // BATCH_SIZE + 1} FAILED: {resp.status_code}")
            print(f"    {resp.text[:500]}")

        # Respect rate limits between batches
        if i + BATCH_SIZE < len(contacts_list):
            time.sleep(RATE_LIMIT_PAUSE)

    return created, failed


def main():
    if not HUBSPOT_API_KEY:
        print("ERROR: HUBSPOT_ACCESS_TOKEN not set in .env file")
        print("Create a .env file with: HUBSPOT_ACCESS_TOKEN=your-token-here")
        return

    print(f"Reading CSV: {CSV_FILE}")
    contacts = extract_parents_from_csv(CSV_FILE)
    print(f"Found {len(contacts)} unique parent contacts (by email) in CSV\n")

    if not contacts:
        print("No contacts to upload.")
        return

    print("Downloading existing contacts from HubSpot...")
    existing_emails = fetch_existing_emails()
    print(f"Found {len(existing_emails)} contacts already in HubSpot\n")

    # Filter out contacts that already exist
    new_contacts = [c for email, c in contacts.items() if email not in existing_emails]
    print(f"{len(new_contacts)} new contacts to upload\n")

    if not new_contacts:
        print("All contacts already exist in HubSpot. Nothing to do.")
        return

    print("Uploading contacts to HubSpot...")
    created, failed = create_contacts_batch(new_contacts)
    print(f"\nDone! Created: {created} | Failed: {failed}")


if __name__ == "__main__":
    main()
