# HubSpot Contact Uploader

Two scripts for uploading contact data from CSV exports into HubSpot.

| Script | Purpose |
|---|---|
| `upload_subscriptions.py` | Upload subscription exports — sets parent + player fields, marks contacts as marketing, adds to a list |
| `upload_parents.py` | Upload PlayMetrics player exports — extracts Parent 1–4 from each row as individual contacts |

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Create a HubSpot Private App and get your token

1. Log into HubSpot and click your **account name** (top-right avatar)
2. Go to **Settings** (gear icon) → **Integrations** → **Private Apps**
3. Click **Create a private app**
4. Give it a name (e.g. `Subscription Upload`)
5. Go to the **Scopes** tab and enable:
   - `crm.objects.contacts.read`
   - `crm.objects.contacts.write`
   - `crm.lists.read`
   - `crm.lists.write`
6. Click **Create app** → **Continue creating**
7. Copy the access token shown — it starts with `pat-na1-...`

> The token is only shown once. If you lose it, you can rotate it from the Private Apps page.

### 3. Configure your environment

```bash
cp .env.example .env
```

Open `.env` and replace the placeholder with your token:

```
HUBSPOT_ACCESS_TOKEN=pat-na1-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

---

## upload_subscriptions.py

Reads a subscription export CSV, upserts each row as a HubSpot contact (creating or updating by email), sets all available fields including player info, marks contacts as marketing contacts, and adds them to a static list.

### CSV format

The script expects these columns (column names must match exactly):

| Column | Maps to HubSpot property |
|---|---|
| `user_email` | `email` (unique key) |
| `user_first_name` | `firstname` |
| `user_last_name` | `lastname` |
| `user_id` | `user_id` |
| `street` | `address` |
| `city` | `city` |
| `state` | `state` |
| `zip` | `zip` |
| `player_id` | `player_id` |
| `player_first_name` | `player_first_name` |
| `player_last_name` | `player_last_name` |
| `birth_date` | `player_date_of_birth` |
| `subscription_id` | `subscription_id` |
| `subscription_purchase_date` | `subscription_purchase_date` |
| `program_id` | `program_id` |
| `program_name` | `program_name` |
| `package_id` | `package_id` |
| `package_name` | `package_name` |

Date columns (`birth_date`, `subscription_purchase_date`) must be in `M/D/YYYY` format.

### Usage

```
python upload_subscriptions.py [-h] [--uploadcsv UPLOADCSV] [--listname LISTNAME] [--dryrun]
```

```bash
# Dry run — validate fields and preview contacts without writing anything
python upload_subscriptions.py --dryrun

# Upload using defaults (SubscriptionsExport (2).csv → list ID_20260420)
python upload_subscriptions.py

# Upload a specific file to a named list
python upload_subscriptions.py --uploadcsv "my_export.csv" --listname "MY_LIST"
```

### What it does on each run

1. **CSV pre-flight** — detects encoding (UTF-8, UTF-8 with BOM, Windows-1252), strips stray whitespace from headers, and validates all required columns are present before touching the API
2. **Field validation** — fetches all valid HubSpot contact property names and warns about (and drops) any fields that don't exist in your account
3. **Upsert contacts** — creates new contacts or updates existing ones, matched by email address, with all mapped fields
4. **Multi-player warning** — if a parent email appears on multiple rows (multiple children), warns you that only the last child's player data will be stored
5. **List membership** — finds or creates a static HubSpot list and adds all upserted contacts to it
6. **Error log** — any contacts that fail to upsert are written to `upload_errors_<timestamp>.csv` for review and retry

### Example output

```
=== upload_subscriptions.py — LIVE ===

Reading CSV: SubscriptionsExport (2).csv
  Encoding: utf-8-sig
Found 38 unique contact(s) to process

Validating field mappings against HubSpot...
  All 18 field(s) validated OK

Upserting contacts in HubSpot...
  Batch 1: upserted 38 contacts
  Upserted: 38 | Failed: 0

Getting or creating list 'ID_20260420'...
  Found existing list 'ID_20260420' with ID 44

Adding 38 contact(s) to list ID 44...
  Added 38 contacts to list

=== Done ===
  Contacts upserted : 38
  Contacts in list  : 38
  List name         : ID_20260420
  List ID           : 44
```

---

## upload_parents.py

Reads a PlayMetrics player export CSV, extracts up to four parent contacts per player row, deduplicates by email, and batch-creates any contacts not already in HubSpot.

### CSV format

Expects a PlayMetrics player export with columns in the pattern:

| Column | Example |
|---|---|
| `Parent 1 Name` | Jane Doe |
| `Parent 1 Email` | jane.doe@email.com |
| `Parent 1 Phone` | 555-123-4567 |
| `Parent 2 Name` | John Doe |
| `Parent 2 Email` | john.doe@email.com |
| `Parent 3 Name` / `Parent 4 Name` | (same pattern) |

### Usage

```bash
# Upload using the default file (playmetrics_players.csv)
python upload_parents.py

# Upload a specific file
python upload_parents.py --uploadcsv your_file.csv
```

---

## Sensitive files

The following are excluded from version control via `.gitignore` and must never be committed:

| File/Pattern | Contains |
|---|---|
| `.env` | Your HubSpot access token |
| `*.csv` | Contact data (PII) |
| `*.xlsx` | Contact data (PII) |

Always use `.env.example` as the template — it contains only the placeholder, never a real token.
