# PlayMetrics to HubSpot Contact Uploader

Uploads parent contact data from a PlayMetrics player export CSV into HubSpot as individual contacts.

## What It Does

1. Reads a PlayMetrics player export CSV and extracts Parent 1–4 from each row as separate contacts
2. Splits parent full names into first name and last name
3. Downloads your existing HubSpot contact list and compares locally to prevent duplicate uploads
4. Batch-creates only new contacts via the HubSpot API
5. Filters out test/example email addresses (`@example.com`)

## Expected CSV Format

The script expects a PlayMetrics player export with these columns:

| Column | Example |
|---|---|
| `Parent 1 Name` | Jane Doe |
| `Parent 1 Email` | jane.doe@email.com |
| `Parent 1 Phone` | 555-123-4567 |
| `Parent 2 Name` | John Doe |
| `Parent 2 Email` | john.doe@email.com |
| `Parent 2 Phone` | |
| `Parent 3 Name` / `Parent 4 Name` | (same pattern) |

Each parent with a valid email becomes a HubSpot contact with `firstname`, `lastname`, `email`, and `phone` properties.

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Create a HubSpot Private App

1. Go to **HubSpot** > **Settings** > **Integrations** > **Private Apps**
2. Create a new app with the **`crm.objects.contacts.read`** and **`crm.objects.contacts.write`** scopes
3. Copy the access token

### 3. Configure the environment

Create a `.env` file in the project root:

```
HUBSPOT_ACCESS_TOKEN=pat-na1-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

### 4. Add your CSV

Place your PlayMetrics player export CSV in the project directory. The default expected filename is `playmetrics_players.csv`.

## Usage

```bash
python upload_parents.py
```

To use a different CSV file:

```bash
python upload_parents.py --uploadcsv your_file.csv
```

### Example Output

```
Reading CSV: playmetrics_players.csv
Found 1961 unique parent contacts (by email) in CSV

Downloading existing contacts from HubSpot...
  Page 1: fetched 100 contacts (98 total)
  Page 2: fetched 100 contacts (195 total)
Found 195 contacts already in HubSpot

1766 new contacts to upload

Uploading contacts to HubSpot...
  Batch 1: created 100 contacts
  Batch 2: created 100 contacts
  ...

Done! Created: 1766 | Failed: 0
```

## Duplicate Prevention

Duplicates are prevented at two levels:

- **Within the CSV**: If the same email appears on multiple players (e.g., a parent with two kids), only one contact is created
- **Against HubSpot**: The script downloads all existing contacts from HubSpot before uploading, and skips any email that already exists

This makes the script safe to run multiple times — it will only upload contacts that are new.

## Rate Limiting

The script respects HubSpot's API rate limits (100 requests per 10 seconds for private apps) with automatic pausing and retry on `429` responses.
