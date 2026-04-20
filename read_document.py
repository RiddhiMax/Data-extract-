#!/usr/bin/env python3
from pathlib import Path
import json
import argparse
import sys
import csv
import time
from typing import Any, Dict, List, Union

try:
    import requests
except Exception:
    requests = None  # The runtime that creates this file may not have requests. The user can run it locally.

# Global default API settings 
DEFAULT_API_ENDPOINT = ""
DEFAULT_BEARER_TOKEN = ""

# Rate limiting: delay between requests and after batches to avoid overwhelming the API
# The API enforces roughly 30 requests per 60 seconds, so we need at least ~2 seconds per request.
DELAY_BETWEEN_REQUESTS = 2.1  # seconds to pause after each API call
RATE_LIMIT_THRESHOLD = 30  # max requests before longer pause
RATE_LIMIT_DELAY = 30.0  # seconds to sleep after hitting threshold
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2.0


def load_abentries(path: Path) -> List[Dict[str, Any]]:
    """Load Abentry records from a JSON file.

    Supports files that are:
      - a JSON array of Abentry objects, or
      - an object with a top-level key like "Abentries" holding the array, or
      - a single Abentry object.
    """
    with path.open("r", encoding="utf-8") as f:
        data: Union[List[Dict[str, Any]], Dict[str, Any]] = json.load(f)

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        # Common container key guesses
        for key in ("Abentries", "AbEntry", "Abentry", "Items", "Data", "Records"):
            if key in data and isinstance(data[key], list):
                return data[key]  # type: ignore
            if key in data and isinstance(data[key], dict):
                nested = data[key].get("Data")
                if isinstance(nested, list):
                    return nested  # type: ignore
        
        if "Key" in data:
            return [data]  # type: ignore

    raise ValueError("Unrecognized Abentry.json structure. Expected array or object containing array.")


def build_payload(parent_key: str) -> Dict[str, Any]:
    """Construct the request body for the Document search API, matching the example provided."""
    return {
        "Document": {
            "Scope": {
                "Fields": {
                    "Key": 1,
                    "Name": 1,
                    "Description": 1,
                    "Ext": 1,
                    "Type": 1,
                    "Size": 1,
                    "ParentKey": 1,
                    "Category": 1,
                }
            },
            "Criteria": {
                "SearchQuery": {
                    "/Document/ParentKey": {
                        "$EQ": parent_key
                    }
                }
            },
        },
        "Configuration": {
            "Drivers": {
                "IDocumentSearcher": "Maximizer.Model.Access.Sql.DocumentSearcher"
            }
        },
    }


def call_api(
    url: str,
    payload: Dict[str, Any],
    token: str = "",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    if requests is None:
        raise RuntimeError(
            "The 'requests' library is not available in this environment. "
            "Please run this script on your machine with 'requests' installed (pip install requests)."
        )

    headers = {"Content-Type": "application/json"}
    if token:
        
        headers["Authorization"] = f"Bearer {token}"

    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if resp.status_code == 429:
            wait = RATE_LIMIT_DELAY * (RETRY_BACKOFF_FACTOR ** (attempt - 1))
            print(
                f"[Retry] HTTP 429 received on attempt {attempt}/{MAX_RETRIES}. "
                f"Sleeping {wait:.1f}s before retrying...",
                file=sys.stderr,
            )
            time.sleep(wait)
            continue

        try:
            resp.raise_for_status()
        except Exception as e:
            # Try to show server error text to help troubleshooting
            raise RuntimeError(f"HTTP error {resp.status_code}: {resp.text}") from e

        try:
            return resp.json()
        except Exception as e:
            raise RuntimeError("Response was not valid JSON") from e

    raise RuntimeError("Exceeded retry limit due to repeated HTTP 429 responses")


def extract_documents(response_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract the Document.Data array from the response body, if present."""
    doc = response_json.get("Document", {})
    data = doc.get("Data", [])
    if not isinstance(data, list):
        return []
    return data


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Read documents for each Abentry key by calling the Document search API.\n\n"
            "Example usage:\n"
            "  python read_document.py --input Abentry.json --url https://your.api/endpoint --token YOUR_TOKEN --out documents_by_abentry.json\n"
        )
    )
    parser.add_argument("--input", "-i", type=str, default="Abentry.json", help="Path to Abentry.json (default: Abentry.json)")
    parser.add_argument("--url", "-u", type=str, default="", help="API endpoint URL to post the search request. If omitted, uses DEFAULT_API_ENDPOINT")
    parser.add_argument("--token", "-t", type=str, default="", help="Optional auth token (Bearer). If omitted, uses DEFAULT_BEARER_TOKEN")
    parser.add_argument("--out", "-o", type=str, default="", help="Optional output JSON file to save results")
    parser.add_argument("--csv", type=str, default="", help="Optional CSV file to save a flat table of documents")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds (default: 30)")
    parser.add_argument("--limit", type=int, default=0, help="Optional: limit to first N records (0 = no limit, default: 0)")
    parser.add_argument("--docs-limit", type=int, default=0, help="Optional: stop after N entries that returned documents (0 = no limit)")

    args = parser.parse_args()

    input_path = Path(args.input)
    api_url = args.url or DEFAULT_API_ENDPOINT
    token = args.token or DEFAULT_BEARER_TOKEN

    if not api_url:
        print("API URL is not configured. Set DEFAULT_API_ENDPOINT or pass --url", file=sys.stderr)
        sys.exit(1)
    if not token:
        print("Bearer token is not configured. Set DEFAULT_BEARER_TOKEN or pass --token", file=sys.stderr)
        sys.exit(1)
    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    try:
        abentries = load_abentries(input_path)
    except Exception as e:
        print(f"Failed to parse Abentry JSON: {e}", file=sys.stderr)
        sys.exit(1)

    # Apply limit if specified
    if args.limit > 0:
        abentries = abentries[:args.limit]

    results: List[Dict[str, Any]] = []
    request_count = 0
    entries_with_docs = 0

    for idx, entry in enumerate(abentries, start=1):
        parent_key = entry.get("Key")
        if not parent_key:
            print(f"[{idx}/{len(abentries)}] Skipping entry without 'Key'", file=sys.stderr)
            continue

        payload = build_payload(parent_key)
        try:
            resp_json = call_api(api_url, payload, token=token, timeout=args.timeout)
            docs = extract_documents(resp_json)
            request_count += 1
            
            # Add delay between each request
            time.sleep(DELAY_BETWEEN_REQUESTS)
            
            # Rate limiting: pause after every N requests for extra safety
            if request_count % RATE_LIMIT_THRESHOLD == 0:
                print(f"[Rate limit] Sent {request_count} requests. Pausing for {RATE_LIMIT_DELAY}s...", file=sys.stderr)
                time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            print(f"[{idx}/{len(abentries)}] Error fetching documents for Key={parent_key}: {e}", file=sys.stderr)
            continue

        
        print(f"[{idx}/{len(abentries)}] Key={parent_key}: {len(docs)} document(s)")
        for d in docs:
            name = d.get("Name", "<no name>")
            ext = d.get("Ext", "")
            size = d.get("Size", "")
            print(f"    - {name}{ext} ({size} bytes)")

        if docs:
            entries_with_docs += 1

        results.append({
            "AbentryKey": parent_key,
            "FirstName": entry.get("FirstName"),
            "LastName": entry.get("LastName"),
            "Documents": docs,
        })

        if args.docs_limit > 0 and entries_with_docs >= args.docs_limit:
            print(f"Reached docs-limit: {entries_with_docs} entries with documents.", file=sys.stderr)
            break


    # Always save CSV with parent and document keys 
    csv_path_str = args.csv or "documents_results.csv"
    csv_path = Path(csv_path_str)
    with csv_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'AbentryKey', 'FirstName', 'LastName',
            'DocumentKey', 'DocumentName', 'Description', 'Ext', 'Type', 'Size', 'ParentKey', 'Category'
        ])
        for r in results:
            abkey = r.get('AbentryKey')
            fname = r.get('FirstName')
            lname = r.get('LastName')
            docs = r.get('Documents', []) or []
            if not docs:
                writer.writerow([abkey, fname, lname, '', '', '', '', '', '', '', ''])
            else:
                for d in docs:
                    writer.writerow([
                        abkey, fname, lname,
                        d.get('Key', ''), d.get('Name', ''), d.get('Description', ''), d.get('Ext', ''),
                        d.get('Type', ''), d.get('Size', ''), d.get('ParentKey', ''), d.get('Category', '')
                    ])
    print(f"Saved CSV to: {csv_path.resolve()}")


if __name__ == "__main__":
    main()
