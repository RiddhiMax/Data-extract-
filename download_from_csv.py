import csv
import os
import requests
import base64
from pathlib import Path

# --- CONFIG ---
BASE_URL = "https://api.maximizer.com/octopus/BinaryDownload/"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJteHA0MW9xbm92emppcnZvN3JweCIsImlhdCI6MTc3Mzg1ODk0MywiZXhwIjoxODA1MzI4MDAwLCJteC1jaWQiOiIzMEVBNDQ5Qi1DQjYwLTRFREMtOUJDNi02N0RENjUxQkZDMDUiLCJteC13c2lkIjoiMUI1OTVDOTMtNkJBNi00QkNELUI0NDQtNzI5Q0IxNzEzMUQ2IiwibXgtZGIiOiIxYzVkZGFhMDVmZGQ0YWRiYWE5MjRkZjQxYzM1NGFkOCIsIm14LXVpZCI6Ik1BU1RFUiIsIm14LXBsIjoiY2xvdWQifQ.bs1CbWoucmplOmz04z4wSp5dUCgf_OW2WHV49PXmXjw"  # <-- Fill in your Bearer token here
CSV_PATH = "documents_results.csv"  # Input CSV file
OUTPUT_DIR = "downloads"  # All folders will be created inside this directory

MIME_TYPE_MAP = {
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "text/plain": ".txt",
    "application/zip": ".zip",
    "application/x-rar-compressed": ".rar",
}

def decode_base64_key(encoded_key):
    """Decode base64 key to identify it."""
    try:
        decoded = base64.b64decode(encoded_key).decode('utf-8')
        return decoded
    except Exception:
        return encoded_key  # Return original if decode fails

def sanitize_folder_name(folder_name):
    """Sanitize folder name by removing/replacing invalid Windows characters."""
    # Invalid characters for Windows: < > : " / \ | ? * and control characters (tab, newline, etc)
    invalid_chars = '<>:"/\\|?*\t\n\r\x00'
    for char in invalid_chars:
        folder_name = folder_name.replace(char, '_')
    return folder_name

def download_document(document_key, abentry_key, token):
    url = f"{BASE_URL}{document_key}"
    headers = {"Authorization": f"Bearer {token}"}
    
    # Decode base64 key and sanitize for use as folder name
    decoded_key = decode_base64_key(abentry_key)
    safe_folder_name = sanitize_folder_name(decoded_key)
    
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            print(f"Failed to download {document_key} (status {resp.status_code})")
            return
        content_type = resp.headers.get('content-type', '').lower()
        mime_type = content_type.split(';')[0].strip()
        ext = MIME_TYPE_MAP.get(mime_type, ".bin")
        # Try to get filename from headers
        cd = resp.headers.get('content-disposition', '')
        if 'filename=' in cd:
            filename = cd.split('filename=')[1].strip('"\' ')
        else:
            filename = f"{document_key}{ext}"
        # Create output folder using sanitized decoded key
        folder = Path(OUTPUT_DIR) / safe_folder_name
        folder.mkdir(parents=True, exist_ok=True)
        out_path = folder / filename
        with open(out_path, "wb") as f:
            f.write(resp.content)
        print(f"Downloaded {filename} to {folder} (Key: {abentry_key})")
    except Exception as e:
        print(f"Error downloading {document_key}: {e}")

def main():
    with open(CSV_PATH, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            abentry_key = row.get('AbentryKey', '').strip()
            document_key = row.get('DocumentKey', '').strip()
            if abentry_key and document_key:
                download_document(document_key, abentry_key, TOKEN)

if __name__ == "__main__":
    main()
