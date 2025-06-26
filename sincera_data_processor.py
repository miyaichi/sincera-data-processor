import sys
import time
import pandas as pd
import requests
from collections import deque

# --- Configuration ---
API_TOKEN = "<Your API Token>"
API_URL_ID_BASE = "https://open.sincera.io/api/publishers?id="
API_URL_DOMAIN_BASE = "https://open.sincera.io/api/publishers?domain="
FIELDS = [
    "publisher_id", "name", "visit_enabled", "status", "primary_supply_type",
    "pub_description", "categories", "slug", "avg_ads_to_content_ratio",
    "avg_ads_in_view", "avg_ad_refresh", "total_unique_gpids",
    "id_absorption_rate", "avg_page_weight", "avg_cpu", "total_supply_paths",
    "reseller_count", "owner_domain", "updated_at"
]
DEFAULT_RETRY_DELAY = 2
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10
# Rate Limiting Configuration
RATE_LIMIT_COUNT = 45
RATE_LIMIT_PERIOD = 60  # in seconds

class RateLimiter:
    """
    A simple rate limiter to ensure we don't exceed a certain number of requests
    within a given time period.
    """
    def __init__(self, max_requests, period_seconds):
        self.max_requests = max_requests
        self.period_seconds = period_seconds
        self.request_timestamps = deque()

    def wait_if_needed(self):
        """
        Checks if the rate limit has been reached and waits if necessary.
        This should be called before making a request.
        """
        current_time = time.monotonic()
        # Remove timestamps older than the defined period
        while self.request_timestamps and self.request_timestamps[0] <= current_time - self.period_seconds:
            self.request_timestamps.popleft()

        if len(self.request_timestamps) >= self.max_requests:
            # Calculate wait time
            time_to_wait = self.request_timestamps[0] - (current_time - self.period_seconds)
            if time_to_wait > 0:
                print(f"Rate limit reached. Waiting for {time_to_wait:.2f} seconds...")
                time.sleep(time_to_wait)
        
    def record_request(self):
        """Records a new request timestamp."""
        self.request_timestamps.append(time.monotonic())

def fetch_publisher_metadata(identifier, id_type='id'):
    """
    Fetches publisher metadata from the Sincera API for a given identifier.
    Includes retry logic for rate limiting and other transient errors.
    'id_type' can be 'id' or 'domain'.
    """
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    
    if id_type == 'id':
        try:
            id_str = str(int(identifier))
            url = API_URL_ID_BASE + id_str
            log_id = f"publisher_id {id_str}"
        except (ValueError, TypeError):
            print(f"Warning: Invalid publisher_id '{identifier}'. Skipping.")
            return {field: None for field in FIELDS}
    elif id_type == 'domain':
        id_str = str(identifier)
        url = API_URL_DOMAIN_BASE + id_str
        log_id = f"domain {id_str}"
    else:
        print(f"Warning: Invalid id_type '{id_type}'. Skipping.")
        return {field: None for field in FIELDS}

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    if data:
                        data = data[0]
                    else:
                        print(f"Warning: Empty list returned for {log_id}.")
                        return {field: None for field in FIELDS}

                result = {field: data.get(field, None) for field in FIELDS}
                if isinstance(result.get("categories"), list):
                    result["categories"] = "; ".join(result["categories"])
                return result

            elif response.status_code == 429:  # Too Many Requests
                retry_after = int(response.headers.get('Retry-After', DEFAULT_RETRY_DELAY))
                print(
                    f"Rate limited for {log_id}. "
                    f"Retrying after {retry_after}s (Attempt {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(retry_after)

            else:
                print(
                    f"Warning: Failed to fetch {log_id}. "
                    f"Status: {response.status_code}, Response: {response.text}"
                )
                return {field: None for field in FIELDS}

        except requests.exceptions.RequestException as e:
            print(f"Error fetching {log_id}: {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"Retrying... (Attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(DEFAULT_RETRY_DELAY)
            else:
                print(f"Failed to fetch {log_id} after {MAX_RETRIES} attempts.")
                return {field: None for field in FIELDS}

    return {field: None for field in FIELDS}

def process_excel_file(input_file):
    """
    Reads an Excel file, fetches publisher data for each row,
    and saves the results to a new Excel file.
    It prioritizes 'domain' over 'publisher_id' if both are present.
    """
    try:
        df = pd.read_excel(input_file)
        print(f"Successfully read {len(df)} rows from '{input_file}'")
    except FileNotFoundError:
        print(f"Error: Input file not found at '{input_file}'")
        return
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    has_pub_id = 'publisher_id' in df.columns
    has_domain = 'domain' in df.columns

    if not has_pub_id and not has_domain:
        print("Error: Excel file must contain either a 'publisher_id' or 'domain' column.")
        return

    rate_limiter = RateLimiter(RATE_LIMIT_COUNT, RATE_LIMIT_PERIOD)
    results = []
    total_rows = len(df)

    for index, row in df.iterrows():
        rate_limiter.wait_if_needed()

        result = None
        log_msg = ""
        
        # Determine identifier and type
        identifier, id_type = (None, None)
        if has_domain and pd.notna(row.get('domain')):
            identifier = row['domain']
            id_type = 'domain'
            log_msg = f"Processing {index + 1}/{total_rows}: domain={identifier}"
        elif has_pub_id and pd.notna(row.get('publisher_id')):
            identifier = row['publisher_id']
            id_type = 'id'
            log_msg = f"Processing {index + 1}/{total_rows}: publisher_id={identifier}"
        
        print(log_msg)

        if identifier:
            rate_limiter.record_request()
            result = fetch_publisher_metadata(identifier, id_type=id_type)
        else:
            print(f"Skipping row {index + 1}/{total_rows}: No valid publisher_id or domain.")
            result = {field: None for field in FIELDS}

        # Preserve original identifiers
        if has_pub_id:
            result['input_publisher_id'] = row.get('publisher_id')
        if has_domain:
            result['input_domain'] = row.get('domain')
            
        results.append(result)

    result_df = pd.DataFrame(results)
    
    if '.' in input_file:
        base_name = input_file.rsplit('.', 1)[0]
        output_file = f"{base_name}_results.xlsx"
    else:
        output_file = f"{input_file}_results.xlsx"

    try:
        result_df.to_excel(output_file, index=False)
        print(f"\nProcessing complete. Results written to '{output_file}'")
    except Exception as e:
        print(f"\nError writing to Excel file: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python sincera_data_processor.py <path_to_excel_file>")
        sys.exit(1)

    input_excel_file = sys.argv[1]
    process_excel_file(input_excel_file)
