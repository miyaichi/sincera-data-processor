# Sincera API Data Processor

This script automates the process of fetching publisher metadata from the Sincera API using a list of publisher IDs or domains from an Excel file. It is designed to handle API rate limits gracefully and provides robust error handling.

## Features

- **Bulk Data Fetching**: Reads publisher IDs or domains from an input Excel file.
- **Flexible Identifier**: Can use either `publisher_id` or `domain` for API lookups. If a `domain` is provided, it will be prioritized.
- **Rate Limit Handling**: Automatically manages the API rate limit (45 requests per minute) to prevent interruptions.
- **Retry Logic**: Implements a retry mechanism for transient network errors or API issues.
- **Excel Output**: Saves the fetched data into a new, clearly-named Excel file (`<original_filename>_results.xlsx`).

## Requirements

- Python 3.x
- `pandas` library
- `requests` library

## Installation

1.  **Clone the repository or download the script.**

2.  **Install the required Python libraries:**
    ```bash
    pip install pandas requests
    ```

## Usage

Run the script from your terminal, providing the path to your input Excel file as an argument.

```bash
python sincera_data_processor.py <path_to_your_excel_file>
```

**Example:**
```bash
python sincera_data_processor.py tranco.xlsx
```

### Input File Format

The script requires an Excel file containing at least one of the following columns:
- `publisher_id`: The Sincera publisher ID.
- `domain`: The publisher's domain name.

If both columns are present, the `domain` will be used for the API lookup. The script will process each row to fetch the corresponding publisher data.

### Output File Format

The script generates a new Excel file with `_results` appended to the original filename (e.g., `tranco_results.xlsx`). This file will contain:
- All the metadata fields fetched from the Sincera API.
- `input_publisher_id`: The original publisher ID from the input file (if it existed).
- `input_domain`: The original domain from the input file (if it existed).

## Configuration

The following constants can be configured at the top of the `sincera_data_processor.py` script:

- `API_TOKEN`: Your Sincera API token.
- `MAX_RETRIES`: The maximum number of retries for a failed request.
- `REQUEST_TIMEOUT`: The timeout in seconds for an API request.
- `RATE_LIMIT_COUNT` / `RATE_LIMIT_PERIOD`: The parameters for the rate limiter (e.g., 45 requests per 60 seconds).
