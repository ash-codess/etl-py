# ETL Process for Bank Market Capitalization

This project extracts, transforms, and loads (ETL) data related to the market capitalization of the largest banks. The data is fetched from a web archive of a Wikipedia page, transformed according to specified exchange rates, and then loaded into a CSV file and an SQLite database.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Functions](#functions)
- [Contact](#contact)

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/ash-codess/etl-python.git
    ```
2. Navigate to the project directory:
    ```sh
    cd etl-python
    ```
3. Install the required dependencies:
    ```sh
    pip install -r requirements.txt
    ```

## Usage

1. Ensure you have the exchange rates CSV file (`exchange_rate.csv`) in the project directory.
2. Run the main script:
    ```sh
    python main.py
    ```

## Functions

### fetch_html(url: str) -> str
    Fetches the HTML content of the given URL.

### extract_table(html: str, table_attribs: str) -> pd.DataFrame
    Extracts a table from the given HTML content based on the provided table attributes.

### extract(url: str, table_attribs: str) -> Union[pd.DataFrame, None]
    Extracts a table from the given URL and attributes.

### transform_market_cap(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame
    Transforms the 'Market cap (US$ billion)' column in the DataFrame to different currencies.

### load_to_csv(df: pd.DataFrame, output_path: str) -> None
    Saves the DataFrame to a CSV file.

### load_to_db(df: pd.DataFrame, conn: sqlite3.Connection, table_name: str) -> None
    Saves the DataFrame to an SQLite database table.

### run_query(query: str, conn: sqlite3.Connection) -> list
    Runs an SQL query on the database and returns the results.


## Contact

For questions or suggestions, please contact [asthaghosh.it@gmail.com](mailto:asthaghosh.it@gmail.com).

