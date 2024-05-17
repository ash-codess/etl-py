import logging
import sqlite3
from typing import Union
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

def fetch_html(url: str) -> str:
    """
    Fetch the HTML content of the given URL.

    Args:
        url (str): The URL to fetch the HTML content from.

    Returns:
        str: The HTML content of the URL.

    Raises:
        Exception: If an error occurs while fetching the HTML content.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching HTML content: {e}")

def extract_table(html: str, table_attribs: str) -> pd.DataFrame:
    """
    Extract a table from the given HTML content based on the provided table attributes.

    Args:
        html (str): The HTML content to extract the table from.
        table_attribs (str): The attributes of the table to extract.

    Returns:
        pd.DataFrame: The extracted table as a pandas DataFrame.

    Raises:
        Exception: If an error occurs while extracting the table.
    """
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('span', string=table_attribs).find_next('table')
    try:
        df = pd.read_html(StringIO(str(table)))[0]
        return df
    except ValueError as e:
        raise Exception(f"Error extracting table: {e}")

def extract(url: str, table_attribs: str) -> Union[pd.DataFrame, None]:
    """
    Extract a table from the given URL based on the provided table attributes.

    Args:
        url (str): The URL to fetch the HTML content from.
        table_attribs (str): The attributes of the table to extract.

    Returns:
        pd.DataFrame or None: The extracted table as a pandas DataFrame, or None if an error occurred.
    """
    try:
        html = fetch_html(url)
        df = extract_table(html, table_attribs)
        logging.info('Data extraction complete. Initiating Transformation process.')
        return df
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        return None

def transform_market_cap(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame:
    """
    Transform the 'Market cap (US$ billion)' column in the given DataFrame
    to different currencies using the provided exchange rates.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'Market cap (US$ billion)' column.
        exchange_rates (dict): A dictionary containing exchange rates for different currencies.

    Returns:
        pd.DataFrame: The transformed DataFrame with additional columns for market cap
        in different currencies.
    """
    for currency, rate in exchange_rates.items():
        df[f'MC_{currency}_Billion'] = [round(x * rate, 2) for x in df['Market cap (US$ billion)']]
    logging.info('Data transformation complete. Initiating Loading process.')
    return df

def load_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Save the given DataFrame to a CSV file at the specified output path.

    Args:
        df (pd.DataFrame): The DataFrame to save as a CSV file.
        output_path (str): The output path for the CSV file.
    """
    df.to_csv(output_path, index=False)
    logging.info('Data saved to CSV file.')

def load_to_db(df: pd.DataFrame, conn: sqlite3.Connection, table_name: str) -> None:
    """
    Save the given DataFrame to a database table with the provided name.

    Args:
        df (pd.DataFrame): The DataFrame to save to the database table.
        conn (sqlite3.Connection): The SQLite connection object.
        table_name (str): The name of the database table to save the DataFrame to.
    """
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    logging.info('Data loaded to Database as a table, Executing queries.')

def run_query(query: str, conn: sqlite3.Connection) -> list:
    """
    Run the given SQL query on the database and return the results.

    Args:
        query (str): The SQL query to execute.
        conn (sqlite3.Connection): The SQLite connection object.

    Returns:
        list: A list of tuples containing the query results.
    """
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    logging.info('Query executed successfully.')
    return results

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    output_csv_path = './Largest_banks_data.csv'
    database_name = 'Banks.db'
    table_name = 'Largest_banks'
    exchange_rates = pd.read_csv('exchange_rate.csv', index_col=0).to_dict()['Rate']

    logging.info('Preliminaries complete. Initiating ETL process.')

    df = extract(url, 'By market capitalization')
    if df is not None:
        df = transform_market_cap(df, exchange_rates)
        load_to_csv(df, output_csv_path)

        with sqlite3.connect(database_name) as conn:
            load_to_db(df, conn, table_name)
            print(run_query('SELECT * FROM Largest_banks LIMIT 5', conn))
            print(run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', conn))
            print(run_query('SELECT "Bank name" from Largest_banks LIMIT 5', conn))

    logging.info('Process completed.')

if __name__ == '__main__':
    main()