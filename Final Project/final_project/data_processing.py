import requests
import pandas as pd
from connection import DatabaseConnection
import psycopg2
from psycopg2.extras import execute_values


class DataProcessor:
    def __init__(self, api_url):
        self.api_url = api_url

    def fetch_data(self, params=None, headers=None):
        try:
            response = requests.get(self.api_url, params=params, headers=headers)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print(f"Failed to fetch data. Status Code: {response.status_code}")
                return None
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def process_data(self, data):
        if data and 'data' in data and 'content' in data['data']:
            return data['data']['content']
        else:
            return None

    def get_dataframe(self):
        data = self.fetch_data()
        processed_data = self.process_data(data)
        df = pd.DataFrame(processed_data)
        return df
        

# Ganti 'https://api.example.com/data' dengan URL API JSON yang sesuai
api_url = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab'

# Contoh penggunaan kelas JSONAPIScraper
if __name__ == "__main__":
    scraper = DataProcessor(api_url)

    # Example query parameters and headers
    params = {"level": "kab"}
    headers = {"accept": "application/json"}

    data = scraper.fetch_data(params=params, headers=headers)

    if data:
        processed_data = scraper.process_data(data)
        if processed_data:
            print("Processed Data.")
        else:
            print("Data processing failed.")
    else:
        print("Data retrieval failed.")

    parse_df = DataProcessor(api_url)
    df = parse_df.get_dataframe()

    # Database connection parameters
    db_params = {
        'host': 'localhost',  # e.g., 'localhost' or '127.0.0.1'
        'database': 'fp_rn_raw',
        'user': 'rani',
        'password': '123456'
    }

    conn = psycopg2.connect(
    host=db_params['host'],
    database=db_params['database'],
    user=db_params['user'],
    password=db_params['password'],
    port='5432'
    )   

    cursor = conn.cursor()

    # Assuming you have connected to PostgreSQL and have the 'connection' object
    # Replace 'your_table_name' with the desired table name
    table_name = 'rekap_jabar'

    # DataFrame 'df' contains the data you want to store in the table
    # Get the DataFrame column names
    columns = df.columns.tolist()

    column_data_types = {
    'int64': 'integer',   # Map pandas int64 to PostgreSQL integer
    'float64': 'float',   # Map pandas float64 to PostgreSQL float
    'object': 'text',     # Map pandas object to PostgreSQL text
    'datetime64': 'timestamp',  # Map pandas datetime64 to PostgreSQL timestamp
    'bool': 'boolean',    # Map pandas bool to PostgreSQL boolean
    }

    # Generate the SQL query to create the table
    create_table_query = f"CREATE TABLE {table_name} ("
    for col in columns:
        data_type = column_data_types.get(df[col].dtype.name, 'text')
        create_table_query += f"{col} {data_type}, "

    # Remove the last comma and space from the query
    create_table_query = create_table_query[:-2]
    create_table_query += ");"
    insert_query = f"INSERT INTO {table_name} VALUES %s"

    rows = [tuple(row) for row in df.values]

    # Write the DataFrame to the PostgreSQL table
    try:
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' has been successfully created.")

        execute_values(cursor, insert_query, rows)
        print("DataFrame has been successfully written to the table.")

        conn.commit()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")



