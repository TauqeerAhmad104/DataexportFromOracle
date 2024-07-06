import cx_Oracle
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define your connection details
username = 'your_username'
password = 'your_password'
dsn = 'hostname:port/service_name'  # e.g., 'localhost:1521/orclpdb1'

# Define chunk size
chunk_size = 10000  # Adjust based on your memory capacity

def fetch_data_in_chunks(query, chunk_size):
    try:
        logging.info("Connecting to Oracle database...")
        connection = cx_Oracle.connect(username, password, dsn)
        cursor = connection.cursor()

        logging.info("Executing query...")
        cursor.execute(query)

        logging.info("Fetching data in chunks...")
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield rows, [desc[0] for desc in cursor.description]
        
        logging.info("Closing connection...")
        cursor.close()
        connection.close()

    except cx_Oracle.DatabaseError as e:
        logging.error(f"Database error occurred: {e}")
        raise
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

def main():
    query = '''
    SELECT column1, column2, column3
    FROM your_table
    WHERE your_conditions
    '''

    parquet_file = 'your_output_file.parquet'

    first_chunk = True

    for chunk, column_names in fetch_data_in_chunks(query, chunk_size):
        logging.info(f"Processing chunk with {len(chunk)} rows")
        
        df_chunk = pd.DataFrame(chunk, columns=column_names)

        # Convert DataFrame to Arrow Table
        table = pa.Table.from_pandas(df_chunk)

        # Write to Parquet file
        if first_chunk:
            pq.write_table(table, parquet_file)
            first_chunk = False
        else:
            with pq.ParquetWriter(parquet_file, table.schema) as writer:
                writer.write_table(table)

    logging.info(f"Data has been saved to {parquet_file}")

if __name__ == "__main__":
    main()
