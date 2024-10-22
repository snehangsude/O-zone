from google.cloud import bigquery
from google.cloud.exceptions import NotFound

class BigQueryManager:
    def __init__(self, location: str, dataset_id: str, table_id: str, client: bigquery.Client):
        """
        Initialize the BigQuery manager with necessary configuration.
        :param location: The location for the BigQuery dataset.
        :param dataset_id: The ID of the BigQuery dataset.
        :param table_id: The ID of the BigQuery table.
        :param client: An instance of the BigQuery client.
        """
        self.location = location
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = client

    def create_dataset_if_not_exists(self):
        """Creates the dataset if it does not already exist."""
        dataset = bigquery.Dataset(self.dataset_id)
        dataset.location = self.location

        try:
            self.client.create_dataset(dataset)
            print(f"Created dataset {self.dataset_id}.")
        except Exception as e:
            print(f"Dataset {self.dataset_id} already exists or another error occurred: {e}")

    def create_table_if_not_exists(self, schema: list):
        """Creates the table if it does not already exist."""
        table_ref = f"{self.dataset_id}.{self.table_id}"

        try:
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            print(f"Created table {table_ref}.")
        except Exception as e:
            print(f"Table {table_ref} already exists or another error occurred: {e}")

    def insert_data(self, rows_to_insert: list):
        """Inserts rows of data into the table."""
        table_ref = f"{self.dataset_id}.{self.table_id}"
        errors = self.client.insert_rows_json(table_ref, rows_to_insert)

        if errors == []:
            print("New rows have been added.")
        else:
            print("Errors occurred while inserting rows: {}".format(errors))

    def ensure_dataset_and_table(self, schema: list):
        """Ensures that both the dataset and table exist before inserting data."""
        self.create_dataset_if_not_exists()
        self.create_table_if_not_exists(schema)

    def extract_data(self, query: str):
        """Extracts data from BigQuery by running a SQL query.
        
        :param query: SQL query string to run against BigQuery.
        :return: A list of dictionaries containing the query result.
        """
        try:
            query_job = self.client.query(query)
            results = query_job.result()  # Waits for job to complete

            # Convert result to list of dictionaries
            data = [dict(row) for row in results]
            print(f"Query returned {len(data)} rows.")
            return data

        except Exception as e:
            print(f"Error while running query: {e}")
            return []

