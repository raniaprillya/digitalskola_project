import psycopg2

class DatabaseConnection:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print(f"Database '{self.database}' connected successfully!")
        except (Exception, psycopg2.Error) as error:
            print(f"Error while connecting to database '{self.database}': {error}")

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
            print(f"Query executed successfully on database '{self.database}'!")
        except (Exception, psycopg2.Error) as error:
            print(f"Error executing the query on database '{self.database}': {error}")

    def fetch_data(self, query):
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            return result
        except (Exception, psycopg2.Error) as error:
            print(f"Error fetching data from database '{self.database}': {error}")
            return None

    def close_connection(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            print(f"Database connection for '{self.database}' closed.")

# # Change the values below with your actual PostgreSQL host, user, and password
# HOST = "localhost"
# USER = "rani"
# PASSWORD = "123456"

# # Database 1
# DATABASE1 = "fp_rn_raw"
# db_connection1 = DatabaseConnection(HOST, DATABASE1, USER, PASSWORD)
# db_connection1.connect()

# # Database 2
# DATABASE2 = "fp_rn_result"
# db_connection2 = DatabaseConnection(HOST, DATABASE2, USER, PASSWORD)
# db_connection2.connect()
