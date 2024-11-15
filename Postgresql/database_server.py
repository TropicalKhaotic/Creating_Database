import psycopg2
import json
from ExcelConversion.SheetsConfig.SheetsConfig import date_type

class Postgres:
    def __init__(self, dbname, user, password=None, host='localhost', port='5432',
                 sslmode='prefer', sslrootcert=None, sslcert=None, sslkey=None):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.sslmode = sslmode
        self.sslrootcert = sslrootcert
        self.sslcert = sslcert
        self.sslkey = sslkey
        self.connection = None

    def connect(self):
        """
        Establish a connection to the PostgreSQL database with SSL.
        """
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                sslmode=self.sslmode,
                sslrootcert=self.sslrootcert,
                sslcert=self.sslcert,
                sslkey=self.sslkey
            )
            print("Connection established with DATABASE")
        except psycopg2.OperationalError as e:
            print(f"Database connection error: {e}")
            self.connection = None  # Explicitly set connection to None on failure

    # Use this to connect to the database, create a table if it does not exist, and dump JSON files into this table
    def insert_table(self):
        if not self.connection:
            print("No active database connection. Unable to insert data.")
            return
        try:
            # Load and parse JSON data
            with open('/home/rafael-vieira/Desktop/CPV_Server/ExcelConversion/JsonFiles/JsonFiles.json') as file:
                data = json.load(file)
                data = date_type(data)  # Convert date fields to appropriate format
                print(data)

            with self.connection.cursor() as cursor:
                # Create the table if it does not exist
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS cpv_database_clients_Agosto (
                    "ID" SERIAL PRIMARY KEY,
                    "EMPRESA" VARCHAR(100),
                    "CIDADE" VARCHAR(60),
                    "ESTADO" VARCHAR(60),
                    "SITUACAO_DA_CONTA" VARCHAR(20),
                    "DATA_DE_INICIO_DO_CONTRATO" DATE,
                    "DATA_FINAL_DO_CONTRATO" DATE,
                    "CONTRATO_EM_MESES" INTEGER,
                    "DIAS_EM_CONTRATO" INTEGER,
                    "TIER" CHAR(2),
                    "FLAG" VARCHAR(50),
                    "HONORARIO" NUMERIC,
                    "LIFE_TIME_VALUE" NUMERIC,
                    "CUSTO_TOTAL" NUMERIC,
                    "ROI" DECIMAL(5,4),
                    "ROAS" DECIMAL(5,4),
                    "DATE_OF_INSERTION" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)

                # Insert data from JSON
                query_sql = """
                    INSERT INTO cpv_database_clients_Agosto(
                        "EMPRESA", "CIDADE", "ESTADO", "SITUACAO_DA_CONTA", 
                        "DATA_DE_INICIO_DO_CONTRATO", "DATA_FINAL_DO_CONTRATO", 
                        "CONTRATO_EM_MESES", "DIAS_EM_CONTRATO", 
                        "TIER", "FLAG", "HONORARIO", "LIFE_TIME_VALUE", 
                        "CUSTO_TOTAL", "ROI", "ROAS"
                    )
                    SELECT "EMPRESA", "CIDADE", "ESTADO", "SITUACAO_DA_CONTA", 
                        "DATA_DE_INICIO_DO_CONTRATO", "DATA_FINAL_DO_CONTRATO", 
                        "CONTRATO_EM_MESES", "DIAS_EM_CONTRATO", 
                        "TIER", "FLAG", "HONORARIO", "LIFE_TIME_VALUE", 
                        "CUSTO_TOTAL", "ROI", "ROAS"
                    FROM json_populate_recordset(NULL::cpv_database_clients_Agosto, %s)
                """
                cursor.execute(query_sql, (json.dumps(data),))
                self.connection.commit()
                print("Data inserted successfully.")

        except Exception as error:
            print(f"Error inserting data: {error}")
            # Rollback in case of error to keep the transaction clean
            if self.connection:
                self.connection.rollback()

    def close(self):
        if self.connection:
            self.connection.close()
            print("Connection closed.")
