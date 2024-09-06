import sqlite3
import psycopg2
from loguru import logger


def get_data_model(model_name):

    data_model_northwind = '''
        CREATE TABLE IF NOT EXISTS Categories (
            CategoryID SERIAL PRIMARY KEY,
            CategoryName VARCHAR(255) NOT NULL,
            Description TEXT
        );

        CREATE TABLE IF NOT EXISTS Customers (
            CustomerID VARCHAR(5) PRIMARY KEY,
            CompanyName VARCHAR(255) NOT NULL,
            ContactName VARCHAR(255),
            ContactTitle VARCHAR(255),
            Address VARCHAR(255),
            City VARCHAR(255),
            Region VARCHAR(255),
            PostalCode VARCHAR(255),
            Country VARCHAR(255),
            Phone VARCHAR(255),
            Fax VARCHAR(255)
        );

        CREATE TABLE IF NOT EXISTS Employees (
            EmployeeID SERIAL PRIMARY KEY,
            LastName VARCHAR(255) NOT NULL,
            FirstName VARCHAR(255) NOT NULL,
            Title VARCHAR(255),
            TitleOfCourtesy VARCHAR(255),
            BirthDate DATE,
            HireDate DATE,
            Address VARCHAR(255),
            City VARCHAR(255),
            Region VARCHAR(255),
            PostalCode VARCHAR(255),
            Country VARCHAR(255),
            HomePhone VARCHAR(255),
            Extension VARCHAR(255),
            Notes TEXT,
            ReportsTo INTEGER,
            FOREIGN KEY (ReportsTo) REFERENCES Employees (EmployeeID)
        );

        CREATE TABLE IF NOT EXISTS Shippers (
            ShipperID SERIAL PRIMARY KEY,
            CompanyName VARCHAR(255) NOT NULL,
            Phone VARCHAR(255)
        );

        CREATE TABLE IF NOT EXISTS Suppliers (
            SupplierID SERIAL PRIMARY KEY,
            CompanyName VARCHAR(255) NOT NULL,
            ContactName VARCHAR(255),
            ContactTitle VARCHAR(255),
            Address VARCHAR(255),
            City VARCHAR(255),
            Region VARCHAR(255),
            PostalCode VARCHAR(255),
            Country VARCHAR(255),
            Phone VARCHAR(255),
            Fax VARCHAR(255),
            HomePage TEXT
        );

        CREATE TABLE IF NOT EXISTS Products (
            ProductID SERIAL PRIMARY KEY,
            ProductName VARCHAR(255) NOT NULL,
            SupplierID INTEGER,
            CategoryID INTEGER,
            QuantityPerUnit VARCHAR(255),
            UnitPrice REAL,
            UnitsInStock INTEGER,
            UnitsOnOrder INTEGER,
            ReorderLevel INTEGER,
            Discontinued INTEGER NOT NULL,
            FOREIGN KEY (SupplierID) REFERENCES Suppliers (SupplierID),
            FOREIGN KEY (CategoryID) REFERENCES Categories (CategoryID)
        );

        CREATE TABLE IF NOT EXISTS Orders (
            OrderID SERIAL PRIMARY KEY,
            CustomerID VARCHAR(5),
            EmployeeID INTEGER,
            OrderDate DATE,
            RequiredDate DATE,
            ShippedDate DATE,
            ShipVia INTEGER,
            Freight REAL,
            ShipName VARCHAR(255),
            ShipAddress VARCHAR(255),
            ShipCity VARCHAR(255),
            ShipRegion VARCHAR(255),
            ShipPostalCode VARCHAR(255),
            ShipCountry VARCHAR(255),
            FOREIGN KEY (CustomerID) REFERENCES Customers (CustomerID),
            FOREIGN KEY (EmployeeID) REFERENCES Employees (EmployeeID),
            FOREIGN KEY (ShipVia) REFERENCES Shippers (ShipperID)
        );

        CREATE TABLE IF NOT EXISTS OrderDetails (
            OrderID INTEGER,
            ProductID INTEGER,
            UnitPrice REAL NOT NULL,
            Quantity INTEGER NOT NULL,
            Discount REAL NOT NULL,
            PRIMARY KEY (OrderID, ProductID),
            FOREIGN KEY (OrderID) REFERENCES Orders (OrderID),
            FOREIGN KEY (ProductID) REFERENCES Products (ProductID)
        );
        '''

    if model_name == 'northwind':
        logger.info("Retrieving Northwind data model...")
        return data_model_northwind


def create_physical_db(model_name, database_name):

    if database_name == 'sqlite':
        try:
            # Connect to the database (creates it if it doesn't exist)
            conn = sqlite3.connect('northwind.db')
            cursor = conn.cursor()

            # Create tables
            cursor.executescript(get_data_model(model_name))

            # Commit changes and close connection
            conn.commit()
            conn.close()

            logger.success("Northwind database created successfully in sqlite.")
        except Exception as e:
            logger.error(f"Error creating Northwind database: {str(e)}")
            raise

    elif database_name == 'postgresql':
        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="postgres",
                host="localhost"
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Create the northwind database
            cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'northwind'")
            exists = cursor.fetchone()
            if not exists:
                cursor.execute("CREATE DATABASE northwind")
                logger.info("Database 'northwind' created successfully.")
            else:
                logger.info("Database 'northwind' already exists.")

            # Connect to the northwind database
            conn.close()
            conn = psycopg2.connect(
                dbname="northwind",
                user="postgres",
                password="postgres",
                host="localhost"
            )
            cursor = conn.cursor()

            # Create tables
            logger.info("Creating tables...")
            cursor.execute(get_data_model(model_name))

            # Commit changes and close connection
            conn.commit()
            cursor.close()
            conn.close()

            logger.success("Northwind database created successfully in posgresql.")
        except Exception as e:
            logger.error(f"Error creating Northwind database: {str(e)}")
            raise


def list_tables(data_base_name):

    if data_base_name == 'sqlite':
        # Connect to the database
        conn = sqlite3.connect('northwind.db')
        cursor = conn.cursor()

        # Query to list all tables
        logger.info("Tables in the northwind database using sqlite:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # Print the table names

        for table in tables:
            print(table[0])

        # Close the connection
        conn.close()

    elif data_base_name == 'postgresql':
        try:
            # Connect to the northwind database
            conn = psycopg2.connect(
                dbname="northwind",
                user="postgres",
                password="postgres",
                host="localhost"
            )
            cursor = conn.cursor()
            logger.info("Tables in the northwind database using postgresql:")

            # Query to list all tables
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
            tables = cursor.fetchall()

            # Print the table names

            for table in tables:
                print(table[0])

            # Close the connection
            conn.close()
        except Exception as e:
            logger.error(f"Error listing tables: {str(e)}")
            raise


if __name__ == "__main__":

    create_physical_db('northwind', 'postgresql')
    #create_physical_db('northwind', 'sqlite')
    #list_tables(data_base_name='sqlite')
    list_tables('postgresql')
