import sqlite3
from loguru import logger


def create_northwind_db():
    # Connect to the database (creates it if it doesn't exist)
    conn = sqlite3.connect('northwind.db')
    cursor = conn.cursor()

    # Create tables
    cursor.executescript('''
    -- Categories table
    CREATE TABLE IF NOT EXISTS Categories (
        CategoryID INTEGER PRIMARY KEY,
        CategoryName TEXT NOT NULL,
        Description TEXT
    );

    -- Customers table
    CREATE TABLE IF NOT EXISTS Customers (
        CustomerID TEXT PRIMARY KEY,
        CompanyName TEXT NOT NULL,
        ContactName TEXT,
        ContactTitle TEXT,
        Address TEXT,
        City TEXT,
        Region TEXT,
        PostalCode TEXT,
        Country TEXT,
        Phone TEXT,
        Fax TEXT
    );

    -- Employees table
    CREATE TABLE IF NOT EXISTS Employees (
        EmployeeID INTEGER PRIMARY KEY,
        LastName TEXT NOT NULL,
        FirstName TEXT NOT NULL,
        Title TEXT,
        TitleOfCourtesy TEXT,
        BirthDate TEXT,
        HireDate TEXT,
        Address TEXT,
        City TEXT,
        Region TEXT,
        PostalCode TEXT,
        Country TEXT,
        HomePhone TEXT,
        Extension TEXT,
        Notes TEXT,
        ReportsTo INTEGER,
        FOREIGN KEY (ReportsTo) REFERENCES Employees (EmployeeID)
    );

    -- OrderDetails table
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

    -- Orders table
    CREATE TABLE IF NOT EXISTS Orders (
        OrderID INTEGER PRIMARY KEY,
        CustomerID TEXT,
        EmployeeID INTEGER,
        OrderDate TEXT,
        RequiredDate TEXT,
        ShippedDate TEXT,
        ShipVia INTEGER,
        Freight REAL,
        ShipName TEXT,
        ShipAddress TEXT,
        ShipCity TEXT,
        ShipRegion TEXT,
        ShipPostalCode TEXT,
        ShipCountry TEXT,
        FOREIGN KEY (CustomerID) REFERENCES Customers (CustomerID),
        FOREIGN KEY (EmployeeID) REFERENCES Employees (EmployeeID),
        FOREIGN KEY (ShipVia) REFERENCES Shippers (ShipperID)
    );

    -- Products table
    CREATE TABLE IF NOT EXISTS Products (
        ProductID INTEGER PRIMARY KEY,
        ProductName TEXT NOT NULL,
        SupplierID INTEGER,
        CategoryID INTEGER,
        QuantityPerUnit TEXT,
        UnitPrice REAL,
        UnitsInStock INTEGER,
        UnitsOnOrder INTEGER,
        ReorderLevel INTEGER,
        Discontinued INTEGER NOT NULL,
        FOREIGN KEY (SupplierID) REFERENCES Suppliers (SupplierID),
        FOREIGN KEY (CategoryID) REFERENCES Categories (CategoryID)
    );

    -- Shippers table
    CREATE TABLE IF NOT EXISTS Shippers (
        ShipperID INTEGER PRIMARY KEY,
        CompanyName TEXT NOT NULL,
        Phone TEXT
    );

    -- Suppliers table
    CREATE TABLE IF NOT EXISTS Suppliers (
        SupplierID INTEGER PRIMARY KEY,
        CompanyName TEXT NOT NULL,
        ContactName TEXT,
        ContactTitle TEXT,
        Address TEXT,
        City TEXT,
        Region TEXT,
        PostalCode TEXT,
        Country TEXT,
        Phone TEXT,
        Fax TEXT,
        HomePage TEXT
    );
    ''')

    # Commit changes and close connection
    conn.commit()
    conn.close()

    print("Northwind database created successfully.")


def list_tables():
    # Connect to the database
    conn = sqlite3.connect('northwind.db')
    cursor = conn.cursor()

    # Query to list all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Print the table names
    for table in tables:
        print(table[0])

    # Close the connection
    conn.close()


if __name__ == "__main__":
    create_northwind_db()
    list_tables()
