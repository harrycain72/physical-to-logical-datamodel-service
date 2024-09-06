from sqlalchemy import create_engine, inspect


def get_ddl_statements(db_url):
    engine = create_engine(db_url)
    inspector = inspect(engine)

    ddl_statements = []

    for table_name in inspector.get_table_names():
        ddl = f"CREATE TABLE {table_name} (\n"
        columns = inspector.get_columns(table_name)
        primary_keys = inspector.get_pk_constraint(table_name)['constrained_columns']

        column_definitions = []
        for column in columns:
            col_def = f"  {column['name']} {column['type']}"
            if column['nullable'] is False:
                col_def += " NOT NULL"
            if column['name'] in primary_keys:
                col_def += " PRIMARY KEY"
            column_definitions.append(col_def)

        ddl += ",\n".join(column_definitions)
        ddl += "\n);"
        ddl_statements.append(ddl)

    return "\n\n".join(ddl_statements)


if __name__ == "__main__":

    db_url = "postgresql://postgres:postgres@localhost/northwind"
    ddl_statements = get_ddl_statements(db_url)
    print(ddl_statements)
