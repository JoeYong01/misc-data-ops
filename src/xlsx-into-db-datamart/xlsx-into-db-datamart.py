import pandas as pd
from sqlalchemy import create_engine, text
from enum import Enum


class DatabaseType(Enum):
    """Enum type used in create_datamart()"""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    MSSQL = "mssql"


CONNECTION_FORMATS = {
    DatabaseType.MYSQL: "mysql+pymysql://{user}:{password}@{host}/{schema}",
    DatabaseType.POSTGRESQL: "postgresql://{user}:{password}@{host}/{schema}",
    DatabaseType.MSSQL: "mssql+pyodbc://{user}:{password}@{host}/{schema}"
}

CREATE_SCHEMA_SQL = (
    "CREATE SCHEMA IF NOT EXISTS {datamart_name} "
    "DEFAULT CHARSET utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci"
    )


def read_xlsx(xlsx: str, sheet_name: str, index: str) -> pd.DataFrame:
    """
    reads an table from an xlsx from a given sheet

    Args:
        xlsx (str): xlsx name (not path) without extension
        sheet_name (str): sheet name of the table of interest
        index (str): the name of the column that goes 1,2,3

    Returns:
        pd.DataFrame
    """
    df = pd.read_excel(
        f"src/xlsx-into-db-datamart/place-xlsx-here/{xlsx}.xlsx",
        sheet_name = sheet_name,
        index_col = index)
    return df


def create_datamart(
    db_type: DatabaseType,
    db_user: str,
    db_password: str,
    db_host: str,
    db_schema: str,
    datamart_name: str,
    source_table: pd.DataFrame
) -> None:
    """
    reads a given table & create a datamart (a new schema)

    Args:
        db_type (str): type of database from DatabaseType class
        db_user (str): database user
        db_password (str): database user password
        db_host (str): database host
        db_schema (str): schema
        datamart_name (str): database to create
        source_table (pd.DataFrame): the source table to read from
    """
    if db_type not in CONNECTION_FORMATS:
        raise ValueError(
            "Unsupported database type. "
            "Choose from DatabaseType.[MYSQL/POSTGRESQL/MSSQL]."
            )
    db_url = CONNECTION_FORMATS[db_type].format(
        user = db_user,
        password = db_password,
        host = db_host,
        schema = db_schema
    )
    engine = create_engine(db_url)
    with engine.connect() as con:
        try:
            sql = CREATE_SCHEMA_SQL.format(datamart_name=datamart_name)
            con.execute(text(sql))
        except Exception as e:
            print(f"an error occured: {e}.")

    source_table.to_sql(
        "your table name",
        engine,
        schema = datamart_name,
        if_exists = 'append'
    )


def main():
    table = read_xlsx("your spreadsheet", "your sheet", "your index column")
    create_datamart(
        DatabaseType.MYSQL,
        db_user = "",
        db_password = "",
        db_host = "",        
        db_schema = "",
        datamart_name = "",
        source_table = table
    )


if __name__ == "__main__":
    main()
