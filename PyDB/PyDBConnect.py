import pyodbc
import pandas as pd
import xmltodict


def connect_to_db(**kwargs):
    conn: pyodbc.Connection = pyodbc.connect(
        f"DRIVER={kwargs.get('db_driver')}" +
        f";SERVER={kwargs.get('server')}" +
        f";DATABASE={kwargs.get('database')}" +
        f";Trusted_Connection={kwargs.get('trusted_connection')}" +
        f";UID={kwargs.get('username')}" +
        f";PWD={kwargs.get('password')}")

    cursor: pyodbc.Cursor = conn.cursor()

    return cursor


def db_to_dataframe(cursor: pyodbc.Cursor, select_query: str, **kwargs) -> pd.DataFrame:
    records = cursor.execute(select_query).fetchall()
    columns = [c[0] for c in cursor.description]

    return pd.DataFrame.from_records(data=records, columns=columns)


def post_data_to_db(cursor: pyodbc.Cursor, df: pd.DataFrame, insert_query: str, table_name: str) -> None:
    # Truncating the table
    cursor.execute(f"TRUNCATE TABLE {table_name}")

    # Convert the dataframe to Recordset
    records = df.values.tolist()

    cursor.executemany(insert_query, records)
    cursor.commit()

    return None

# # Just having this here for testing purposes
# with open("args.xml", "r") as f:
#     main_kwargs = dict(xmltodict.parse(f.read())["args"])
# cursor = connect_to_db(**main_kwargs)
#
# select_query = "SELECT b.ClientIDSrc AS [ClientID], c.FirstName, c.Surname, c.DateOfBirth, h.City, h.County, h.Country, b.BookingNo, b.BookingValue, b.BookingMarginValue, b.BookingStatus, b.ConfirmedOn, b.Adults, b.Children, b.GroupType, b.HolidayType, b.NoOfNights " \
#                "FROM [DWHReporting].[fact].[Bookings] b " \
#                "LEFT JOIN [DWHReporting].[dim].[Client] c " \
#                "ON b.ClientIDSrc = c.ClientIDSrc " \
#                "LEFT JOIN [DWHReporting].[dim].[Household] h " \
#                "ON h.HouseholdIDSrc = c.HouseholdIDSrc " \
#                "WHERE b.DB_VersionStatus = 'CUR' AND c.DB_VersionStatus = 'CUR' AND b.ClientIDSrc <> '' " \
#                "AND b.ConfirmedOn > 2000 AND b.GroupType IS NOT NULL AND c.DateOfBirth IS NOT NULL"
#
# df = db_to_dataframe(cursor=cursor, select_query=select_query)
# print(df.head())
