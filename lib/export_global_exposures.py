import sqlalchemy
from sqlalchemy import text
from sqlalchemy import create_engine
import urllib

DRIVERNAME = "mssql+pyodbc"
HOST_RMS = r"pr0503-14002-00\\LMRMSINSURANCE"
HOST_GROUPUW = r"prod-groupuw-db"
RMS_MODELLING_DB = "Group_Rollup_Hisco_Modelling"
ODBC_DRIVER = "ODBC Driver 18 for SQL Server"
Encrypt = "yes"
TrustServerCertificate = "yes"
authentication = "ActiveDirectoryIntegrated"


def odbc_str(server: str, database: str):
    return (
        "Driver=ODBC Driver 18 for SQL Server;"
        f"Server={server};"
        f"Database={database};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Authentication=ActiveDirectoryIntegrated;"
    )


def create_constr(odbc_str: str, driver_name: str):
    params = urllib.parse.quote_plus(odbc_str)
    return f"{DRIVERNAME}:///?odbc_connect={params}"


query = text(
    f"""
    select eventid, wkt from data.sql_shapefiles
    where eventid = 106
    """
)

engine1 = create_engine(
    create_constr(odbc_str(HOST_GROUPUW, "GlobalExposures"), driver_name=DRIVERNAME)
)

with engine.connect() as con:
    result = con.execute(query)
    result = result.fetchall()

engine2 = create_engine(
    create_constr(odbc_str(HOST_RMS, RMS_MODELLING_DB), driver_name=DRIVERNAME)
)

# result will be a list of tuples e.g. [(1, varcharstring)]
with engine.connect() as con2:
    insert_sql = text(
        """
        insert into [HISCOX\\hamptonr].shapefiles
        (eventid, wkt)
        values
        (:eventid, :wkt)
         """
    )
    params = [{"eventid": r[0], "wkt": r[1]} for r in result]
    con2.execute(insert_sql, params)
