import os

## CONFIGURE DATABASE CONNECTION CREDENTIALS
## VALID KEYS ARE PSYCOPG2 CONNECTION ARGUMENTS
db = {
    "dbname": os.getenv("DBNAME"),
    "user": os.getenv("USER"),
    "password": os.getenv("PASSWORD"),
    "host": os.getenv("HOST"),
    "port": int(os.getenv("PORT")),
}
## CONFIGURE TABLES THAT WILL BE PROCESSED
## SET TRUE OR FALSE
tables = {
    "detection": True,
    "non_detection": False,
    "object": False,
    "magstats": False,
    "ps1_ztf": False,
    "ss_ztf": False,
    "reference": False,
    "dataquality": False,
    "xmatch": False,
    "probability": False,
    "feature": False,
    "gaia_ztf": False,
}
## SET SOURCE LOCATION OF PARQUET FILES
## FOR S3 THE s3a URI CAN BE USED
sources = {
    "raw_detection": "",
    "detection": "",
    "non_detection": "",
    "object": "",
    "magstats": "",
    "ps1_ztf": "",
    "ss_ztf": "",  # can be empty
    "reference": "",  # can be empty
    "dataquality": "",  # can be empty
    "gaia_ztf": "",  # can be empty
    "xmatch": "",
    "probability": "",
    "feature": "",
}
## SET OUTPUT LOCATION FOR CSV FILES
## LOCAL DIRECTORY OR s3 URI CAN BE USED
outputs = {
    "detection": "",
    "non_detection": "",
    "object": "",
    "magstats": "",
    "ps1_ztf": "",
    "ss_ztf": "",
    "reference": "",
    "dataquality": "",
    "xmatch": "",
    "probability": "",
    "feature": "",
    "gaia_ztf": "",
}
## ARGUMENTS FOR SAVING CSV FILES WITH SPARK WRITER
csv_loader_config = {
    "n_partitions": 16,
    "max_records_per_file": 100000,
    "mode": "overwrite",
}
## THIS IS THE CONFIG THAT WILL BE READ BY THE MAIN APP
load_config = {
    "db": db,
    "tables": tables,
    "sources": sources,
    "outputs": outputs,
    "csv_loader_config": csv_loader_config,
}
