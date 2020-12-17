import click
import os
import time
import psycopg2
import glob
from multiprocessing import Pool, cpu_count
from pathlib import Path
import logging


def get_db_credentials():
    credentials = {
        "database": os.environ["DB_NAME"],
        "user": os.environ["DB_USER"],
        "password": os.environ["DB_PASSWORD"],
        "host": os.environ["DB_HOST"],
        "port": os.environ["DB_PORT"],
    }
    return credentials


def execute_copy(file, con):
    logging.info(f"Copying {file}")
    fileName = open(file)
    tablename = Path(fileName).stem
    cursor = con.cursor()
    cursor.copy_from(fileName, tablename, sep=",", null="")
    con.commit()
    con.close()
    fileName.close()


@click.command()
@click.argument("CSV_PATH")
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def load_csv(csv_path, loglevel):
    # Set logging level
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % loglevel)
    logging.basicConfig(filename="fill_missing_fields.log", level=numeric_level)

    start = time.time()

    names = glob.glob(csv_path + "/*")
    config = get_db_credentials()
    logging.info("Connecting to database")
    con = psycopg2.connect(
        config["database"],
        config["user"],
        config["password"],
        config["host"],
        config["port"],
    )
    logging.info(f"Starting pool of {cpu_count()} processes")
    with Pool(cpu_count()) as p:
        print(p.map(execute_copy, [[names, con]]))

    total = time.time() - start
    logging.info("TOTAL_TIME=%s" % (str(total)))


if __name__ == "__main__":
    load_csv()
