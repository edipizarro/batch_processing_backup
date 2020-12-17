import click
from partition_avro.partition_dets_ndets import partition_dets_ndets
from partition_avro.postprocess_create_csv import create_csv
from partition_avro.psql_load import load_csv


@click.group()
def cli():
    pass


if __name__ == "__main__":
    cli.add_command(partition_dets_ndets)
    cli.add_command(create_csv)
    cli.add_command(load_csv)
    cli()
