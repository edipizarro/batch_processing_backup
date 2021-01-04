import click
from partition_avro.partition_dets_ndets import partition_dets_ndets
from load_psql.postprocess_create_csv_main import process_csv


@click.group()
def cli():
    pass


if __name__ == "__main__":
    cli.add_command(partition_dets_ndets)
    cli.add_command(process_csv)
    cli()
