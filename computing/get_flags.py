import click
import logging
import dask.dataframe as dd


def compute_flags(objects_path, magstats_path):
    objects = dd.read_parquet(objects_path, columns=["objectId", "diffpos", "reference_change"])
    objects = objects.compute()
    objects.columns = ["objectId", "flag_diffpos", "flag_reference_change"]
    objects.set_index("objectId", drop=True, inplace=True)

    logging.info(f"Opening {magstats_path}")
    magstats = dd.read_parquet(magstats_path, columns=["objectId", "fid", "corrected", "ndubious", "saturation_rate"])
    magstats = magstats[magstats.fid != 3]
    magstats["fid"] = magstats["fid"].map(lambda x: "g" if x == 1 else "r")
    magstats = magstats.compute()

    logging.info(f"Getting flags for {objects.shape[0]} objects")
    flags = magstats.pivot_table(values=["corrected", "ndubious", "saturation_rate"],
                                 index="objectId",
                                 columns=["fid"])
    flags.columns = flags.columns.map('flag_{0[0]}_{0[1]}'.format)
    flags.reset_index(inplace=True)
    flags = flags.join(objects, on="objectId")
    return flags


@click.command()
@click.argument("magstats_path", type=str)
@click.argument("objects_path", type=str)
@click.argument("output_path", type=str)
@click.option(
    "--log",
    "loglevel",
    default="INFO",
    help="log level to use",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
def get_flags(magstats_path, objects_path, output_path, loglevel):
    """
    MAGSTATS_PATH
    OBJECTS_PATH
    OUTPUT_PATH
    """
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: %s" % loglevel)
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    logging.info(f"Opening {objects_path}")
    flags = compute_flags(objects_path, magstats_path)
    logging.info(f"Writing file {output_path}")
    flags.to_parquet(output_path)
    return


if __name__ == "__main__":
    get_flags()

