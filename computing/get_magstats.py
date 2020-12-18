import pandas as pd
import logging
import click
import os

from utils import monitor
from lc_correction.compute import *
from lc_correction.helpers import *


@click.command()
@click.argument("corrected_dir", type=str)
@click.argument("non_det_dir", type=str)
@click.argument("output_dir", type=str)
@click.argument("partition", type=int)
@click.argument("node_id", type=str)
@click.argument("job_id", type=str)
@click.argument("logs_dir", type=str, default=".")
@click.option(
    "-v", "--version", default="bulk_version_0.0.1", help="Version of correction"
)
@click.option(
    "--file-format",
    default="part-%s-d04f7f52-9773-4a13-a06b-55c7db613831-c000.snappy.parquet",
    help="Parquet file name format. Id number should be replaced with %s. Default is part-%s-d04f7f52-9773-4a13-a06b-55c7db613831-c000.snappy.parquet",
)
@click.option(
    "--format-times",
    default=1,
    help="Number of times the file_format has a string to be parsed. Example: 'my %s format %s' has value 2",
)
def main(
    corrected_dir,
    non_det_dir,
    output_dir,
    partition,
    node_id,
    job_id,
    logs_dir,
    version,
    file_format,
    format_times,
):
    """
    Correct a set of detections indexed by object id.

    DETECTIONS_DIR is the name of the directory with alert parquets. DETECTIONS_DIR can be a
    local directory or a URI. For example a S3 URI like s3a://ztf-historic-data/det

    OUTPUT_DIR is the name of the output directory for corrected detections parquet files.
    OUTPUT_DIR can be a local directory or a URI. For example a S3 URI like s3://ztf-historic-data/det_corrected

    PARTITION is a number of partition to be processed

    NODE_ID identifier of the node in leftraru

    JOB_ID identifier of job in slurm
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    tup = (str(partition).zfill(5),)
    if format_times > 1:
        for i in range(format_times):
            tup += tup

    try:
        detection_file = file_format % tup
    except TypeError as e:
        raise Exception("file_format and format_times not compatible")

    logging.info(f"Opening corrected detection file: {detection_file}")
    detections = pd.read_parquet(os.path.join(corrected_dir, detection_file))

    # Init monitor
    monitor(logs_dir, f"corrected_{partition}_{node_id}_{job_id}", log=True, plot=False)

    logging.info("Getting magnitude stats")
    magstats = detections.groupby(["objectId", "fid"]).apply(apply_mag_stats)
    magstats.reset_index(inplace=True)

    logging.info("Getting object table")
    objstats = apply_object_stats_df(detections, magstats)
    objstats.reset_index(inplace=True)
    del detections

    logging.info("Writting objstats")
    objstats.to_parquet(os.path.join(output_dir, f"object_{partition}.parquet"))
    del objstats

    files = sorted(os.listdir(non_det_dir))
    non_det_file = files[partition]
    non_det = pd.read_parquet(
        os.path.join(non_det_dir, non_det_file),
        columns=["objectId", "jd", "isdiffpos", "fid", "diffmaglim"],
    )
    non_det["mjd"] = non_det.jd - 2400000.5
    del non_det["jd"]

    logging.info("Doing dm/dt")
    dd = do_dmdt_df(magstats, non_det)
    magstats["step_id_corr"] = version

    logging.info("Joining magstats and dm/dt")
    magstats = magstats.join(dd, on=["objectId", "fid"])

    magstats.to_parquet(os.path.join(output_dir, f"magstats_{partition}.parquet"))


if __name__ == "__main__":
    main()
