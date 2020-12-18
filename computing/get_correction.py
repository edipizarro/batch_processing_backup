import pandas as pd
import logging
import click
import os

from utils import monitor
from lc_correction.compute import *
from lc_correction.helpers import *


@click.command()
@click.argument("detections_dir", type=str)
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
    default="part-{}-5d486975-47dd-4c9a-a90a-b142fda2c49e_{}.c000.snappy.parquet",
    help="Parquet file name format. Id number should be replaced with {}. Default is part-{}-5d486975-47dd-4c9a-a90a-b142fda2c49e_{}.c000.snappy.parquet",
)
def get_correction(
    detections_dir,
    output_dir,
    partition,
    node_id,
    job_id,
    logs_dir,
    version,
    file_format,
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

    rep = str(partition).zfill(5)
    detection_file = file_format.replace("{}", rep)

    logging.info(f"Opening detection file: {detection_file}")
    detections = pd.read_parquet(os.path.join(detections_dir, detection_file))

    # Init monitor
    monitor(logs_dir, f"corrected_{partition}_{node_id}_{job_id}", log=True, plot=False)

    logging.info("Converting jd to mjd")
    detections["mjd"] = detections.jd - 2400000.5
    del detections["jd"]

    logging.info("Grouping objects and doing correction")
    correction_df = detections.groupby(["objectId", "fid"]).apply(apply_correction_df)
    correction_df.reset_index(inplace=True)
    del detections

    correction_df["step_id_corr"] = version
    correction_df["has_stamp"] = correction_df["parent_candid"] == 0

    logging.info(f"Writing output in {output_dir}")
    if not os.path.exists(os.path.join(output_dir)):
        os.mkdir(output_dir)
    output_name = os.path.join(output_dir, f"detections_corrected_{partition}.parquet")

    correction_df.to_parquet(output_name)


if __name__ == "__main__":
    get_correction()
