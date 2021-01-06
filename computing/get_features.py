import pandas as pd
import numpy as np
import logging
import click
import os

from utils import monitor
from lc_classifier.features import CustomHierarchicalExtractor


@click.command()
@click.argument("corrected_dir", type=str)
@click.argument("objects_dir", type=str)
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
    default="part-{}-5d486975-47dd-4c9a-a90a-b142fda2c49e_{}.c000.snappy.parquet",
    help="Parquet file name format. Id number should be replaced with {}",
)
def main(
        corrected_dir,
        objects_dir,
        non_det_dir,
        output_dir,
        partition,
        node_id,
        job_id,
        logs_dir,
        version,
        file_format
):
    """
    Correct a set of detections indexed by object id.

    DETECTIONS_DIR is the name of the directory with alert parquets. DETECTIONS_DIR can be a
    local directory or a URI. For example a S3 URI like s3a://ztf-historic-data/det

    OBJECTS_DIR is the name of the directory with objects parquets. OBJECTS_DIR can be a
    local directory or a URI.

    OUTPUT_DIR is the name of the output directory for corrected detections parquet files.
    OUTPUT_DIR can be a local directory or a URI. For example a S3 URI like s3://ztf-historic-data/det_corrected

    PARTITION is a number of partition to be processed

    NODE_ID identifier of the node in leftraru

    JOB_ID identifier of job in slurm
    """
    # Init monitor
    monitor(logs_dir, f"corrected_{partition}_{node_id}_{job_id}", log=True, plot=False)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    detection_file = f"detections_corrected_{partition}.parquet"
    logging.info(f"Opening corrected detection file: {detection_file}")
    detections = pd.read_parquet(os.path.join(corrected_dir, detection_file))
    detections.set_index("objectId", inplace=True)
    detections.index.name = 'oid'
    #  detections["magpsf"] = detections["magpsf"].astype(np.float64)
    #  detections["sigmapsf"] = detections["sigmapsf"].astype(np.float64)

    objects_file = f"object_{partition}.parquet"
    logging.info(f"Opening object file: {objects_file}")
    objects = pd.read_parquet(os.path.join(objects_dir, objects_file))
    objects.set_index("objectId", inplace=True)
    objects.index.name = 'oid'

    rep = str(partition).zfill(5)
    non_det_file = file_format.replace("{}", rep)
    logging.info(f"Opening non_detections file: {non_det_file}")
    non_det = pd.read_parquet(
        os.path.join(non_det_dir, non_det_file),
        columns=["objectId", "jd", "isdiffpos", "fid", "diffmaglim"],
    )
    non_det["mjd"] = non_det.jd - 2400000.5
    del non_det["jd"]
    non_det.set_index("objectId", inplace=True)
    non_det.index.name = 'oid'

    logging.info(f"Computing features for: {objects.shape[0]} objects")
    extractor = CustomHierarchicalExtractor()
    features_df = extractor.compute_features(
        detections=detections,
        non_detections=non_det,
        objects=objects,
    )

    features_df.reset_index(inplace=True)
    logging.info(f"Writing output for: {features_df.shape[0]}")
    features_df.to_parquet(os.path.join(output_dir, f"features_{partition}.parquet"))


if __name__ == "__main__":
    main()
