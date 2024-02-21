import logging
import click
import iodf
import os


@click.command()
@click.argument("input_dir", type=str)
@click.argument("output_dir", type=str)
@click.argument("partition", type=int)
@click.argument("node_id", type=str)
@click.argument("job_id", type=str)
@click.argument("pattern", type=str)
@click.option(
    "-f", "--file-format", default="parquet", help="Format of output file"
)
def upload_file(
    input_dir,
    output_dir,
    partition,
    node_id,
    job_id,
    pattern,
    file_format
):
    """
    Upload a file to S3 or move to another directory.

    INPUT_DIR is the name of the directory with parquets. DETECTIONS_DIR can be a
    local directory or a URI. For example a S3 URI like s3a://ztf-historic-data/det

    OUTPUT_DIR is the name of the output directory for corrected detections parquet files.
    OUTPUT_DIR can be a local directory or a URI. For example a S3 URI like s3://ztf-historic-data/det_corrected

    PARTITION is a number of partition to be processed

    NODE_ID identifier of the node in leftraru

    JOB_ID identifier of job in slurm

    PATTERN is the prefix of the file. For example "magstats"
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(f"Job: {job_id} | Node: {node_id} | Partition {partition}")

    filename = f"{pattern}_{partition}.{file_format}"
    file_path = os.path.join(input_dir, filename)
    output_path = os.path.join(output_dir, filename)

    logging.info(f"Opening detection file: {file_path}")
    data = iodf.read_file(file_path)

    logging.info(f"Writing output in {output_path}")
    iodf.write_file(data, output_path, file_format=file_format)
    logging.info(f"Finish him!")
    return


if __name__ == "__main__":
    upload_file()
