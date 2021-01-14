# Batch Processing

Batch processing of ALeRCE ZTF historic data has three phases.

1. Partition all the raw avro files stored in AWS S3 into parquet files. This is done with Spark in an Amazon ECR Cluster.
2. Use those partitioned files to process corrected lightcurves, magnitude statistics, features and classifications. This is usually done in a specialized high computing cluster like Leftraru and the results are stored back in S3.
3. Create CSV files from the processed data. Each CSV file corresponds to a table in the database and is then uploaded using psql COPY command.

The three stages are executed separately but a more automated pipeline is being developed using Apache Airflow. So far the first step is run with apache airflow wich makes it easy to create the ECR cluster and execute the task of partition. Eventually al stages will be run with Airflow.


## Phase 1: Partition data
The first step takes raw avro files stored in S3 and creates `n` parquet files where `n` is the number of partitions. This process is executed in an Amazon EMR cluster and can be launched with the Airflow web server.

### Configuration

### Launch Task

## Phase 2: Process data

## Phase 3: Copy data to database

## The Batch Processing Package
Every step of the batch processing flow is integrated in a CLI that has a short description for each command and its parameters.

The CLI is accesed through the `main.py` script. Execute `python main.py --help` to get preview of the available commands. `python main.py COMMAND --help` shows the same but with the available options for that specific command.

### Package structure

#### partition_avro
Contains the script `partition-dets-ndets` that executes the partition of raw files.
Partititon detection and non detections into parquet files. The following arguments can be passed:

- SOURCE_AVROS is the name of the directory with alert avros. SOURCE_AVROS can be a local directory or a URI. For example a S3 URI like s3a://ztf-historic-data/object

- OUTPUT_DETECTIONS is the name of the output directory for detections parquet files. OUTPUT_DETECTIONS can be a local directory or a URI. For example a S3 URI like s3://ztf-historic-data/detections

- OUTPUT_NON_DETECTIONS is the name of the output directory for non detections parquet files. OUTPUT_NON_DETECTIONS can be a local directory or a URI. For example a S3 URI like s3a://ztf-historic-data/detections

In this script also lies the ztf avro schema used to open the raw avro files.

#### computing 
Contains the `slurm` scripts for executing tasks in `Leftraru` and the scripts for performing light curve correction, feature computation, etc.

#### load_psql
The load_psql module contains the `postprocess_create_csv.py` script that adds three commands to the main CLI.

- `process-csv`: creates csv files from the pre-computed results in parquet and copies the csv files to a PSQL database.
  Arguments: 
  - CONFIG_FILE: a path to a valid config.py file
- `create-csv`: creates csv files from the pre-computed results in parquet.
    Arguments: 
  - CONFIG_FILE: a path to a valid config.py file
- `psql-copy-csv`: uploads csv file to PSQL database
    Arguments: 
  - CONFIG_FILE: a path to a valid config.py file

#### airflow

## Airflow Deployment
