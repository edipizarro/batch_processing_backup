# Batch Processing Module

The Batch Processing module is currently designed for handling large-scale data processing tasks, specifically focused on ZTF (Zwicky Transient Facility) data. This module includes classes for reading Parquet files (ParquetReader) and utility functions for various tasks related to ZTF data processing (ZTFUtils).

## ParquetReader Class

### Overview

The ParquetReader class facilitates the reading and processing of Parquet files. It is configured using a JSON file and provides methods to iterate through dataframes within a specified date range.

### Configuration

The configuration for the `ParquetReader` class is provided in a JSON file. 
Below is an example configuration file (`read.config.json`):

```json
{
    "files_base_folder": "/home/alerce/batch_processing/data",
    "parquet_folder": "parquet",
    "start_date": "2023/12/26",
    "end_date": "2023/12/27",
    "date_format": "%Y/%m/%d"
}
```

For more details on the configuration parameters and usage, refer to the Configuration README.

### Usage
Refer to the `read.py` example in the `/scripts` folder.

## ZTFUtils Class

### Overview

The ZTFUtils class provides utility functions for various tasks related to ZTF data processing, including downloading, untarring, and creating Parquet files.

### Configuration

The configuration for the `ZTFUtils` class is provided in a JSON file. Below is an example configuration file (`raw.config.json`):

```json
{
    "url_source_base": "https://ztf.uw.edu/alerts/public/",
    "url_source_file_prefix": "ztf_public_",
    "url_source_file_postfix": ".tar.gz",
    "files_base_folder": "/home/alerce/batch_processing/data",
    "downloads_folder": "compressed_avros",
    "untar_folder": "uncompressed_avros",
    "parquet_folder": "parquet",
    "batch_size": 2500,
    "compression": "snappy",
    "delete_data": "false",
    "overwrite_data": "false",
    "start_date": "2023/12/26",
    "end_date": "2023/12/27",
    "date_format": "%Y/%m/%d"
}
```

For more details on the configuration parameters and usage, refer to the Configuration README.

### Usage
Refer to the `prepare_ztf.py` example in the `/scripts` folder.

## Installation

`poetry install`