# Configuration

## ZTFUtils Configuration

This section provides an overview of the configuration parameters used in the 
ZTF (Zwicky Transient Facility) ZTFUtils class. The configuration is specified 
in a JSON file and is used to customize various aspects of the data retrieval 
and storage.

### Configuration File: `raw.config.json`
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

### Configuration Parameters:

    1. `url_source_base` (string): The base URL for downloading ZTF public data.

    2. `url_source_file_prefix` (string): Prefix for ZTF public data files.

    3. `url_source_file_postfix` (string): Postfix for ZTF public data files.

    4. `files_base_folder` (string): Base folder where data files will be stored.

    5. `downloads_folder` (string): Subfolder for storing compressed Avro files.

    6. `untar_folder` (string): Subfolder for storing uncompressed Avro files.

    7. `parquet_folder` (string): Subfolder for storing Parquet files.

    8. `batch_size` (integer): Size of each batch for processing data.

    9. `compression` (string): Compression algorithm to be used (e.g., "snappy").

    10. `delete_data` (string): If "true," original data files will be deleted after processing.

    11. `overwrite_data` (string): If "true," existing data files will be overwritten.

    12. `start_date` (string): Start date for data processing (format given by date_format).

    13. `end_date` (string): End date for data processing (format given by date_format).

    14. `date_format` (string): Format of dates in the configuration (e.g., "%Y/%m/%d").

## ParquetReader Configuration

This section provides an overview of the configuration parameters used by the 
ParquetReader class. The configuration is specified in a JSON file and is used 
to customize the behavior of the ParquetReader when reading and processing 
Parquet files.

### Configuration File: `read.config.json`

```json
{
    "files_base_folder": "/home/alerce/batch_processing/data",
    "parquet_folder": "parquet",
    "start_date": "2023/12/26",
    "end_date": "2023/12/27",
    "date_format": "%Y/%m/%d"
}
```

### Configuration Parameters:

    1. `files_base_folder` (string): The base folder where Parquet files are located.

    2. `parquet_folder` (string): Subfolder within files_base_folder containing Parquet files.

    3. `start_date` (string): Start date for reading Parquet files (format: "YYYY/MM/DD").

    4. `end_date` (string): End date for reading Parquet files (format: "YYYY/MM/DD").

    5. `date_format` (string): Format of dates in the configuration (e.g., "%Y/%m/%d").