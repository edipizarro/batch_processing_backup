# Configuration

## ZTFCrawler Configuration

This section provides an overview of the configuration parameters used in the 
ZTF (Zwicky Transient Facility) ZTFCrawler class. The configuration is specified 
in a JSON file and is used to customize various aspects of the data retrieval 
and storage.

### Configuration File: `raw.config.json`
```json
{
    "UrlSourceBase": "https://ztf.uw.edu/alerts/public/",
    "UrlSourceFilePrefix": "ztf_public_",
    "UrlSourceFilePostfix": ".tar.gz",
    "DataFolder": "/home/alerce/batch_processing/data",
    "CompressedAvrosFolder": "compressed_avros",
    "UncompressedAvrosFolder": "uncompressed_avros",
    "RawParquetFolder": "parquet",
    "BatchSize": 2500,
    "ParquetCompression": "snappy",
    "DeleteData": "false",
    "OverwriteData": "false",
    "StartDate": "2023/12/26",
    "EndDate": "2023/12/27",
    "DateFormat": "%Y/%m/%d"
}
```

### Configuration Parameters:

    1. `UrlSourceBase` (string): The base URL for downloading ZTF public data.

    2. `UrlSourceFilePrefix` (string): Prefix for ZTF public data files.

    3. `UrlSourceFilePostfix` (string): Postfix for ZTF public data files.

    4. `DataFolder` (string): Base folder where data files will be stored.

    5. `CompressedAvrosFolder` (string): Subfolder for storing compressed Avro files.

    6. `UncompressedAvrosFolder` (string): Subfolder for storing uncompressed Avro files.

    7. `RawParquetFolder` (string): Subfolder for storing Parquet files.

    8. `BatchSize` (integer): Size of each batch for processing data.

    9. `ParquetCompression` (string): Compression algorithm to be used (e.g., "snappy").

    10. `DeleteData` (string): If "true," original data files will be deleted after processing.

    11. `OverwriteData` (string): If "true," existing data files will be overwritten.

    12. `StartDate` (string): Start date for data processing (format given by DateFormat).

    13. `EndDate` (string): End date for data processing (format given by DateFormat).

    14. `DateFormat` (string): Format of dates in the configuration (e.g., "%Y/%m/%d").

## ParquetReader Configuration

This section provides an overview of the configuration parameters used by the 
ParquetReader class. The configuration is specified in a JSON file and is used 
to customize the behavior of the ParquetReader when reading and processing 
Parquet files.

### Configuration File: `read.config.json`

```json
{
    "DataFolder": "/home/alerce/batch_processing/data",
    "RawParquetFolder": "parquet",
    "StartDate": "2023/12/26",
    "EndDate": "2023/12/27",
    "DateFormat": "%Y/%m/%d"
}
```

### Configuration Parameters:

    1. `DataFolder` (string): The base folder where Parquet files are located.

    2. `RawParquetFolder` (string): Subfolder within DataFolder containing Parquet files.

    3. `StartDate` (string): Start date for reading Parquet files (format: "YYYY/MM/DD").

    4. `EndDate` (string): End date for reading Parquet files (format: "YYYY/MM/DD").

    5. `DateFormat` (string): Format of dates in the configuration (e.g., "%Y/%m/%d").