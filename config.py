db = {}
tables = {
    "detection": True,
    "non_detection": True,
    "object": True,
    "magstats": True,
    "ps1": True,
    "ss": True,
    "reference": True,
    "dataquality": True,
    "xmatch": False,
    "probability": False,
    "feature": False,
}
sources = {
    "raw_detection": "s3a://ztf-historic-data/20201228/detections/part-00000-d27a5ff4-b27d-4ccf-9ccc-d7838431dc4b_00000.c000.snappy.parquet",
    "detection": "s3a://ztf-historic-data/20201228/corrected_detections/detections_corrected_0.parquet",
    "non_detection": "s3a://ztf-historic-data/20201228/non_detections/part-00000-54615e63-9fa3-49ea-8077-c900ef281e26_00000.c000.snappy.parquet",
    "object": "s3a://ztf-historic-data/20201228/objects/object_0.parquet",
    "magstats": "s3a://ztf-historic-data/20201228/magstats/magstats_0.parquet",
    "ps1": "s3a://ztf-historic-data/20201228/corrected_detections/detections_corrected_0.parquet",
    "ss": "",  # can be empty
    "reference": "",  # can be empty
    "dataquality": "",  # can be empty
    "xmatch": "",
    "probability": "",
    "feature": "",
}
outputs = {
    "detection": "s3a://ztf-historic-data/atest/detections",
    "non_detection": "s3a://ztf-historic-data/atest/non_detections",
    "object": "s3a://ztf-historic-data/atest/objects",
    "magstats": "s3a://ztf-historic-data/atest/magstats",
    "ps1": "s3a://ztf-historic-data/atest/ps1",
    "ss": "s3a://ztf-historic-data/atest/ss",
    "reference": "s3a://ztf-historic-data/atest/reference",
    "dataquality": "s3a://ztf-historic-data/atest/dataquality",
    "xmatch": "s3a://ztf-historic-data/atest/xmatch",
    "probability": "s3a://ztf-historic-data/atest/prob",
    "feature": "s3a://ztf-historic-data/atest/feature",
}
csv_loader_config = {
    "n_partitions": 16,
    "max_records_per_file": 100000,
    "mode": "overwrite",
}
load_config = {
    "db": db,
    "tables": tables,
    "sources": sources,
    "outputs": outputs,
    "csv_loader_config": csv_loader_config,
}
