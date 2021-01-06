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
    "xmatch": True,
    "probability": True,
    "feature": True,
}
sources = {
    "detection": "",
    "non_detection": "",
    "object": "",
    "magstats": "",
    "ps1": "",
    "ss": "",
    "reference": "",
    "dataquality": "",
    "xmatch": "",
    "probability": "",
    "feature": "",
}
outputs = {
    "detection": "",
    "non_detection": "",
    "object": "",
    "magstats": "",
    "ps1": "",
    "ss": "",
    "reference": "",
    "dataquality": "",
    "xmatch": "",
    "probability": "",
    "feature": "",
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
