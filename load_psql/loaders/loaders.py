from __future__ import annotations
from abc import ABC, abstractmethod
from load_psql.table_data import (
    DetectionTableData,
    ObjectTableData,
    NonDetectionTableData,
    SSTableData,
)
from pyspark.sql import SparkSession, DataFrame


class CSVLoader(ABC):
    def __init__(self, source: str, read_args: dict):
        self.source = source
        self.read_args = read_args

    @abstractmethod
    def create_table_data(self, spark_session, source: str, read_args: dict):
        pass

    def save_csv(
        self,
        spark_session: SparkSession,
        output_path: str,
        n_partitions: int,
        max_records_per_file: int,
        mode: str,
        *args,
        **kwargs
    ) -> None:
        tabledata = self.create_table_data(spark_session, self.source, self.read_args)
        selected_data = tabledata.select(*args, **kwargs)
        tabledata.save(
            output_dir=output_path,
            selected=selected_data,
            n_partitions=n_partitions,
            max_records_per_file=max_records_per_file,
            mode=mode,
            *args,
            **kwargs
        )

    def psql_load_csv(self) -> None:
        pass


class DetectionsCSVLoader(CSVLoader):
    def create_table_data(
        self, spark_session: SparkSession, source: str, read_args: dict
    ) -> DetectionTableData:
        return DetectionTableData(spark_session, source=source, read_args=read_args)

    # Override method to return tt_det used in other loaders
    def save_csv(
        self,
        spark_session: SparkSession,
        output_path: str,
        n_partitions: int,
        max_records_per_file: int,
        mode: str,
        *args,
        **kwargs
    ) -> DataFrame:
        tabledata = self.create_table_data(spark_session, self.source, self.read_args)
        selected_data, tt_det = tabledata.select(*args, **kwargs)
        tabledata.save(
            output_dir=output_path,
            selected=selected_data,
            n_partitions=n_partitions,
            max_records_per_file=max_records_per_file,
            mode=mode,
            *args,
            **kwargs
        )
        return tt_det


class ObjectsCSVLoader(CSVLoader):
    def create_table_data(
        self, spark_session: SparkSession, source: str, read_args: dict
    ):
        return ObjectTableData(spark_session, source, read_args)


class NonDetectionsCSVLoader(CSVLoader):
    def create_table_data(
        self, spark_session: sparksession, source: str, read_args: dict
    ):
        return NonDetectionTableData(spark_session, source, read_args)


class SSCSVLoader(CSVLoader):
    def create_table_data(
        self, spark_session: sparksession, source: str, read_args: dict
    ):
        return SSTableData(spark_session, source, read_args)
