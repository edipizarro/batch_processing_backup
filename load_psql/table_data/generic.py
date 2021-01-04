from __future__ import annotations
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession


class TableData(ABC):
    def __init__(
        self, spark_session: SparkSession, source: str, read_args: dict
    ) -> TableData:
        if len(source):
            self.dataframe = spark_session.read.load(source, **read_args)
        else:
            self.dataframe = None

    @abstractmethod
    def select(self) -> DataFrame:
        pass

    @abstractmethod
    def save(self, output_path: str, selected_data: DataFrame = None) -> None:
        pass
