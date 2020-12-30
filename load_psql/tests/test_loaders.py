from load_psql.loaders import (
    CSVLoader,
    DetectionsCSVLoader,
    ObjectsCSVLoader,
    NonDetectionsCSVLoader,
    SSCSVLoader,
    DataQualityCSVLoader,
    MagstatsCSVLoader,
    PS1CSVLoader,
    GaiaCSVLoader,
    ReferenceCSVLoader,
)
from load_psql.table_data import (
    DetectionTableData,
    ObjectTableData,
    NonDetectionTableData,
    SSTableData,
    DataQualityTableData,
    MagstatsTableData,
    PS1TableData,
    GaiaTableData,
    ReferenceTableData,
)
import unittest
from unittest import mock


class DetectionsCSVLoaderTest(unittest.TestCase):
    def test_create_table_data(self):
        loader = DetectionsCSVLoader(source="source", read_args={"ok": "ok"})
        self.assertEqual(loader.source, "source")
        self.assertEqual(loader.read_args["ok"], "ok")
        table_data = loader.create_table_data(
            spark_session=mock.MagicMock(), source="source", read_args={}
        )
        self.assertIsInstance(table_data, DetectionTableData)

    @mock.patch.object(DetectionTableData, "select")
    @mock.patch.object(DetectionTableData, "save")
    def test_save_csv(self, save, select):
        loader = DetectionsCSVLoader(source="source", read_args={"ok": "ok"})
        loader.save_csv(
            spark_session=mock.Mock(),
            output_path="test",
            tt_det=mock.MagicMock(),
            step_id="step_id",
            n_partitions=1,
            max_records_per_file=1,
            mode="mode",
        )
        select.assert_called_once()
        save.assert_called_once()


class ObjectsCSVLoaderTest(unittest.TestCase):
    def test_create_table_data(self):
        loader = ObjectsCSVLoader(source="source", read_args={"ok": "ok"})
        self.assertEqual(loader.source, "source")
        self.assertEqual(loader.read_args["ok"], "ok")
        table_data = loader.create_table_data(
            spark_session=mock.MagicMock(), source="source", read_args={}
        )
        self.assertIsInstance(table_data, ObjectTableData)

    @mock.patch.object(ObjectTableData, "select")
    @mock.patch.object(ObjectTableData, "save")
    def test_save_csv(self, save, select):
        loader = ObjectsCSVLoader(source="source", read_args={"ok": "ok"})
        loader.save_csv(
            spark_session=mock.Mock(),
            output_path="test",
            n_partitions=1,
            max_records_per_file=1,
            mode="mode",
        )
        select.assert_called_once()
        save.assert_called_once()


class NonDetectionsCSVLoaderTest(unittest.TestCase):
    def test_create_table_data(self):
        loader = NonDetectionsCSVLoader(source="source", read_args={"ok": "ok"})
        self.assertEqual(loader.source, "source")
        self.assertEqual(loader.read_args["ok"], "ok")
        table_data = loader.create_table_data(
            spark_session=mock.MagicMock(), source="source", read_args={}
        )
        self.assertIsInstance(table_data, NonDetectionTableData)

    @mock.patch.object(NonDetectionTableData, "select")
    @mock.patch.object(NonDetectionTableData, "save")
    def test_save_csv(self, save, select):
        loader = NonDetectionsCSVLoader(source="source", read_args={"ok": "ok"})
        loader.save_csv(
            spark_session=mock.Mock(),
            output_path="test",
            n_partitions=1,
            max_records_per_file=1,
            mode="mode",
        )
        select.assert_called_once()
        save.assert_called_once()


class DataQualityCSVLoaderTest(unittest.TestCase):
    def test_create_table_data(self):
        loader = DataQualityCSVLoader(source="source", read_args={"ok": "ok"})
        self.assertEqual(loader.source, "source")
        self.assertEqual(loader.read_args["ok"], "ok")
        table_data = loader.create_table_data(
            spark_session=mock.MagicMock(), source="source", read_args={}
        )
        self.assertIsInstance(table_data, DataQualityTableData)

    @mock.patch.object(DataQualityTableData, "select")
    @mock.patch.object(DataQualityTableData, "save")
    def test_save_csv(self, save, select):
        loader = DataQualityCSVLoader(source="source", read_args={"ok": "ok"})
        loader.save_csv(
            spark_session=mock.Mock(),
            output_path="test",
            n_partitions=1,
            max_records_per_file=1,
            mode="mode",
            tt_det=mock.MagicMock(),
        )
        select.assert_called_once()
        save.assert_called_once()


class MagstatsCSVLoaderTest(unittest.TestCase):
    def test_create_table_data(self):
        loader = MagstatsCSVLoader(source="source", read_args={"ok": "ok"})
        self.assertEqual(loader.source, "source")
        self.assertEqual(loader.read_args["ok"], "ok")
        table_data = loader.create_table_data(
            spark_session=mock.MagicMock(), source="source", read_args={}
        )
        self.assertIsInstance(table_data, MagstatsTableData)

    @mock.patch.object(MagstatsTableData, "select")
    @mock.patch.object(MagstatsTableData, "save")
    def test_save_csv(self, save, select):
        loader = MagstatsCSVLoader(source="source", read_args={"ok": "ok"})
        loader.save_csv(
            spark_session=mock.Mock(),
            output_path="test",
            n_partitions=1,
            max_records_per_file=1,
            mode="mode",
            tt_det=mock.MagicMock(),
        )
        select.assert_called_once()
        save.assert_called_once()


class PS1CSVLoaderTest(unittest.TestCase):
    def test_create_table_data(self):
        loader = PS1CSVLoader(source="source", read_args={"ok": "ok"})
        self.assertEqual(loader.source, "source")
        self.assertEqual(loader.read_args["ok"], "ok")
        table_data = loader.create_table_data(
            spark_session=mock.MagicMock(), source="source", read_args={}
        )
        self.assertIsInstance(table_data, PS1TableData)

    @mock.patch.object(PS1TableData, "select")
    @mock.patch.object(PS1TableData, "save")
    def test_save_csv(self, save, select):
        loader = PS1CSVLoader(source="source", read_args={"ok": "ok"})
        loader.save_csv(
            spark_session=mock.Mock(),
            output_path="test",
            n_partitions=1,
            max_records_per_file=1,
            mode="mode",
            tt_det=mock.MagicMock(),
        )
        select.assert_called_once()
        save.assert_called_once()


class GaiaCSVLoaderTest(unittest.TestCase):
    def test_create_table_data(self):
        loader = GaiaCSVLoader(source="source", read_args={"ok": "ok"})
        self.assertEqual(loader.source, "source")
        self.assertEqual(loader.read_args["ok"], "ok")
        table_data = loader.create_table_data(
            spark_session=mock.MagicMock(), source="source", read_args={}
        )
        self.assertIsInstance(table_data, GaiaTableData)

    @mock.patch.object(GaiaTableData, "select")
    @mock.patch.object(GaiaTableData, "save")
    def test_save_csv(self, save, select):
        loader = GaiaCSVLoader(source="source", read_args={"ok": "ok"})
        loader.save_csv(
            spark_session=mock.Mock(),
            output_path="test",
            n_partitions=1,
            max_records_per_file=1,
            mode="mode",
        )
        select.assert_called_once()
        save.assert_called_once()


class ReferenceCSVLoaderTest(unittest.TestCase):
    def test_create_table_data(self):
        loader = ReferenceCSVLoader(source="source", read_args={"ok": "ok"})
        self.assertEqual(loader.source, "source")
        self.assertEqual(loader.read_args["ok"], "ok")
        table_data = loader.create_table_data(
            spark_session=mock.MagicMock(), source="source", read_args={}
        )
        self.assertIsInstance(table_data, ReferenceTableData)

    @mock.patch.object(ReferenceTableData, "select")
    @mock.patch.object(ReferenceTableData, "save")
    def test_save_csv(self, save, select):
        loader = ReferenceCSVLoader(source="source", read_args={"ok": "ok"})
        loader.save_csv(
            spark_session=mock.Mock(),
            output_path="test",
            n_partitions=1,
            max_records_per_file=1,
            mode="mode",
        )
        select.assert_called_once()
        save.assert_called_once()
