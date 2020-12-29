from load_psql.loaders import (
    CSVLoader,
    DetectionsCSVLoader,
    ObjectsCSVLoader,
    NonDetectionsCSVLoader,
)
from load_psql.table_data import (
    DetectionTableData,
    ObjectTableData,
    NonDetectionTableData,
)
import unittest
from unittest import mock


class DetectionsCSVLoaderTest(unittest.TestCase):
    def test_create_table_data(self):
        det_loader = DetectionsCSVLoader(source="source", read_args={"ok": "ok"})
        self.assertEqual(det_loader.source, "source")
        self.assertEqual(det_loader.read_args["ok"], "ok")

    @mock.patch.object(DetectionTableData, "select")
    @mock.patch.object(DetectionTableData, "save")
    def test_save_csv(self, save, select):
        det_loader = DetectionsCSVLoader(source="source", read_args={"ok": "ok"})
        det_loader.save_csv(
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
        det_loader = ObjectsCSVLoader(source="source", read_args={"ok": "ok"})
        self.assertEqual(det_loader.source, "source")
        self.assertEqual(det_loader.read_args["ok"], "ok")

    @mock.patch.object(ObjectTableData, "select")
    @mock.patch.object(ObjectTableData, "save")
    def test_save_csv(self, save, select):
        det_loader = ObjectsCSVLoader(source="source", read_args={"ok": "ok"})
        det_loader.save_csv(
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
        det_loader = NonDetectionsCSVLoader(source="source", read_args={"ok": "ok"})
        self.assertEqual(det_loader.source, "source")
        self.assertEqual(det_loader.read_args["ok"], "ok")

    @mock.patch.object(NonDetectionTableData, "select")
    @mock.patch.object(NonDetectionTableData, "save")
    def test_save_csv(self, save, select):
        det_loader = NonDetectionsCSVLoader(source="source", read_args={"ok": "ok"})
        det_loader.save_csv(
            spark_session=mock.Mock(),
            output_path="test",
            n_partitions=1,
            max_records_per_file=1,
            mode="mode",
        )
        select.assert_called_once()
        save.assert_called_once()
