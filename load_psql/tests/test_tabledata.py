import unittest
from unittest import mock
from load_psql.table_data.detection import DataFrame, DetectionTableData
from load_psql.table_data.object import ObjectTableData
from load_psql.table_data.non_detection import NonDetectionTableData
from load_psql.table_data.ss import SSTableData


class DetectionTableDataTest(unittest.TestCase):
    def setUp(self):
        mock_session = mock.MagicMock()
        self.table_data = DetectionTableData(
            spark_session=mock_session, source="source", read_args={}
        )

    @mock.patch("load_psql.table_data.detection.col")
    @mock.patch("load_psql.table_data.detection.lit")
    def test_select(self, col, lit):
        tt_det = mock.MagicMock()
        fillna_mock = mock.MagicMock()
        fillna_mock.fillna.return_value.fillna.return_value.select.return_value = "ok"
        tt_det.select.return_value.withColumn.return_value.withColumn.return_value = (
            fillna_mock
        )
        step_id = "step_id"
        resp = table_data.select(tt_det, step_id)
        self.assertEqual(resp, "ok")

    def test_save(self):
        output_dir = "test"
        n_partitions = 1
        max_records_per_file = 1
        mode = "mode"
        selected = mock.MagicMock()
        self.table_data.save(
            output_dir, n_partitions, max_records_per_file, mode, selected
        )
        selected.coalesce.return_value.write.option.return_value.mode.return_value.csv.assert_called()


class ObjectTableDataTest(unittest.TestCase):
    def setUp(self):
        mock_session = mock.MagicMock()
        self.table_data = ObjectTableData(
            spark_session=mock_session, source="source", read_args={}
        )

    @mock.patch("load_psql.table_data.object.col")
    def test_select(self, col):
        from load_psql.table_data.table_columns import obj_col

        self.table_data.dataframe.select.return_value = "ok"
        resp = self.table_data.select()
        self.assertNotIn("objectId", obj_col)
        self.assertNotIn("ndethist", obj_col)
        self.assertNotIn("ncovhist", obj_col)
        self.assertEqual(resp, "ok")

    def test_save(self):
        output_dir = "test"
        n_partitions = 1
        max_records_per_file = 1
        mode = "mode"
        selected = mock.MagicMock()
        self.table_data.save(
            output_dir, n_partitions, max_records_per_file, mode, selected
        )
        selected.coalesce.return_value.write.option.return_value.mode.return_value.csv.assert_called()


class NonDetectionTableDataTest(unittest.TestCase):
    def setUp(self):
        mock_session = mock.MagicMock()
        self.table_data = NonDetectionTableData(
            spark_session=mock_session, source="source", read_args={}
        )

    @mock.patch("load_psql.table_data.non_detection.col")
    def test_select(self, col):
        self.table_data.dataframe.withColumn.return_value.drop.return_value.select.return_value = (
            "ok"
        )
        resp = self.table_data.select()
        self.assertEqual(resp, "ok")

    def test_save(self):
        output_dir = "test"
        n_partitions = 1
        max_records_per_file = 1
        mode = "mode"
        selected = mock.MagicMock()
        self.table_data.save(
            output_dir, n_partitions, max_records_per_file, mode, selected
        )
        selected.coalesce.return_value.write.option.return_value.mode.return_value.csv.assert_called()


class SSTableDataTest(unittest.TestCase):
    def setUp(self):
        mock_session = mock.MagicMock()
        self.table_data = SSTableData(
            spark_session=mock_session, source="source", read_args={}
        )

    @mock.patch("load_psql.table_data.ss.col")
    @mock.patch("load_psql.table_data.ss.Window")
    @mock.patch("load_psql.table_data.ss.SSTableData.get_min")
    def test_select(self, get_min, window, col):
        from load_psql.table_data.table_columns import ss_col

        tt_det = mock.MagicMock()
        ssmin, window = self.table_data.select(tt_det)
        self.assertNotIn("objectId", ss_col)
        self.assertNotIn("candid", ss_col)
        self.assertIsNotNone(ssmin)
        self.assertIsNotNone(window)

    def test_save(self):
        output_dir = "test"
        n_partitions = 1
        max_records_per_file = 1
        mode = "mode"
        selected = mock.MagicMock()
        self.table_data.save(
            output_dir, n_partitions, max_records_per_file, mode, selected
        )
        selected.coalesce.return_value.write.option.return_value.mode.return_value.csv.assert_called()
