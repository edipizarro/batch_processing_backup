import unittest
from unittest import mock
from load_psql.table_data.detection import DataFrame
from load_psql.table_data import (
    DetectionTableData,
    NonDetectionTableData,
    ObjectTableData,
    SSTableData,
    DataQualityTableData,
    MagstatsTableData,
    PS1TableData,
    GaiaTableData,
    ReferenceTableData,
)


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
        resp = self.table_data.select([], tt_det, step_id)
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

        obj_col_cpy = obj_col.copy()

        self.table_data.dataframe.select.return_value = "ok"
        resp = self.table_data.select(obj_col_cpy)
        self.assertNotIn("objectId", obj_col_cpy)
        self.assertNotIn("ndethist", obj_col_cpy)
        self.assertNotIn("ncovhist", obj_col_cpy)
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
        resp = self.table_data.select([])
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
    @mock.patch("load_psql.table_data.ss.spark_min")
    def test_select(self, spark_min, window, col):
        from load_psql.table_data.table_columns import ss_col

        ss_col_cpy = ss_col.copy()

        tt_det = mock.MagicMock()
        ssmin = self.table_data.select(ss_col_cpy, tt_det, mock.MagicMock())
        self.assertNotIn("objectId", ss_col_cpy)
        self.assertNotIn("candid", ss_col_cpy)
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


class DataQualityTableDataTest(unittest.TestCase):
    def setUp(self):
        mock_session = mock.MagicMock()
        self.table_data = DataQualityTableData(
            spark_session=mock_session, source="source", read_args={}
        )

    @mock.patch("load_psql.table_data.data_quality.col")
    def test_select(self, col):
        from load_psql.table_data.table_columns import qua_col

        qua_col_cpy = qua_col.copy()

        tt_det = mock.MagicMock()
        resp = self.table_data.select(qua_col_cpy, tt_det)
        self.assertNotIn("objectId", qua_col_cpy)
        self.assertNotIn("candid", qua_col_cpy)
        self.assertEqual(resp, tt_det.select.return_value)

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


class MagstatsTableDataTest(unittest.TestCase):
    def setUp(self):
        mock_session = mock.MagicMock()
        self.table_data = MagstatsTableData(
            spark_session=mock_session, source="source", read_args={}
        )

    @mock.patch("load_psql.table_data.magstats.col")
    @mock.patch("load_psql.table_data.magstats.lit")
    def test_select(self, lit, col):
        resp = self.table_data.select([])
        self.assertEqual(
            resp,
            self.table_data.dataframe.withColumn.return_value.withColumn.return_value.select.return_value,
        )

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


class PS1TableDataTest(unittest.TestCase):
    def setUp(self):
        mock_session = mock.MagicMock()
        self.table_data = PS1TableData(
            spark_session=mock_session, source="source", read_args={}
        )

    @mock.patch("load_psql.table_data.ps1.col")
    @mock.patch("load_psql.table_data.ps1.countDistinct")
    @mock.patch("load_psql.table_data.ps1.spark_min")
    def test_select(self, spark_min, lit, col):
        from load_psql.table_data.table_columns import ps1_col

        ps1_col_cpy = ps1_col.copy()

        resp = self.table_data.select(ps1_col_cpy, mock.MagicMock())
        self.assertNotIn("objectId", ps1_col_cpy)
        self.assertNotIn("unique1", ps1_col_cpy)
        self.assertNotIn("unique2", ps1_col_cpy)
        self.assertNotIn("unique3", ps1_col_cpy)

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


class GaiaTableDataTest(unittest.TestCase):
    def setUp(self):
        mock_session = mock.MagicMock()
        self.table_data = GaiaTableData(
            spark_session=mock_session, source="source", read_args={}
        )

    @mock.patch("load_psql.table_data.gaia.col")
    @mock.patch("load_psql.table_data.gaia.countDistinct")
    @mock.patch("load_psql.table_data.gaia.spark_min")
    @mock.patch("load_psql.table_data.gaia.spark_abs")
    @mock.patch("load_psql.table_data.gaia.GaiaTableData.compare_threshold")
    def test_select(self, comp_threshold, spark_abs, spark_min, count, col):
        from load_psql.table_data.table_columns import gaia_col

        gaia_col_cpy = gaia_col.copy()

        resp = self.table_data.select(gaia_col_cpy, mock.MagicMock(), mock.MagicMock())
        self.assertNotIn("objectId", gaia_col_cpy)
        self.assertNotIn("candid", gaia_col_cpy)
        self.assertNotIn("unique1", gaia_col_cpy)

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


class ReferenceTableDataTest(unittest.TestCase):
    def setUp(self):
        mock_session = mock.MagicMock()
        self.table_data = ReferenceTableData(
            spark_session=mock_session, source="source", read_args={}
        )

    @mock.patch("load_psql.table_data.reference.col")
    @mock.patch("load_psql.table_data.reference.Window")
    @mock.patch("load_psql.table_data.reference.spark_min")
    def test_select(self, spark_min, window, col):
        from load_psql.table_data.table_columns import ref_col

        ref_col_cpy = ref_col.copy()

        resp = self.table_data.select(ref_col_cpy, mock.MagicMock())
        self.assertNotIn("objectId", ref_col_cpy)
        self.assertNotIn("rfid", ref_col_cpy)

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
