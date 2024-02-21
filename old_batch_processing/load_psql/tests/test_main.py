import unittest
from unittest import mock
from load_psql.postprocess_create_csv_main import (
    create_session,
    get_tt_det,
    validate_config,
)
from valid_config import load_config


class MainTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_create_session(self):
        pass

    def test_get_tt_det(self):
        pass

    def test_validate_config(self):
        self.assertTrue(validate_config(load_config))

    def test_validate_config_with_error(self):
        pass
