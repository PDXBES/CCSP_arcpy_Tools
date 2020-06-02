from unittest import TestCase
from testbusinessclasses.mock_config import MockConfig
from dataio.utility import Utility
import mock
import datetime

class TestUtility(TestCase):


    def setUp(self):
        mock_config = MockConfig()
        self.config = mock_config.config
        self.utility = Utility(self.config)

        self.patch_Exists = mock.patch("arcpy.Exists")
        self.mock_Exits = self.patch_Exists.start()

        self.patch_date_today = mock.patch.object(self.utility, "date_today")
        self.mock_date_today = self.patch_date_today.start()

        self.patch_todays_gdb_name = mock.patch.object(self.utility, "todays_gdb_name")
        self.mock_todays_gdb_name = self.patch_todays_gdb_name.start()

    def tearDown(self):
        self.mock_Exits = self.patch_Exists.stop()
        # self.mock_date_today = self.patch_date_today.stop()
        # self.mock_todays_gdb_name = self.patch_todays_gdb_name.stop()

    def test_date_today_calls_strftime_with_correct_date_format_argument(self):
        todays_date = mock.MagicMock(datetime.datetime.today())
        todays_date.strftime.return_value = "test string"
        return_string = self.utility.date_today(todays_date)
        self.assertEquals(return_string, "test string")
        self.assertEquals("%Y%m%d", todays_date.strftime.call_args[0][0])

# TODO - I'm not getting these right
    # def test_todays_gdb_name_calls_date_today_with_correct_arguments(self):
    #    mock_date_object = mock.MagicMock(datetime.datetime)
    #    self.utility.todays_gdb_name(mock_date_object)
    #    self.mock_date_today.assert_called_with(mock_date_object)

    # def test_todays_gdb_full_path_name_calls_todays_gdb_name_with_correct_arguments(self):
    #    mock_date_object = mock.MagicMock(datetime.datetime.today())
    #    self.utility.todays_gdb_full_path_name(mock_date_object)
    #    self.mock_todays_gdb_name.assert_called_with(mock_date_object)

    # def test_source_formatter_calls_os_path_join_with_correct_arguments(self):
    #     with mock.patch("os.path.join") as mock_os_path_join:
    #         self.utility.source_formatter("source_string")
    #         mock_os_path_join.assert_called_with("sde_connections", "source_string")

    #def test_valid_source_values_calls_Exists_with_correct_arguments(self)

    #def test_valid_source_values_calls_Exists_with_valid_source_returns_true(self)






