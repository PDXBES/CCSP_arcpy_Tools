from collections import OrderedDict
from unittest import TestCase
import arcpy
import mock
from testbusinessclasses.mock_config import MockConfig
from businessclasses.generic_object import GenericObject
from dataio.db_data_io import DbDataIo


class TestDbDataIO(TestCase):

    def setUp(self):
        mock_config = MockConfig()
        self.config = mock_config.config

        self.parent_id_to_db_field_mapping = [(1, "id_field_one"), (2, "id_field_two")]
        self.db_data_io = DbDataIo(self.config)
        self.object_class = None
        self.field_names_retrieve_id = ["Object_Type", "Current_ID"]

        self.mock_row = (1, 2)
        self.field_attribute_lookup_add_object = OrderedDict([("id_db", "id"), ("parent_id_db", "parent_id")])

        self.patch_create_object = mock.patch.object(self.db_data_io, "create_object")
        self.mock_create_object = self.patch_create_object.start()

        self.patch_create_object_with_current_id = mock.patch.object(self.db_data_io, "create_object_with_id")
        self.mock_create_object_with_current_id = self.patch_create_object_with_current_id.start()

        self.patch_delete_management = mock.patch("arcpy.Delete_management")
        self.mock_delete_management = self.patch_delete_management.start()

        self.mock_update_cursor = mock.MagicMock(arcpy.da.UpdateCursor)
        self.mock_update_cursor.__iter__.return_value = iter([("MagicMock", 44), ("GenericObject", 55)])

        self.mock_update_cursor1 = mock.MagicMock(arcpy.da.UpdateCursor)
        row = [1]
        row[0] = 1
        self.mock_update_cursor1.__iter__.return_value = iter([row])

        self.mock_insert_cursor = mock.MagicMock(arcpy.da.InsertCursor)

        self.mock_search_cursor = mock.MagicMock(arcpy.da.SearchCursor)
        self.mock_search_cursor.__iter__.return_value = iter([self.mock_row])

        self.patch_append = mock.patch("arcpy.Append_management")
        self.mock_append = self.patch_append.start()

        self.patch_make_query_table = mock.patch("arcpy.MakeQueryTable_management")
        self.mock_make_query_table = self.patch_make_query_table.start()

        self.patch_copy_features_management = mock.patch("arcpy.CopyFeatures_management")
        self.mock_copy_features_management = self.patch_copy_features_management.start()

        self.patch_add_field_management = mock.patch("arcpy.AddField_management")
        self.mock_add_field_management = self.patch_add_field_management.start()

        self.patch_calculate_field = mock.patch("arcpy.CalculateField_management")
        self.mock_calculate_field = self.patch_calculate_field.start()

        self.patch_da_UpdateCursor = mock.patch("arcpy.da.UpdateCursor")
        self.mock_da_UpdateCursor = self.patch_da_UpdateCursor.start()

        self.patch_da_InsertCursor = mock.patch("arcpy.da.InsertCursor")
        self.mock_da_InsertCursor = self.patch_da_InsertCursor.start()

        self.patch_da_SearchCursor = mock.patch("arcpy.da.SearchCursor")
        self.mock_da_SearchCursor = self.patch_da_SearchCursor.start()
        self.mock_da_SearchCursor.return_value = self.mock_search_cursor

        self.patch_get_count_management = mock.patch("arcpy.GetCount_management")
        self.mock_get_count_management = self.patch_get_count_management.start()
        self.mock_get_count_management.return_value = ["1"]

        self.patch_create_field_map_for_sde_db = mock.patch.object(self.db_data_io, "_create_field_map_for_sde_db")
        self.mock_create_field_map_for_sde_db = self.patch_create_field_map_for_sde_db.start()

        self.patch_create_feature_class = mock.patch("arcpy.CreateFeatureclass_management")
        self.mock_create_feature_class = self.patch_create_feature_class.start()

        self.mock_da_InsertCursor.return_value = self.mock_insert_cursor

        self.field_attribute_lookup_add_object = OrderedDict()
        self.field_attribute_lookup_add_object["id_field"] = "id"
        self.field_attribute_lookup_add_object["parent_id_field"] = "parent_id"

        self.field_attribute_lookup_create_object = OrderedDict()
        self.field_attribute_lookup_create_object["id_db"] = "id"
        self.field_attribute_lookup_create_object["parent_id_db"] = "parent_id"

        self.field_attribute_lookup_create_table_from_objects = self.field_attribute_lookup_create_object

        self.mock_generic_object = mock.MagicMock(GenericObject)
        self.mock_generic_object.id = 1
        self.mock_generic_object.valid = False
        self.mock_generic_object.parent_id = 2
        self.mock_generic_object.input_field_attribute_lookup.return_value = self.field_attribute_lookup_create_object

        self.patch_input_field_attribute_lookup = mock.patch("businessclasses.generic_object.GenericObject.input_field_attribute_lookup")
        self.mock_input_field_attribute_lookup = self.patch_input_field_attribute_lookup.start()
        self.mock_input_field_attribute_lookup.return_value = self.field_attribute_lookup_create_table_from_objects

        self.object_tracking_sde_path = "object_tracking_sde_path"

    def tearDown(self):
        self.mock_da_UpdateCursor = self.patch_da_UpdateCursor.stop()
        self.mock_da_InsertCursor = self.patch_da_InsertCursor.stop()
        self.mock_append = self.patch_append.stop()
        self.mock_copy_features_management = self.patch_copy_features_management.stop()
        self.mock_add_field_management = self.patch_add_field_management.stop()
        self.mock_calculate_field = self.patch_calculate_field.stop()
        self.mock_create_field_map_for_sde_db = self.patch_create_field_map_for_sde_db.stop()
        self.mock_da_SearchCursor = self.patch_da_SearchCursor.stop()
        self.mock_create_object = self.patch_create_object.stop()
        self.mock_delete_management = self.patch_delete_management.stop()
        self.mock_create_feature_class = self.patch_create_feature_class.stop()
        self.mock_input_field_attribute_lookup = self.patch_input_field_attribute_lookup.stop()
        self.mock_make_query_table = self.patch_make_query_table.stop()

    def test_retrieve_current_id_calls_update_cursor_with_correct_arguments(self):
        self.mock_da_UpdateCursor.return_value = self.mock_update_cursor
        self.db_data_io.current_id_database_table_path = "current_id_database_table_path"
        self.db_data_io.retrieve_current_id(mock.MagicMock)
        self.mock_da_UpdateCursor.assert_called_with("current_id_database_table_path", self.field_names_retrieve_id)

    def test_retrieve_current_id_return_current_ID(self):
        self.mock_da_UpdateCursor.return_value = self.mock_update_cursor
        current_id = self.db_data_io.retrieve_current_id(mock.MagicMock)
        self.assertTrue(current_id == 44)

    def test_retrieve_current_id_update_next_id_of_object_1(self):
        self.mock_da_UpdateCursor.return_value = self.mock_update_cursor
        self.db_data_io.retrieve_current_id(mock.MagicMock)
        self.assertTrue(self.mock_update_cursor.updateRow.called)
        self.mock_update_cursor.updateRow.assert_called_with(["MagicMock", 45])

    def test_retrieve_current_id_update_next_id_of_object_2(self):
        self.mock_da_UpdateCursor.return_value = self.mock_update_cursor
        self.db_data_io.retrieve_current_id(GenericObject)
        self.assertTrue(self.mock_update_cursor.updateRow.called)
        self.mock_update_cursor.updateRow.assert_called_with(["GenericObject", 56])

    def test_retrieve_block_of_ids_number_of_objects_is_100_get_next_id_of_object_2(self):
        self.mock_da_UpdateCursor.return_value = self.mock_update_cursor
        self.db_data_io._retrieve_block_of_ids(GenericObject, 100)
        self.assertTrue(self.mock_update_cursor.updateRow.called)
        self.mock_update_cursor.updateRow.assert_called_with(["GenericObject", 155])

    #TODO create better exceptions
    def test_retrieve_block_of_ids_number_of_objects_is_zero_throws_exception(self):
        with self.assertRaises(Exception):
            self.db_data_io._retrieve_block_of_ids("object_2", 0)

    def test_retrieve_block_of_ids_number_of_objects_is_less_than_zero_throws_exception(self):
        with self.assertRaises(Exception):
            self.db_data_io._retrieve_block_of_ids("object_2", -1)

    def test_create_row_from_object_creates_row_with_correct_values(self):
        row = self.db_data_io.create_row_from_object(self.mock_generic_object, self.field_attribute_lookup_add_object)
        self.assertEquals(row, [1, 2])

    def test_create_row_from_object_raise_exception_when_attribute_name_does_not_exist(self):
        self.field_attribute_lookup_add_object["color"] = "red"
        with self.assertRaises(AttributeError):
            self.db_data_io.create_row_from_object(self.mock_generic_object, self.field_attribute_lookup_add_object)

    def test_copy_to_memory_calls_copy_features_management_with_correct_arguments(self):
        self.db_data_io.copy_to_memory("input_table", "in_memory_output_table_name")
        self.mock_copy_features_management.assert_called_with("input_table", "in_memory\\in_memory_output_table_name")

    def test_create_object_from_row_creates_object_with_correct_attributes(self):
        self.mock_generic_object.id = None
        self.mock_generic_object.parent_id = None
        self.db_data_io.create_object_from_row(self.mock_generic_object, self.field_attribute_lookup_create_object, self.mock_row)
        self.assertEqual(self.mock_generic_object.id, 1)
        self.assertEqual(self.mock_generic_object.parent_id, 2)

    def test_create_objects_from_table_calls_search_cursor_with_correct_arguments(self):
        self.db_data_io.create_objects_from_table("area", "table", self.field_attribute_lookup_create_object)
        self.mock_da_SearchCursor.assert_called_with("table", self.field_attribute_lookup_create_object.keys())

    def test_create_objects_from_table_returns_list_with_correct_object(self):
        self.mock_create_object.return_value = self.mock_generic_object
        list_of_objects = self.db_data_io.create_objects_from_table("area", "table",
                                                                    self.field_attribute_lookup_create_object)
        object_1 = list_of_objects[0]
        self.assertEqual(object_1.id, 1)
        self.assertEqual(object_1.parent_id, 2)

    def test_create_objects_from_table_with_current_id_calls_search_cursor_with_correct_arguments(self):
        with mock.patch.object(self.db_data_io, "_retrieve_block_of_ids") as mock_return_block_of_ids:
            mock_return_block_of_ids.return_value = 0
            self.db_data_io.create_objects_from_table_with_current_id("area", "table",
                                                                      self.field_attribute_lookup_create_object)
            self.mock_da_SearchCursor.assert_called_with("table", self.field_attribute_lookup_create_object.keys())

    def test_create_objects_from_table_with_current_id_calls_retrieve_block_of_ids_with_correct_arguments(self):
        with mock.patch.object(self.db_data_io, "_retrieve_block_of_ids") as mock_return_block_of_ids:
            mock_return_block_of_ids.return_value = 0
            self.db_data_io.create_objects_from_table_with_current_id("area", "table",
                                                                      self.field_attribute_lookup_create_object)
            mock_return_block_of_ids.assert_called_with("area", 1)

    def test_create_objects_from_table_with_current_id_returns_list_with_correct_object(self):
        with mock.patch.object(self.db_data_io, "_retrieve_block_of_ids") as mock_return_block_of_ids:
            mock_return_block_of_ids.return_value = 1
            self.mock_create_object_with_current_id.return_value = self.mock_generic_object
            list_of_objects = self.db_data_io.create_objects_from_table_with_current_id("area", "table",
                                                                                        self.field_attribute_lookup_create_object)
            object_1 = list_of_objects[0]
            self.assertEqual(1, object_1.id)
            self.assertEqual(2, object_1.parent_id)

    def test_create_objects_from_table_with_current_id_exceeds_maximum_id_throws_exception(self):
        with mock.patch.object(self.db_data_io, "_retrieve_block_of_ids") as mock_return_block_of_ids:
            mock_return_block_of_ids.return_value = 0
            self.mock_get_count_management.return_value = ["0"]
            self.mock_create_object_with_current_id.return_value = self.mock_generic_object
            with self.assertRaises(Exception):
                self.db_data_io.create_objects_from_table_with_current_id("area", "table",
                                                                          self.field_attribute_lookup_create_object)

    def test_create_feature_class_from_objects_calls_create_table_with_correct_arguments(self):
        self.db_data_io.workspace = "in_memory"
        output_feature_class_name = "output_feature_class_name"
        object_list = ["obj1", "obj2"]
        field_attribute_lookup = OrderedDict()
        template_feature_class = "template_feature_class_path"
        self.db_data_io.create_feature_class_from_objects(object_list, self.db_data_io.workspace, output_feature_class_name,
                                                          field_attribute_lookup, template_feature_class)
        self.mock_create_feature_class.assert_called_with("in_memory", "output_feature_class_name", "", "template_feature_class_path", "", "", "template_feature_class_path")

    def test_create_feature_class_from_objects_calls_insert_cursor_with_correct_arguments(self):
        self.db_data_io.workspace = "in_memory"
        output_feature_class_name = "output_feature_class_name"
        object_list = [self.mock_generic_object]
        template_feature_class_path = "template_feature_class_path"
        field_list = ["id_db", "parent_id_db"]
        self.db_data_io.create_feature_class_from_objects(object_list, self.db_data_io.workspace,
                                                          output_feature_class_name,
                                                          self.field_attribute_lookup_create_table_from_objects,
                                                          template_feature_class_path)
        self.mock_da_InsertCursor.assert_called_with("in_memory\\output_feature_class_name", field_list)

    def test_create_feature_class_from_objects_calls_insert_row_with_correct_arguments(self):
        self.db_data_io.workspace = "in_memory"
        output_feature_class_name = "output_feature_class_name"
        object_list = [self.mock_generic_object]
        template_feature_class_path = "template_feature_class_path"
        self.db_data_io.create_feature_class_from_objects(object_list, self.db_data_io.workspace,
                                                          output_feature_class_name,
                                                          self.field_attribute_lookup_create_table_from_objects,
                                                          template_feature_class_path)
        self.mock_insert_cursor.insertRow.assert_called_with([1, 2])

    def test_append_feature_class_to_db_calls_create_feature_class_from_objects_with_correct_arguments(self):
        with mock.patch.object(self.db_data_io, "create_feature_class_from_objects") as mock_create_feature_class_from_objects:
            self.db_data_io.workspace = "in_memory"
            output_feature_class_name = "intermediate_feature_class_to_append"
            object_list = [self.mock_generic_object]
            field_attribute_lookup = OrderedDict()
            template_feature_class= "template_feature_class"
            target_path = "target_path"
            self.db_data_io.append_objects_to_db(object_list, field_attribute_lookup, template_feature_class, target_path)
            mock_create_feature_class_from_objects.assert_called_with(object_list, "in_memory", output_feature_class_name, field_attribute_lookup, template_feature_class)

    def test_append_feature_class_to_db_calls_append_with_correct_arguments(self):
        with mock.patch.object(self.db_data_io, "create_feature_class_from_objects"):
            with mock.patch.object(self.db_data_io, "_create_field_map_for_sde_db") as mock_create_field_map_for_sde_db:
                self.db_data_io.workspace = "in_memory"
                object_list = [self.mock_generic_object]
                field_attribute_lookup = OrderedDict()
                template_feature_class_path = "template_feature_class_path"
                target_path = "target_path"
                mock_create_field_map_for_sde_db.return_value = "field_mapping_for_sde_db"

                self.db_data_io.append_objects_to_db(object_list, field_attribute_lookup, template_feature_class_path,
                                                     target_path)
                self.mock_append.assert_called_with("in_memory\\intermediate_feature_class_to_append", "target_path", "NO_TEST", "field_mapping_for_sde_db")

    def test_create_objects_from_database_calls_copy_to_memory_with_correct_arguments(self):
        input_table = self.config.model_tracking_sde_path
        class_type = GenericObject
        in_memory_output_table_name = "object_table"
        with mock.patch.object(self.db_data_io, "copy_to_memory") as mock_copy_to_memory:
            with mock.patch.object(self.db_data_io, "create_objects_from_table"):
                self.db_data_io.create_objects_from_database(class_type, input_table)
                mock_copy_to_memory.assert_called_with(input_table, in_memory_output_table_name)

    def test_create_objects_from_database_calls_create_objects_from_table_with_correct_arguments(self):
        input_table = self.config.model_tracking_sde_path
        table = "in_memory/object_table"
        class_type = GenericObject
        field_attribute_lookup = self.field_attribute_lookup_create_object
        with mock.patch.object(self.db_data_io, "copy_to_memory"):
            with mock.patch.object(self.db_data_io, "create_objects_from_table") as mock_create_objects_from_table:
                self.db_data_io.create_objects_from_database(class_type, input_table)
                mock_create_objects_from_table.assert_called_with( class_type, table, field_attribute_lookup)

    def test_create_objects_from_database_calls_delete_management_with_correct_arguments(self):
        input_table = self.config.model_tracking_sde_path
        table = "in_memory/object_table"
        class_type = GenericObject
        with mock.patch.object(self.db_data_io, "copy_to_memory"):
            with mock.patch.object(self.db_data_io, "create_objects_from_table"):
                self.db_data_io.create_objects_from_database(class_type, input_table)
                self.mock_delete_management.assert_called_with(table)

    def test_create_objects_from_database_returns_correct_object(self):
        input_table = self.config.model_tracking_sde_path
        table = "in_memory/object_table"
        class_type = GenericObject
        with mock.patch.object(self.db_data_io, "copy_to_memory"):
            with mock.patch.object(self.db_data_io, "create_objects_from_table") as mock_create_objects_from_table:
                test_object = "test_object"
                mock_create_objects_from_table.return_value = test_object
                objects = self.db_data_io.create_objects_from_database(class_type, input_table)
                self.assertEqual(test_object, objects)

    def test_copy_to_memory_with_id_filter_single_value_calls_make_query_table_with_correct_arguments(self):
        input_table = "input_table"
        output_table = "output_table"
        id_field_name = "id_field_name"
        id_list = [9999]
        where_clause = "id_field_name in (9999)"
        self.db_data_io.copy_to_memory_with_id_filter(input_table, output_table, id_field_name, id_list)
        self.mock_make_query_table.assert_called_with(input_table, "in_memory\\output_table","","","",where_clause)

    def test_copy_to_memory_with_id_filter_multi_value_calls_make_query_table_with_correct_arguments(self):
        input_table = "input_table"
        output_table = "output_table"
        id_field_name = "id_field_name"
        id_list = [9999, 666]
        where_clause = "id_field_name in (9999,666)"
        self.db_data_io.copy_to_memory_with_id_filter(input_table, output_table, id_field_name, id_list)
        self.mock_make_query_table.assert_called_with(input_table, "in_memory\\output_table","","","",where_clause)

    def test_create_objects_from_database_with_id_filter_calls_copy_to_memory_with_correct_arguments(self):
        class_type = GenericObject
        input_table_name = "input_table_name"
        id_field_name = "id_field_name"
        id_list = "id_list"
        in_memory_output_table_name = "object_table"
        with mock.patch.object(self.db_data_io, "create_objects_from_table"):
            with mock.patch.object(self.db_data_io, "copy_to_memory_with_id_filter") as mock_copy_to_memory_with_id_filter:
                self.db_data_io.create_objects_from_database_with_id_filter(class_type, input_table_name, id_field_name, id_list)
                mock_copy_to_memory_with_id_filter.assert_called_with(input_table_name, in_memory_output_table_name, id_field_name, id_list)

    def test_create_objects_from_database_with_id_filter_calls_create_objects_from_table_with_correct_arguments(self):
        class_type = GenericObject
        input_table_name = "input_table_name"
        id_field_name = "id_field_name"
        id_list = "id_list"
        table = "in_memory/object_table"
        with mock.patch.object(self.db_data_io, "create_objects_from_table") as mock_create_objects_from_table:
            with mock.patch.object(self.db_data_io, "copy_to_memory_with_id_filter"):
                self.db_data_io.create_objects_from_database_with_id_filter(class_type, input_table_name, id_field_name, id_list)
                mock_create_objects_from_table.assert_called_with(class_type, table, self.field_attribute_lookup_create_table_from_objects)

    def test_create_objects_from_database_with_id_filter_calls_delete_management_with_correct_arguments(self):
        class_type = GenericObject
        input_table_name = "input_table_name"
        id_field_name = "id_field_name"
        id_list = "id_list"
        table = "in_memory/object_table"
        with mock.patch.object(self.db_data_io, "create_objects_from_table"):
            with mock.patch.object(self.db_data_io, "copy_to_memory_with_id_filter"):
                self.db_data_io.create_objects_from_database_with_id_filter(class_type, input_table_name, id_field_name, id_list)
                self.mock_delete_management.assert_called_with(table)

    def test_create_objects_from_database_with_id_filter_returns_correct_objects(self):
        class_type = GenericObject
        input_table_name = "input_table_name"
        id_field_name = "id_field_name"
        id_list = "id_list"
        test_objects = "test_objects"
        with mock.patch.object(self.db_data_io, "create_objects_from_table") as mock_create_objects_from_table:
            with mock.patch.object(self.db_data_io, "copy_to_memory_with_id_filter"):
                mock_create_objects_from_table.return_value = test_objects
                objects = self.db_data_io.create_objects_from_database_with_id_filter(class_type, input_table_name, id_field_name, id_list)
                self.assertEquals(objects, test_objects)

    def test_add_ids_calls_add_field_management_with_correct_arguments(self):
        with mock.patch.object(self.db_data_io, "_retrieve_block_of_ids") as mock_return_block_of_ids:
            mock_return_block_of_ids.return_value = 1
            in_memory_table = "table"
            id_field = "id_field"
            object_type = "object"
            self.db_data_io.add_ids(in_memory_table, id_field, object_type)
            self.mock_add_field_management.assert_called_with("table", "id_field", "LONG")

    def test_add_ids_calls_update_search_cursor_with_correct_arguments(self):
        with mock.patch.object(self.db_data_io, "_retrieve_block_of_ids") as mock_return_block_of_ids:
            mock_return_block_of_ids.return_value = 1
            in_memory_table = "table"
            id_field = "id_field"
            object_type = "object"
            self.db_data_io.add_ids(in_memory_table, id_field, object_type)
            self.mock_da_UpdateCursor.assert_called_with("table", "id_field")

    def test_add_ids_calls_update_row_with_correct_arguments(self):
        with mock.patch.object(self.db_data_io, "_retrieve_block_of_ids") as mock_return_block_of_ids:
            mock_return_block_of_ids.return_value = 1
            self.mock_da_UpdateCursor.return_value = self.mock_update_cursor1
            in_memory_table = "table"
            id_field = "id_field"
            object_type = "object"
            self.db_data_io.add_ids(in_memory_table, id_field, object_type)
            self.mock_update_cursor1.updateRow.assert_called_with([1])
    #TODO - don't know how to make this test work so always fails - code is correct
