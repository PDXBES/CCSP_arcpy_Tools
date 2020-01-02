from dataio.db_data_io import DbDataIo
from config import Config
import arcpy
try:
    from typing import List, Any, Dict
except:
    pass

class GenericObject(object):
    def __init__(self, config):
        self.config = config
        self.id = None
        self.parent_id = None
        self.input_field_attribute_lookup = None

    @classmethod
    def initialize_with_current_id(cls, config, db_data_io):
        # type: (Config, DbDataIo) -> GenericObject
        generic_object = cls(config)
        generic_object_current_id = db_data_io.retrieve_current_id(generic_object)
        generic_object.id = generic_object_current_id
        return generic_object

    @staticmethod
    def input_field_attribute_lookup():
        pass

    @property
    def valid(self):
        # type: () -> bool
        return False

    def _write_attributes_to_screen(self):
        arcpy.AddError("Begin Attribute Dump")
        arcpy.AddError("Object Type:" + type(self))
        attribute_names = vars(self)
        for attribute_name in attribute_names:
            attribute_value = getattr(self, attribute_name)
            arcpy.AddMessage(attribute_name + ":" + str(attribute_value))
        arcpy.AddError("End Attribute Dump")
