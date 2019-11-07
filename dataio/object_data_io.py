import arcpy
from businessclasses.config import Config
try:
    from typing import List, Any, Dict
except:
    pass
#from generic_object import GenericObject
from db_data_io import DbDataIo


class ObjectDataIo(object):
    def __init__(self, config, db_data_io):
        # type: (Config, DbDataIo) -> None
        self.config = config
        self.db_data_io = db_data_io

    def append_object_to_db(self, parent_id, generic_object, field_attribute_lookup, object_table_sde_path):
        # type: (int, GenericObject, Dict, str) -> None
        generic_object.parent_id = parent_id
        # object_class.id = self.db_data_io.retrieve_current_id(object_class.name)
        self.db_data_io.append_object_to_db(generic_object, field_attribute_lookup, object_table_sde_path, object_table_sde_path)

    def start_editing_session(self, workspace_path):
        editor = arcpy.da.Editor(workspace_path)
        editor.startEditing(False, True)
        editor.startOperation()
        return editor

    def stop_editing_session(self, workspace_editor, save_changes):
        workspace_editor.stopOperation()
        workspace_editor.stopEditing(save_changes)



