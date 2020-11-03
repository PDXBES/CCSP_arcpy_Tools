import arcpy

try:
    from typing import List, Any
except:
    pass
from businessclasses.config import Config
from businessclasses.dme_master_link import DmeMasterLink
from db_data_io import DbDataIo

class DmeMasterHybridDbDataIo(DbDataIo):
    def __init__(self, config):
        # type: (Config) -> None
        self.config = config
        self.current_id_database_table_path = self.config.DME_master_hybrid_id_table_sde_path
        self.workspace = "in_memory"

    def append_dme_master_links_to_db(self, dme_master_links):
        # requires an existing feature class to append to
        # type: (List[DmeMasterLink]) -> None
        input_field_attribute_lookup = DmeMasterLink.input_field_attribute_lookup()
        template_table = self.config.DME_master_hybrid_sde_path
        target_table = self.config.DME_master_hybrid_sde_path
        self.append_objects_to_db_with_ids(dme_master_links,
                                  input_field_attribute_lookup,
                                  template_table,
                                  target_table)

    def copy_dme_master_links_to_db(self, dme_master_links):
        # type: (List[DmeMasterLink]) -> None
        input_field_attribute_lookup = DmeMasterLink.input_field_attribute_lookup()
        template_table = self.config.DME_master_hybrid_sde_path
        target_table = self.config.DME_master_hybrid_gdb_path
        output_name = "bes_collection_system_master_hybrid_ccsp"
        self.copy_objects_to_db_with_ids(dme_master_links, input_field_attribute_lookup, template_table, target_table, output_name)



