import arcpy

try:
    from typing import List, Any
except:
    pass
from businessclasses.config import Config
from db_data_io import DbDataIo

class DmeMasterHybridDbDataIo(DbDataIo):
    def __init__(self, config):
        # type: (Config) -> None
        self.config = config
        self.current_id_database_table_path = self.config.DME_master_hybrid_id_table_sde_path
        self.workspace = "in_memory"



