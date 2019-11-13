try:
    from typing import List, Any
except:
    pass
from dme_link import DmeLink
from master_link_with_nodes import MasterLinkWithNodes
from dme_master_link import DmeMasterLink
from config import Config
from dataio.dme_master_hybrid_db_data_io import DmeMasterHybridDbDataIo


class DmeMasterHybrid:
    dme_links = None  # type: List[DmeLink]
    master_links = None  # type: List[MasterLinkWithNodes]
    dme_master_links = None  # type: List[DmeMasterLink]

    def __init__(self, config):
        # type: (Config) -> None
        self.dme_links = []
        self.master_links = []
        self.dme_master_links = []
        self.config = config

    def create_dme_links(self, dme_master_hybrid_db_data_io):
        # type: (DmeMasterHybridDbDataIo) -> List[DmeLink]
        input_database = self.config.egh_public
        input_table_name = self.config.DME_table_name
        object_type = DmeLink
        query = "select * from " + input_table_name + " where (OWNRSHIP in ( ' ' , 'BES' , 'UNKN' , 'DNRV' ) or " + \
                "OWNRSHIP is Null) AND (SERVSTAT in ( ' ' , 'CNS' , 'IN' , 'PEND' , 'TBAB' ) or SERVSTAT is Null) AND " + \
                "LAYER_GROUP in ( 'SEWER PIPES' , 'STORM PIPES')"
        dme_links = dme_master_hybrid_db_data_io.create_objects_from_database_with_query(object_type,
                                                                                         input_database,
                                                                                         query)
        return dme_links

    def create_master_links(self, dme_master_hybrid_db_data_io):
        # type: (DmeMasterHybridDbDataIo) -> List[MasterLinkWithNodes]
        input_database = self.config.CCSP_sde_path
        view_name = self.config.Master_links_nodes_view_name
        object_type = MasterLinkWithNodes
        master_links = dme_master_hybrid_db_data_io.create_objects_from_database_view(object_type,
                                                                                      input_database,
                                                                                      view_name)
        return master_links

    def append_dme_master_links_to_db(self, dme_master_links, dme_master_hybrid_db_data_io):
        # type: (List[DmeMasterLink], DmeMasterHybridDbDataIo) -> None
        input_field_attribute_lookup = DmeMasterLink.input_field_attribute_lookup()
        template_table = self.config.DME_master_hybrid_sde_path
        target_table = self.config.DME_master_hybrid_sde_path
        dme_master_hybrid_db_data_io.append_objects_to_db(dme_master_links,
                                                          input_field_attribute_lookup,
                                                          template_table,
                                                          target_table)

