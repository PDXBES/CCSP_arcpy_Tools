from config import Config
from generic_object import GenericObject
from collections import OrderedDict


try:
    from typing import List, Any
except:
    pass


class MasterLinkWithNodes(GenericObject):
    def __init__(self, config):
        # type: (Config) -> None
        self.name = "master_link_with_nodes"
        self.global_id = None
        self.link_id = None
        self.us_node_id = None
        self.ds_node_id = None
        self.us_depth = None
        self.ds_depth = None
        self.geometry = None

    @staticmethod
    def input_field_attribute_lookup():
        field_attribute_lookup = OrderedDict()
        field_attribute_lookup["dme_global_id"] = "global_id"
        field_attribute_lookup["link_id"] = "link_id"
        field_attribute_lookup["us_node_id"] = "us_node_name"
        field_attribute_lookup["ds_node_id"] = "ds_node_name"
        field_attribute_lookup["us_depth"] = "us_depth"
        field_attribute_lookup["ds_depth"] = "ds_depth"
        field_attribute_lookup["Shape@"] = "geometry"
        return field_attribute_lookup

    @property
    def valid(self):
        return True







