from config import Config
from generic_object import GenericObject
from collections import OrderedDict


try:
    from typing import List, Any
except:
    pass


class DmeMasterLink(GenericObject):
    def __init__(self, config):
        # type: (Config) -> None
        self.id = None
        self.global_id = None
        self.compkey = None
        self.link_id = None
        self.us_node_id = None
        self.ds_node_id = None
        self.diameter = None
        self.us_depth = None
        self.ds_depth = None
        self.us_depth_source = None
        self.ds_depth_source = None
        self.geometry = None

    @staticmethod
    def current_id_object_type():
        return "Hybrid"

    @staticmethod
    def input_field_attribute_lookup():
        field_attribute_lookup = OrderedDict()
        field_attribute_lookup["ID"] = "id"
        field_attribute_lookup["GlobalID"] = "global_id"
        field_attribute_lookup["COMPKEY"] = "compkey"
        field_attribute_lookup["link_id"] = "link_id"
        field_attribute_lookup["us_node_id"] = "us_node_id"
        field_attribute_lookup["ds_node_id"] = "ds_node_id"
        field_attribute_lookup["diameter"] = "diameter"
        field_attribute_lookup["us_depth"] = "us_depth"
        field_attribute_lookup["ds_depth"] = "ds_depth"
        field_attribute_lookup["us_source"] = "us_depth_source"
        field_attribute_lookup["ds_source"] = "ds_depth_source"
        field_attribute_lookup["Shape@"] = "geometry"
        return field_attribute_lookup

    @property
    def valid(self):
        return True







