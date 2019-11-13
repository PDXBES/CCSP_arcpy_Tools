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
    def name():
        return "Hybrid"

    @staticmethod
    def input_field_attribute_lookup():
        field_attribute_lookup = OrderedDict()
        field_attribute_lookup["ID"] = "id"
        field_attribute_lookup["GlobalID"] = "global_id"
        field_attribute_lookup["Shape@"] = "geometry"
        return field_attribute_lookup

    @property
    def valid(self):
        return True







