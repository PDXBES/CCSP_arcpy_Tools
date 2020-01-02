from config import Config
from generic_object import GenericObject
from collections import OrderedDict


try:
    from typing import List, Any
except:
    pass


class DmeLink(GenericObject):
    def __init__(self, config):
        # type: (Config) -> None
        self.global_id = None
        self.compkey = None
        self.us_node_name = None
        self.ds_node_name = None
        self.diameter = None
        self.us_depth = None
        self.ds_depth = None
        self.geometry = None

    @staticmethod
    def input_field_attribute_lookup():
        field_attribute_lookup = OrderedDict()
        field_attribute_lookup["GlobalID"] = "global_id"
        field_attribute_lookup["COMPKEY"] = "compkey"
        field_attribute_lookup["FRM_NODE"] = "us_node_name"
        field_attribute_lookup["TO_NODE"] = "ds_node_name"
        field_attribute_lookup["PIPESIZE"] = "diameter"
        field_attribute_lookup["Shape@"] = "geometry"
        return field_attribute_lookup

    @property
    def valid(self):
        return True







