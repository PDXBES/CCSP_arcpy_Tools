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
        self.geometry = None

    @staticmethod
    def input_field_attribute_lookup():
        field_attribute_lookup = OrderedDict()
        field_attribute_lookup["dme_global_id"] = "global_id"
        field_attribute_lookup["Shape@"] = "geometry"
        return field_attribute_lookup

    @property
    def valid(self):
        return True







