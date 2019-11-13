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
        self.name = "dme_link"
        self.geometry = None

    @staticmethod
    def input_field_attribute_lookup():
        field_attribute_lookup = OrderedDict()
        field_attribute_lookup["GlobalID"] = "global_id"
        field_attribute_lookup["Shape@"] = "geometry"
        return field_attribute_lookup

    @property
    def valid(self):
        return True







