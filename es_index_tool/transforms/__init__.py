# -*- coding: utf-8 -*-

"""
es_index_tool.transforms
~~~~~~~~~~~~~~~~~~~~~~~~

This package provides various "transforms" for making it possible to search
source data in cases where that data can't be searched sensibly.  E.g. if
a database record has a "country" field that contains two-letter country codes,
I want a user to be able to type "Brazil" instead of "BR" to get a result.
"""


class Transform(object):
    """Base class for transform classes. A `config` value is optional."""
    def __init__(self, config=None):
        self.config = config

    def transform(self, value):
        pass
