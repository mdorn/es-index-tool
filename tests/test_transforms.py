#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `es_index_tool.transforms` package."""
from datetime import datetime, timedelta, timezone

from es_index_tool.transforms.age_category import AgeCategoryTransform


def test_age_category_transform():
    """Ensure a birthdate is transformed to a list of age categories."""
    age_category_transformer = AgeCategoryTransform()
    diff_19 = timedelta(days=365*19)
    now = datetime.now()
    bday_19 = now - diff_19
    result = age_category_transformer.transform(bday_19)
    # over 18, but not over 21
    assert result == ['over 18']


def test_country_transform():
    # TODO
    pass


def test_object_key_transform():
    # TODO
    pass
