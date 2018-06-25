#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `es_index_tool` package."""

from mock import Mock

from es_index_tool.main import ESIndexTool


def _get_tool(mongodb):
    es_mock = Mock()
    tool = ESIndexTool(
        config_path='./es_index_tool/data/example_config.json',
        mongodb_client=mongodb,
        es_client=es_mock,
    )
    return es_mock, tool


def test_index_document_by_id(mongodb):
    """Ensure a single source DB record is indexed in ES successfully."""
    es_mock, tool = _get_tool(mongodb)
    tool.index_document_by_id('abc123')
    es_mock.index.assert_called()


def test_index_all_documents(mongodb):
    # TODO
    pass


def test_reindex():
    # TODO
    pass


def index_document():
    # TODO
    pass
