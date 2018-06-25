# -*- coding: utf-8 -*-

"""
es_index_tool.main
~~~~~~~~~~~~~~~~~~

This module provides a class for managing an Elasticsearch index.
"""
import json
import logging
import os
import time

import dateutil.parser
import pymongo
from elasticsearch import Elasticsearch

from .transforms.country import CountryTransform
from .transforms.object_key import ObjectKeyTransform
from .transforms.age_category import AgeCategoryTransform

# logging configuration
logging.basicConfig(level=logging.INFO)
if os.environ.get('STAGE') != 'production':
    try:
        import coloredlogs
        coloredlogs.install(level='DEBUG')
    except ImportError:
        pass
    logging.basicConfig(level=logging.DEBUG)
SILENCE_LOGGERS = [
    'requests.packages.urllib3',
    'urllib3',
    'elasticsearch',
]
for logger in SILENCE_LOGGERS:
    logging.getLogger(logger).setLevel(logging.WARNING)
logging.getLogger('requests.packages.urllib3').setLevel(logging.WARNING)
# end logging configuration

DB_NAME = os.environ.get('APP_DB_NAME')
DB_CONN = os.environ.get('APP_DB_CONN')
ELASTICSEARCH = os.environ.get('APP_ELASTICSEARCH_URI')
CONFIG_S3_PATH = os.environ.get('APP_CONFIG_S3')
DATA_PATH = os.path.join(os.path.dirname(__file__), 'data')
SEARCHKIT_ANALYSIS_JSON = os.path.join(
    DATA_PATH, 'searchkit_analysis_config.json')


class ESIndexTool(object):
    """
    Main ES indexing tool class.

    Basic usage::

        from es_index_tool.main import ESIndexTool
        tool = ESIndexTool(config_path='./path/to/config.json')
        tool.reindex()

    """

    TRANSFORMS = {
        'country': CountryTransform,
        'object_key': ObjectKeyTransform,
        'age_category': AgeCategoryTransform,
    }

    def __init__(self, config_path=None, mongodb_client=None, es_client=None):
        if config_path:
            self.config = self._load_file_config(config_path)
        else:
            self.config = self._load_s3_config()
        self.mapping = self._get_mapping_config()

        if mongodb_client:
            self.db_client = mongodb_client
        else:
            self.db_client = pymongo.MongoClient(DB_CONN)[DB_NAME]

        if es_client:
            self.es_client = es_client
        else:
            self.es_client = Elasticsearch(
                ELASTICSEARCH, use_ssl=ELASTICSEARCH.startswith('https'))

    def _load_file_config(self, config_path):
        """Load an ES index config from a file."""
        with open(config_path, 'r') as file:
            config = json.loads(file.read())
        return config

    def _load_s3_config(self):
        """Load an ES index config from an S3 bucket."""
        # TODO: use APP_CONFIG_S3 env var
        pass

    def _get_multi_field_def(self, name):
        """
        Create an ES field mapping definition.

        See http://docs.searchkit.co/stable/server/indexing.html
        """
        conf = {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'string',
                    'index': 'not_analyzed'
                }
            }
        }
        conf['fields'][name] = {
            'type': 'string',
            'index': 'analyzed'
        }
        return conf

    def _get_filter_only_def(self, name):
        """
        Create an ES field mapping definition for non-analyzed fields.

        See http://docs.searchkit.co/stable/server/indexing.html
        """
        conf = {
            'type': 'string',
            'fields': {
                'raw': {
                    'type': 'string',
                    'index': 'not_analyzed',
                    'include_in_all': False,
                }
            }
        }
        conf['fields'][name] = {
            'type': 'string',
            'index': 'not_analyzed',
            'include_in_all': False,
        }
        return conf

    def _get_mapping_config(self):
        """Create an ES mapping from a JSON config file."""
        mapping = {
            'suggest': {'type': 'completion'}
        }
        field_config = self.config['fields']
        for i in field_config:
            if 'transform' in field_config[i]:
                field_config[i]['transformInstance'] = self.TRANSFORMS[
                    field_config[i]['transform']
                ](field_config[i].get('transformConfig'))
            key = i.replace('.', '_')
            if field_config[i]['type'] == '_filter_only':
                mapping[key] = self._get_filter_only_def(key)
            elif field_config[i]['type'] == '_multi_field':
                mapping[key] = self._get_multi_field_def(key)
            else:
                mapping[key] = {'type': field_config[i]['type']}
        return mapping

    def _create_index(self):
        """
        Create a new index, and alias it.

        When used with `update_indices` should result in zero downtime.
        See https://www.elastic.co/guide/en/elasticsearch/guide/current/index-aliases.html  # noqa
        """
        es = self.es_client
        with open(SEARCHKIT_ANALYSIS_JSON, 'r') as file:
            index_config = json.loads(file.read())
        doc_type = self.config['collection']
        index_config['mappings'] = {
            doc_type: {
              'properties': {}
            }
        }
        index_config['mappings'][doc_type]['properties'] = self.mapping
        # get any old index currently aliased to "profiles" for deletion
        #   after the new index is populated
        old_indexes = es.indices.get_alias(
            index=self.config['indexName'], ignore_unavailable=True)
        old_index = list(old_indexes.keys())[0] if old_indexes else None
        # give the new index a unique name based on timestamp
        new_index = '{}_{}'.format(self.config['indexName'], int(time.time()))
        es.indices.create(index=new_index, body=index_config)
        return new_index, old_index

    def _update_indices(self, new_index, old_index):
        """
        Add "profiles" alias to newly created index, and delete the old index.

        See https://www.elastic.co/guide/en/elasticsearch/guide/current/index-aliases.html  # noqa
        """
        es = self.es_client
        actions = [
            {'add': {'index': new_index, 'alias': self.config['indexName']}},
        ]
        if old_index is not None:
            actions.append(
                {'remove': {
                    'index': old_index, 'alias': self.config['indexName']
                }}
            )
        es.indices.update_aliases(body={'actions': actions})
        if old_index is not None:
            es.indices.delete(index=old_index, ignore=[400, 404])

    def _get_obj_field(self, obj, field):
        """
        Process a MongoDB document object field for nicer filter names.

        E.g. where obj.audience.gender = 'mostly-female', replace the hyphen
        """
        field, key = field.split('.')
        val = None
        if field in obj:
            val = obj[field].get(key, None)
            if val is not None:
                if isinstance(val, str):
                    if val == 'unknown':
                        val = None
                    else:
                        val = val.replace('-', ' ')
                else:  # must be an array
                    val = [i.replace('-', ' ') for i in val]
        return val

    def index_document(self, obj, index):
        """Add document to the ES index."""
        es = self.es_client
        doc = {'id': obj['_id']}
        field_config = self.config['fields']
        for field in field_config:
            key = field.replace('.', '_')
            if '.' in field:
                doc[key] = self._get_obj_field(obj, field)
            else:
                value = obj.get(field, None)
                if value is None and 'default' in field_config[field]:
                    doc[key] = field_config[field]['default']
                else:
                    if field_config[field]['type'] == 'date':
                        # FIXME: this is a bit of a kludge to help with
                        # fixture data
                        if isinstance(value, str):
                            value = dateutil.parser.parse(value)
                        doc[key] = value.strftime('%Y-%m-%d'),
                    elif 'transform' in field_config[field]:
                        doc[key] = field_config[field]['transformInstance'].transform(value)  # noqa
                    else:
                        doc[key] = value
        es.index(
            index=index,
            doc_type=self.config['collection'],
            body=doc,
            id=doc['id']
        )

    def index_all_documents(self, new_index):
        """Index all docs that meet filter criteria from DB."""
        collection = self.db_client[self.config['collection']]
        results = collection.find(
            self.config['collectionFilter'],
            list(self.config['fields'].keys())
        ).sort('createdAt', pymongo.DESCENDING).limit(10)
        ct = 0
        for result in results:
            try:
                self.index_document(result, new_index)
                ct += 1
            # TODO: what are the possible specific exceptions
            except Exception as exc:
                logging.warning(
                    'Failed to create document for %s. Error: %s',
                    result['_id'], exc
                )
        logging.info('Index all complete: {} docs'.format(ct))

    def index_document_by_id(self, obj_id):
        """Index a document with the given primary key."""
        collection = self.db_client[self.config['collection']]
        result = collection.find_one({'_id': obj_id})
        self.index_document(result, self.config['indexName'])
        logging.info('Indexed document ID: {}'.format(obj_id))

    def reindex(self):
        """Reindex the entire index."""
        new_index, old_index = self._create_index()
        self.index_all_documents(new_index)
        self._update_indices(new_index, old_index)
