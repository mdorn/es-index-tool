# -*- coding: utf-8 -*-

"""AWS lambda function for es_reindex."""

import json
import logging

from es_index_tool.main import ESIndexTool


def handler(event=None, context=None):
    """
    AWS Lambda handler for es_reindex: used for reindexing a single record
    that has been updated in the source DB.
    """
    try:
        params = json.loads(event['Records'][0]['Sns']['Message'])
    except:
        logging.error('Failed to get message from event: %s', event)
    else:
        id_ = params['id']
        tool = ESIndexTool()  # TODO: S3 bucket needs to be configured
        tool.index_document_by_id(id_)


if __name__ == "__main__":
    # test/example
    js = '{"id": "abc123"}'
    event = {'Records': [{'Sns': {'Message': js}}]}
    handler(event)
