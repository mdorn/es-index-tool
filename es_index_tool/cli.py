# -*- coding: utf-8 -*-

"""Console script for es_reindex."""
import json
import sys
import click

from es_index_tool.main import ESIndexTool


@click.command()
@click.option('--handler', 'function', help='Non-default handler to run.')
@click.option('--json', 'js', help='Arguments in JSON format.')
def main(function, js):
    """Console script for es_reindex."""
    args = json.loads(js)
    config = args['config']
    # e.g. --json='{"config": "./es_index_tool/data/example_config.json"}'
    tool = ESIndexTool(config_path=config)
    if 'id' not in args:
        tool.reindex()
    else:
        # e.g., --json='{"id": "2kS98AsytSXb8prbH"}'
        id_ = args['id']
        tool.index_document_by_id(id_)
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
