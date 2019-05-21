import yaml
import json

import click

from dgp.core import Config, Context, BaseDataGenusProcessor
from dgp.taxonomies.registry import TaxonomyRegistry
from dgp.genera import SimpleDGP, LoaderDGP, TransformDGP, EnricherDGP
from dgp.config.consts import CONFIG_URL, CONFIG_ENCODING, CONFIG_TAXONOMY_ID, CONFIG_HEADER_FIELDS,\
        CONFIG_MODEL_MAPPING, RESOURCE_NAME

from dataflows import Flow, dump_to_path
from dataflows_normalize import normalize_to_db, NormGroup
from slugify import slugify


CONFIG_EXTRA_METADATA_DATASET_NAME = 'extra.metadata.dataset-name'
CONFIG_EXTRA_METADATA_REVISION = 'extra.metadata.revision'
CONFIG_EXTRA_RESOURCE_NAME = 'extra.resource-name'
CONFIG_EXTRA_PRIVATE = 'extra.private'
CONFIG_EXTRA_METADATA_TITLE = 'extra.metadata.title'


class Publisher(BaseDataGenusProcessor):

    def __init__(self, config, context, output_datapackage, output_db, output_es):
        super().__init__(config, context)
        self.output_datapackage = output_datapackage
        self.output_db = output_db
        self.output_es = output_es

    def update_es(self):
        # TODO
        pass

    def flow(self):
        steps = []
        if self.output_datapackage:
            steps.extend([
                dump_to_path(self.output_datapackage)
            ])
        if self.output_db:
            prefixes = set(
                x['columnType'].split(':')[0]
                for x in self.config.get(CONFIG_MODEL_MAPPING)
                if 'columnType' in x
            )
            prefixes.discard('value')
            db_table = '{}_{}'.format(
                self.config.get(CONFIG_TAXONOMY_ID),
                self.config.get(CONFIG_EXTRA_METADATA_DATASET_NAME),
            )
            groups = [
                NormGroup([
                        x['name'].replace(':', '-')
                        for x in self.config.get(CONFIG_MODEL_MAPPING)
                        if 'columnType' in x and x['columnType'].split(':')[0] == prefix
                    ], '{}_id'.format(prefix), 'id',
                    db_table='{}_{}'.format(db_table, prefix))
                for prefix in prefixes
            ]
            steps.extend([
                normalize_to_db(
                    groups,
                    db_table,
                    RESOURCE_NAME
                )
            ])
        if self.output_es:
            steps.extend([
                self.update_es()
            ])
        return Flow(*steps)


def convert_source_spec(source_spec, taxonomy_id):
    for source in source_spec['sources']:
        config = Config()
        context = Context(config, TaxonomyRegistry('taxonomies/index.yaml'))

        config.set(CONFIG_URL, source['url'])
        if 'encoding' in source:
            config.set(CONFIG_ENCODING, source['encoding'])
        config.set(CONFIG_TAXONOMY_ID, taxonomy_id)

        dgp = SimpleDGP(config, context)
        dgp.analyze()

        headers = config.get(CONFIG_HEADER_FIELDS)

        mapping = []
        for header in headers:
            found = False
            for field in source_spec['fields']:
                aliases = set([field['header']] + field.get('aliases', []))
                if header in aliases:
                    mapping.append(dict(
                        name=header,
                        header=field['header'],
                        title=field.get('title', field['header']),
                        columnType=field['columnType'],
                        options=field.get('options', {})
                    ))
                    found = True
                    break
            if not found:
                print('Failed to find mapping for header', header)
        assert len(mapping) == len(headers)
        config.set(CONFIG_MODEL_MAPPING, mapping)

        config.set('extra.deduplicate', source_spec.get('deduplicate') is True)

        title = source_spec['title']
        dataset_name = source_spec.get('dataset-name', title)
        dataset_name = slugify(dataset_name, separator='_').lower()
        resource_name = source_spec.get('resource-name', dataset_name)
        revision = source_spec.get('revision', 0)
        private = source_spec.get('private') is not False

        config.set(CONFIG_EXTRA_METADATA_TITLE, title)
        config.set(CONFIG_EXTRA_METADATA_DATASET_NAME, dataset_name)
        config.set(CONFIG_EXTRA_METADATA_REVISION, revision)
        config.set(CONFIG_EXTRA_RESOURCE_NAME, resource_name)
        config.set(CONFIG_EXTRA_PRIVATE, private)

        dgp = SimpleDGP(config, context)
        if not dgp.analyze():
            for error in dgp.errors:
                print(error)
            break
        else:
            yield config


def process_source(config, output_datapackage, output_db, output_es):
    context = Context(config, TaxonomyRegistry('taxonomies/index.yaml'))

    publisher = Publisher(config, context, output_datapackage, output_db, output_es)

    dgp = SimpleDGP(
        config, context,
        steps=[
            LoaderDGP,
            TransformDGP,
            EnricherDGP,
            publisher
        ]
    )
    assert not dgp.errors, str(dgp.errors)
    flow = dgp.flow()
    assert flow is not None
    flow.process()


@click.command()
@click.option('--source-spec', default=None, help='source spec yaml file')
@click.option('--config', default=None, help='config json file')
@click.option('--taxonomy', default='fiscal', help='selected taxonomy to use')
@click.option('--output-datapackage', default=None, help='path for writing output as datapackage')
@click.option('--output-db', default=None, help='database connection string to write to')
@click.option('--output-es', default=None, help='elasticsearch connection string to update')
def main(source_spec, config, taxonomy,
         output_datapackage, output_db, output_es):

    if not source_spec and not config:
        raise click.UsageError('Expecting to see either source-spec of config')

    configs = False
    if source_spec:
        source_spec = yaml.load(open(source_spec))
        configs = convert_source_spec(source_spec, taxonomy)
    elif config:
        configs = [json.load(open(config))]

    for config in configs:
        process_source(config, output_datapackage, output_db, output_es)
        break


if __name__ == '__main__':
    main()
