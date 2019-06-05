import yaml

import click

from dgp.core import Config, Context
from dgp.taxonomies.registry import TaxonomyRegistry
from dgp.genera import SimpleDGP, LoaderDGP, TransformDGP, EnricherDGP
from dgp.config.consts import CONFIG_URL, CONFIG_ENCODING, CONFIG_TAXONOMY_ID, CONFIG_HEADER_FIELDS,\
        CONFIG_MODEL_MAPPING

from slugify import slugify

from .consts import CONFIG_EXTRA_METADATA_DATASET_NAME, CONFIG_EXTRA_METADATA_REVISION, \
    CONFIG_EXTRA_METADATA_TITLE, CONFIG_EXTRA_PRIVATE, CONFIG_EXTRA_RESOURCE_NAME
from .publisher import PublisherDGP


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

    publisher_dgp = PublisherDGP(config, context, output_datapackage, output_db, output_es)

    dgp = SimpleDGP(
        config, context,
        steps=[
            LoaderDGP,
            TransformDGP,
            EnricherDGP,
            publisher_dgp
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
        source_spec_obj = yaml.load(open(source_spec))
        configs = convert_source_spec(source_spec_obj, taxonomy)
        for i, config in enumerate(configs):
            yaml.dump(config._unflatten(),
                      open('{}.{:02d}.yaml'.format(source_spec, i), 'w'),
                      default_flow_style=False, indent=2, allow_unicode=True, encoding='utf8')
    elif config:
        configs = [Config(config)]

    for config in configs:
        process_source(config, output_datapackage, output_db, output_es)
        break


if __name__ == '__main__':
    main()
