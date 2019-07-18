from dgp.core import BaseDataGenusProcessor
from dataflows import Flow, dump_to_path, add_field
from dataflows_normalize import normalize_to_db, NormGroup
from dgp.config.consts import CONFIG_TAXONOMY_ID, CONFIG_URL, \
        CONFIG_MODEL_MAPPING, RESOURCE_NAME, CONFIG_PUBLISH_ALLOWED
from .consts import CONFIG_EXTRA_METADATA_DATASET_NAME
from dgp_server.log import logger
from dgp_server.publish_flow import clear_by_source, append_to_primary_key


class PublisherDGP(BaseDataGenusProcessor):

    def __init__(self, config, context, output_datapackage,
                 output_db, lazy_engine, output_es):
        super().__init__(config, context)
        self.output_datapackage = output_datapackage
        self.output_db = output_db
        self.output_es = output_es
        self.lazy_engine = lazy_engine

    def update_es(self):

        def progress(res, count):
            for row in res:
                yield row
                if count['i'] % 1000 == 0:
                    logger.info('STATUS: PROGRESS %d', count['i'])
                count['i'] += 1

        def func(package):
            logger.info('STATUS: STARTING')
            yield package.pkg
            count = dict(i=0)
            for res in package:
                yield progress(res, count)
            logger.info('STATUS: DONE')
        return func

    def flow(self):
        steps = []
        if not self.config.get(CONFIG_PUBLISH_ALLOWED):
            return None
        if self.output_datapackage:
            steps.extend([
                dump_to_path(self.output_datapackage)
            ])
        if self.output_db:
            prefixes = set(
                x['columnType'].split(':')[0]
                for x in self.config.get(CONFIG_MODEL_MAPPING)
                if 'columnType' in x and x['columnType'] is not None
            )
            prefixes.discard('value')
            db_table = '{}_{}'.format(
                self.config.get(CONFIG_TAXONOMY_ID),
                self.config.get(CONFIG_EXTRA_METADATA_DATASET_NAME),
            )
            groups = [
                NormGroup([
                        x['columnType'].replace(':', '-')
                        for x in self.config.get(CONFIG_MODEL_MAPPING)
                        if 'columnType' in x and x['columnType'] is not None and x['columnType'].split(':')[0] == prefix
                    ], '{}_id'.format(prefix), 'id',
                    db_table='{}_{}'.format(db_table, prefix))
                for prefix in prefixes
            ]
            source = self.config.get(CONFIG_URL)
            steps.extend([
                add_field('_source', 'string', source),
                append_to_primary_key(['_source']),
                clear_by_source(self.lazy_engine(), db_table, source),
            ])
            steps.extend([
                normalize_to_db(
                    groups,
                    db_table,
                    RESOURCE_NAME,
                    self.output_db,
                    'append'
                ),
            ])
            if self.output_datapackage:
                steps.extend([
                    dump_to_path(self.output_datapackage + '-norm')
                ])
        if self.output_es:
            steps.extend([
                self.update_es()
            ])
        return Flow(*steps)
