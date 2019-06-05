from dgp.core import BaseDataGenusProcessor
from dataflows import Flow, dump_to_path
from dataflows_normalize import normalize_to_db, NormGroup
from dgp.config.consts import CONFIG_TAXONOMY_ID, \
        CONFIG_MODEL_MAPPING, RESOURCE_NAME, CONFIG_PUBLISH_ALLOWED
from .consts import CONFIG_EXTRA_METADATA_DATASET_NAME


class PublisherDGP(BaseDataGenusProcessor):

    def __init__(self, config, context, output_datapackage, output_db, output_es):
        super().__init__(config, context)
        self.output_datapackage = output_datapackage
        self.output_db = output_db
        self.output_es = output_es

    def update_es(self):

        def progress(res, count):
            for row in res:
                yield row
                if count['i'] % 1000 == 0:
                    print('STATUS: PROGRESS', count['i'])
                count['i'] += 1

        def func(package):
            print('STATUS: STARTING')
            yield package.pkg
            count = dict(i=0)
            for res in package:
                yield progress(res, count)
            print('STATUS: DONE')
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
            steps.extend([
                normalize_to_db(
                    groups,
                    db_table,
                    RESOURCE_NAME,
                    self.output_db
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
