from slugify import slugify
from dgp.core import BaseDataGenusProcessor
from dataflows import Flow, dump_to_path, add_field, update_package
from dataflows_normalize import normalize_to_db, NormGroup
from dgp.config.consts import CONFIG_TAXONOMY_ID, CONFIG_URL, \
        CONFIG_MODEL_MAPPING, RESOURCE_NAME, CONFIG_PUBLISH_ALLOWED, \
        CONFIG_TAXONOMY_CT, CONFIG_PRIMARY_KEY
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

    def ref_column(self, prefix):
        return '{}_id'.format(prefix)

    def id_column(self):
        return 'id'

    def slugify(self, x):
        return slugify(x, separator='_')

    def column(self, x):
        return x.replace(':', '-')

    def fetch_label(self, code_ct):
        label_ct = None
        for ct in self.config.get(CONFIG_TAXONOMY_CT):
            if ct.get('labelOf'):
                if code_ct.startswith(ct['labelOf']):
                    label_ct = ct['name']
                    break
        if label_ct is not None:
            for m in self.config.get(CONFIG_MODEL_MAPPING):
                if m.get('columnType') == label_ct:
                    return m

    def fetch_datatype(self, ct):
        for item in self.config.get(CONFIG_TAXONOMY_CT):
            if item['name'] == ct:
                return item.get('dataType', 'string')

    def flow(self):
        steps = []
        if not self.config.get(CONFIG_PUBLISH_ALLOWED):
            return None
        logger.info('Publisher Flow Preparing')
        if self.output_datapackage:
            logger.info('Publisher Flow: Dump To Path Denorm...')
            steps.extend([
                dump_to_path(self.output_datapackage)
            ])
        if self.output_db:
            logger.info('Publisher Flow: Dump To Db...')
            primary_key = self.config.get(CONFIG_PRIMARY_KEY)
            mapping = self.config.get(CONFIG_MODEL_MAPPING)
            for m in mapping:
                if 'columnType' in m and m['columnType']:
                    m['column'] = self.column(m['columnType'])
                    m['slug'] = self.slugify(m['title'])
                    m['hierarchy'] = self.slugify(m['columnType'].split(':')[0])
                    m['primaryKey'] = m['columnType'] in primary_key
                    m['measure'] = m['hierarchy'] == 'value'
                    m['label'] = self.fetch_label(m['columnType'])
                    m['dataType'] = self.fetch_datatype(m['columnType'])
            prefixes = set(
                m['hierarchy']
                for m in mapping
                if m.get('measure') is False
            )
            db_table = '{}_{}'.format(
                self.config.get(CONFIG_TAXONOMY_ID),
                self.config.get(CONFIG_EXTRA_METADATA_DATASET_NAME),
            )
            prefixed = dict(
                (p, list(filter(lambda m: m.get('hierarchy') == p, mapping)))
                for p in prefixes
            )
            groups = [
                NormGroup([
                        m['column']
                        for m in prefixed_items
                    ], self.ref_column(prefix), self.id_column(),
                    db_table='{}_{}'.format(db_table, prefix))
                for prefix, prefixed_items in prefixed.items()
            ]
            babbage_model = dict(
                dimensions=dict(
                    (m['slug'], dict(
                        label=m['title'],
                        key_attribute=m['slug'],
                        attributes=dict([
                            (m['slug'], dict(
                                column=m['column'],
                                label=m['title'],
                                type=m['dataType'],
                            ))
                        ] + ([
                            (m['label']['slug'], dict(
                                column=m['label']['column'],
                                label=m['label']['title'],
                                type=m['label']['dataType'],
                            ))
                        ] if m.get('label') else [])),
                        join_column=[
                            self.ref_column(m['hierarchy']),
                            self.id_column()
                        ],
                        **(dict(
                            label_attribute=m['label']['slug']
                        ) if m.get('label') else {})
                    ))
                    for m in self.config.get(CONFIG_MODEL_MAPPING)
                    if m.get('measure') is False and m.get('primaryKey') is True
                ),
                fact_table=db_table,
                measures=dict(
                    (
                        m['slug'],
                        dict(
                            column=m['column'],
                            label=m['title'],
                            type='number'
                        )
                    )
                    for m in self.config.get(CONFIG_MODEL_MAPPING)
                    if m.get('measure') is True
                ),
                hierarchies=dict(
                    (prefix, dict(
                        label=prefix,
                        levels=[
                            m['slug']
                            for m in prefixed_items
                            if m.get('primaryKey') is True
                        ]
                    ))
                    for prefix, prefixed_items in prefixed.items()
                ),
            )
            steps.append(
                update_package(babbage_model=babbage_model)
            )
            source = self.config.get(CONFIG_URL)
            logger.info('Publisher Flow: _source Handling...')
            steps.extend([
                add_field('_source', 'string', source),
                append_to_primary_key(['_source']),
                clear_by_source(self.lazy_engine(), db_table, source),
            ])
            logger.info('Publisher Flow: Normalize...')
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
                logger.info('Publisher Flow: Dump To Path Norm...')
                steps.extend([
                    dump_to_path(self.output_datapackage + '-norm')
                ])
        if self.output_es:
            logger.info('Publisher Flow: ES...')
            steps.extend([
                self.update_es()
            ])
        logger.info('Publisher Flow Prepared')
        return Flow(*steps)
