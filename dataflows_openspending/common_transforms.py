from dataflows import Flow, add_computed_field, delete_fields, \
    printer, set_type, update_package, join_with_self

from dgp.core.base_enricher import ColumnTypeTester, ColumnReplacer, \
        DatapackageJoiner, enrichments_flows, BaseEnricher, DuplicateRemover
from dgp.config.consts import RESOURCE_NAME, CONFIG_PRIMARY_KEY, CONFIG_MODEL_MAPPING
from dgp_server.log import logger

class LoadMetadata(BaseEnricher):

    def test(self):
        return self.config.get('extra.metadata')

    def postflow(self):
        metadata = self.config._unflatten.get('extra.metadata')
        return Flow(
            update_package(**metadata)
        )


class Deduplicator(BaseEnricher):

    def test(self):
        logger.info('DEDPULICATING %r', self.config.get('extra.deduplicate'))
        return self.config.get('extra.deduplicate')

    def postflow(self):
        key_field_names = [
            ct.replace(':', '-')
            for ct in self.config.get(CONFIG_PRIMARY_KEY)
        ]
        used = set()

        def dedup(rows):
            if rows.res.name == RESOURCE_NAME:
                for row in rows:
                    key = tuple(row.get(k) for k in key_field_names)
                    if key not in used:
                        used.add(key)
                        yield row
            else:
                yield from rows

        steps = [
            dedup,
        ]
        logger.info('DEDPULICATING with KEYS %r', key_field_names)
        f = Flow(*steps)
        return f


# class Deduplicator(BaseEnricher):

#     def test(self):
#         logger.info('DEDPULICATING %r', self.config.get('extra.deduplicate'))
#         return self.config.get('extra.deduplicate')

#     def postflow(self):
#         key_field_names = [
#             ct.replace(':', '-')
#             for ct in self.config.get(CONFIG_PRIMARY_KEY)
#         ]
#         value_field_names = [
#             mapping['columnType'].replace(':', '-')
#             for mapping in self.config.get(CONFIG_MODEL_MAPPING)
#             if ('columnType' in mapping and
#                 mapping['columnType'].split(':')[0] == 'value')
#         ]
#         steps = [
#             join_with_self(
#                 RESOURCE_NAME,
#                 key_field_names,
#                 {
#                     **dict((f, {}) for f in key_field_names),
#                     **dict((f, dict(aggregate='sum')) for f in value_field_names),
#                     '*': dict(aggregate='last')
#                 }
#             ),
#         ]
#         logger.info('DEDPULICATING with KEYS %r', key_field_names)
#         f = Flow(*steps)
#         return f


class AddBabbageModel(BaseEnricher):

    def test(self):
        return True

    def postflow(self):
        db_tables = parameters['db-tables']
        model = dp['model']
        field_types = dict((x['slug'], x['type']) for x in dp['resources'][-1]['schema']['fields'])

        bbg_hierarchies = {}
        bbg_dimensions = {}
        bbg_measures = {}

        # Iterate on dimensions
        for hierarchy_name, h_props in model['dimensions'].items():

            # Append to hierarchies
            hierarchy_name = slugify(hierarchy_name, separator='_')
            hierarchy = dict(
                label=hierarchy_name,
                levels=h_props['primaryKey']
            )
            bbg_hierarchies[hierarchy_name] = hierarchy

            # Get all hierarchy columns
            attributes = h_props['attributes']
            attributes = list(attributes.items())

            # Separate to codes and labels
            codes = dict(filter(lambda x: 'labelfor' not in x[1], attributes))
            labels = dict(map(lambda y: (y[1]['labelfor'], y[1]), 
                            filter(lambda x: 'labelfor' in x[1], attributes)))

            # For each code, create a babbage dimension
            for fieldname, attribute in codes.items():
                dimension_name = fieldname

                bbg_attributes = {
                    fieldname: dict(
                        column='.'.join([db_tables[hierarchy_name], fieldname]),
                        label=attribute.get('title', attribute['source']),
                        type=field_types[fieldname]
                    )
                }
                bbg_dimension = dict(
                    attributes=bbg_attributes,
                    key_attribute=fieldname,
                    label=attribute.get('title'),
                    join_column=[hierarchy_name+'_id', ID_COLUMN_NAME]
                )

                label = labels.get(fieldname)
                if label is not None:
                    fieldname = label['source']
                    attribute = label
                    bbg_attributes.update({
                        fieldname: dict(
                            column='.'.join([db_tables[hierarchy_name], fieldname]),
                            label=attribute.get('title', attribute['source']),
                            type=field_types[fieldname]
                        )
                    })
                    bbg_dimension.update(dict(
                        label_attribute=fieldname
                    ))
                bbg_dimensions[dimension_name] = bbg_dimension

        # Iterate on measures
        for measurename, measure in model['measures'].items():
            bbg_measures[measurename] = dict(
                column=measurename,
                label=measure.get('title', attribute['source']),
                type=field_types[measurename]
            )

        dp['babbageModel'] = dict(
            fact_table = db_tables[''],
            dimensions = bbg_dimensions,
            hierarchies = bbg_hierarchies,
            measures = bbg_measures
        )

        return dp


def flows(config, context):
    return enrichments_flows(
        config, context,
        Deduplicator,
        LoadMetadata,
        # AddBabbageModel,
    )
