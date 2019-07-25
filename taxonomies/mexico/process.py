import os

import datapackage
from dataflows import Flow, add_field
from dgp.config.consts import CONFIG_MODEL_MAPPING, CONFIG_TAXONOMY_CT
from dataflows_openspending.common_transforms import flows as p_flows


def values(config):
    values = [x
              for x in config.get(CONFIG_TAXONOMY_CT) if x['name'].startswith('value:')]
    existing_cts = set(x.get('columnType') for x in config.get(CONFIG_MODEL_MAPPING))
    steps = [
        add_field(x['name'].replace(':', '-'), 'number', **x.get('options', {}), columnType=x['name'])
        for x in values
        if x['name'] not in existing_cts
    ]
    return Flow(*steps)


def objeto_del_gasto(config):
    CT = dict([
        ('ID_CAPITULO', 'economic-classification:generic:level1:label'),
        ('DESC_CAPITULO', 'economic-classification:generic:level1:label'),
        ('ID_CONCEPTO', 'economic-classification:generic:level2:code'),
        ('DESC_CAPITULO', 'economic-classification:generic:level2:label'),
        ('ID_PARTIDA_GENERICA', 'economic-classification:generic:level3:code'),
        ('DESC_PARTIDA_GENERICA', 'economic-classification:generic:level3:label'),
        ('ID_PARTIDA_ESPECIFICA', 'economic-classification:generic:level4:code'),
        ('DESC_PARTIDA_ESPECIFICA', 'economic-classification:generic:level4:label'),
    ])
    CN = dict(
        (k, v.replace(':', '-'))
        for k, v in CT.items()
    )

    new_columns = [
        'DESC_CAPITULO', 'ID_PARTIDA_GENERICA', 'DESC_PARTIDA_GENERICA',
        'ID_PARTIDA_ESPECIFICA', 'DESC_PARTIDA_ESPECIFICA'
    ]

    steps = []
    existing_cts = set(x.get('columnType') for x in config.get(CONFIG_MODEL_MAPPING))
    steps.extend(
        add_field(CN[title], 'string', title=title, columnType=CT[title])
        for title in new_columns
        if CT[title] not in existing_cts
    )

    lookup = {}
    codes = datapackage.Package(
        os.path.join(os.path.dirname(__file__), 'objeto_del_gasto.datapackage.zip')
    )
    for resource in codes.resources:
        kind = resource.name
        lookup[kind] = {}
        for row in resource.iter(keyed=True):
            key = row[kind.upper().replace('Í', 'I')]
            value = row['DESCRIPCION']
            lookup[kind][key] = value

    def process(row):
        year = int(row['date-fiscal-year'])

        # Skip the LAST year of the dataset (currently 2016) it has split columns already
        if year < 2019:
            objeto = row[CN['ID_CONCEPTO']]
            if objeto:
                row[CN['ID_CAPITULO']] = objeto[0] + '000'
                row[CN['ID_CONCEPTO']] = objeto[:2] + '00'
                row[CN['DESC_CAPITULO']] = lookup['capitulo'].get(row[CN['ID_CAPITULO']])
                row[CN['DESC_CONCEPTO']] = lookup['concepto'].get(row[CN['ID_CONCEPTO']])

                nb_generica_digits = 4 if year in (2008, 2009, 2010) else 3

            if objeto and len(objeto) >= 4:
                row[CN['ID_PARTIDA_GENERICA']] = objeto[:nb_generica_digits]

            row[CN['DESC_PARTIDA_GENERICA']] = lookup['partida_generica'].get(row.get(CN['ID_PARTIDA_GENERICA']))

            if year not in (2008, 2009, 2010):
                if objeto and len(objeto) >= 5:
                    row[CN['ID_PARTIDA_ESPECIFICA']] = objeto
                    row[CN['DESC_PARTIDA_ESPECIFICA']] = \
                        lookup['partida_específica'].get(row.get(CN['ID_PARTIDA_ESPECIFICA']))

    steps.append(process)
    return Flow(*steps)


def flows(config, context):
    flows = p_flows(config, context)
    return flows[0], Flow(
        flows[1],
        objeto_del_gasto(config),
        values(config)
    )
