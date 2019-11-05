import os

import datapackage
from dataflows import Flow, set_type
from dgp.config.consts import CONFIG_MODEL_MAPPING, CONFIG_TAXONOMY_CT
from dgp.core import BaseAnalyzer
from dgp_server.log import logger
from dataflows_openspending.common_transforms import flows as p_flows


def flows(config, context):
    return p_flows(config, context)


def analyzers(*_):
    return []
