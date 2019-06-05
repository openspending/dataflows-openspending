import os

from dgp_server.blueprint import DgpServer

from dataflows import Flow, update_resource, \
    set_primary_key, dump_to_sql
from dgp.core import Context, Config, BaseDataGenusProcessor
from dgp.config.consts import CONFIG_URL, CONFIG_TAXONOMY_ID

from .publisher import PublisherDGP


class ConfigStorerDGP(BaseDataGenusProcessor):

    def __init__(self, config, context, lazy_engine):
        super().__init__(config, context)
        self.lazy_engine = lazy_engine
        self.inner_publish_flow = lambda *_: None

    def collate_values(self, fields):
        def func(row):
            return dict((f, row[f]) for f in fields)
        return func

    def flow(self):
        TARGET = 'configurations'
        saved_config = self.config._unflatten()
        saved_config.setdefault('publish', {})['allowed'] = False

        return Flow(
            [
                dict(
                    source=self.config.get(CONFIG_URL),
                    snippets=[
                        '{}: {}'.format(
                            self.config.get(CONFIG_TAXONOMY_ID),
                            os.path.basename(self.config.get(CONFIG_URL))
                        )
                    ],
                    config=saved_config,

                )
            ],
            update_resource(-1, name=TARGET),
            set_primary_key(['source']),
            dump_to_sql(
                dict([
                    (TARGET, {
                        'resource-name': TARGET,
                        'mode': 'update'
                    })
                ]),
                engine=self.lazy_engine(),
            ),
        )


class OsDgpServer(DgpServer):

    def __init__(self):
        super().__init__(
            os.environ.get('BASE_PATH', '/var/dgp'),
            os.environ.get('DATABASE_URL'),
        )

    def publish_flow(self, config: Config, context: Context):
        return [
            PublisherDGP(config, context, None, self.db_url, 'xx'),
            ConfigStorerDGP(config, context, self.lazy_engine())
        ]
