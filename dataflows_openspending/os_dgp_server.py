import os

from dgp_server.blueprint import DgpServer
from dgp_server.config_storer import ConfigStorerDGP

from dgp.core import Context, Config

from .publisher import PublisherDGP


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
