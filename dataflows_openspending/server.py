import logging

from aiohttp import web

from .os_dgp_server import OsDgpServer

app = web.Application()
app.add_subapp('/api', OsDgpServer())

logging.getLogger().setLevel(logging.INFO)

if __name__ == "__main__":
    web.run_app(app, host='127.0.0.1', port=8000, access_log=logging.getLogger())
