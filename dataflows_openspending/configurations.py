from aiohttp import web
from sqlalchemy import (
    MetaData, Table, Column,
    String, JSON
)

__all__ = ['configuration']


meta = MetaData()

configuration = Table(
    'configurations', meta,

    Column('source', String, primary_key=True),
    Column('snippets', JSON, nullable=True),
    Column('key_values', JSON, nullable=True),
    Column('config', JSON, nullable=True),
)


async def configs(request):
    configurations = []
    try:
        async with request.app['db'].acquire() as conn:
            configurations = await conn.execute(
                configuration.select()
            )
            configurations = await configurations.fetchall()
            configurations = [dict(x) for x in configurations]
    except Exception:
        print('EMPTY CONFIGS')
    res = {
        'configurations': configurations
    }
    return web.json_response(res)
