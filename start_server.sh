#!/bin/sh
gunicorn -w 4 -t 180 --bind 0.0.0.0:9000 dataflows_openspending.server:app --worker-class aiohttp.GunicornWebWorker
