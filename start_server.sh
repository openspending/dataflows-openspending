#!/bin/sh
gunicorn -w 4 -t 180 --bind 0.0.0.0:8000 dataflows_openspending.server:app --worker-class aiohttp.GunicornWebWorker