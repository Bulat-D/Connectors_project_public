import aiohttp
import asyncio
from aiohttp import web
from data_processing import MeanReversionStrategy

class WebInterface():
    def __init__(self, server_host, server_port):
        pass

    async def run(self):
        while True:
            await asyncio.sleep(12)