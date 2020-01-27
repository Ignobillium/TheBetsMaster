import asyncio
import logging

import argparse

from gecko_scraper import GeckoScraper
from findlive import findlive
from _config import config


def init_argparse():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--findlive', action='store')
    return argparser


async def handle_request(reader, writer):
    data = await reader.read(1000)
    loop = asyncio.get_event_loop()

    request = data.decode()

    s_requets = request.split()
    task = s_requets[0]

    if task == 'findlive':
        param = s_requets[1].replace('_', ' ')

        loop.create_task(findlive(param))


if __name__ == "__main__":
    gecko_port = config['gecko_port']

    loop = asyncio.get_event_loop()
    logging.basicConfig(level=logging.INFO)

    server_gen = asyncio.start_server(handle_request, port=gecko_port)
    server = loop.run_until_complete(server_gen)

    logging.info('Listening established on {0}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print('User interruption')
        print()
        pass # Press Ctrl+C to stop
    finally:
        print('[@] closing server . . .')
        server.close()
        print('[@] closing server complete')

        print('[@] pending . . .')
        pending = asyncio.Task.all_tasks(loop=loop)
        group = asyncio.gather(*pending, return_exceptions=True)
        loop.run_until_complete(group)
        print('[@] pending complete')

        print('[@] closing event loop . . .')
        loop.close()
        print('[@] closing event loop complete')

        print('[@] Bye!)\n')
