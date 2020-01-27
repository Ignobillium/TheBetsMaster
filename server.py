import asyncio
import logging

from parse_league import parse_league
from parse_live import parse_live
from parse_all import parse_all
from _config import config


async def handle_request(reader, writer):
    data = await reader.read(1000)
    loop = asyncio.get_event_loop()

    request = data.decode()

    s_requets = request.split()
    task = s_requets[0]

    if task == 'parse':
        param = s_requets[1]

        if 'live' in param:
            loop.create_task(parse_live(param))
        elif 'league' in param:
            loop.create_task(parse_league(param))
        elif 'all' in param:
            loop.create_task(parse_all())


if __name__ == "__main__":
    port = config['port']

    loop = asyncio.get_event_loop()
    logging.basicConfig(level=logging.INFO)

    server_gen = asyncio.start_server(handle_request, port=port)
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
