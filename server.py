import asyncio
import logging
from datetime import datetime

from parse_league import parse_league
from parse_live import parse_live
from parse_all import parse_all
from _config import config

from cron_worker import CronWorker
from database_worker import DataBaseWorker


class TBMServerV:
    cron = CronWorker()
    dbw = DataBaseWorker(
        'data/for_models.sqlite3',
        'matches',
        'data/for_models_ended.sqlite3',
        'ended'
    )


async def handle_request(reader, writer):
    data = await reader.read(1000)
    loop = asyncio.get_event_loop()

    request = data.decode()

    print('[ ] Request [%s] recieved' % request)

    s_requets = request.split()
    task = s_requets[0]

    if task == 'parse':
        param = s_requets[1]

        if 'live' in param:
            loop.create_task(parse_live(param, dbw=TBMServerV.dbw))
        elif 'league' in param:
            loop.create_task(parse_league(param))
        elif 'all' in param:
            await parse_all(TBMServerV.cron)

    elif task=='deltask':
        param = s_requets[1]
        TBMServerV.cron.deltask(param)
        TBMServerV.cron.write()

    elif task == 'parse_league':
        param = s_requets[1]
        loop.create_task(parse_league(param))


def hello(port):
    hello_str = '''
* = * = * = * = * = * = * = * = * = * = * = * = * = * = * = * = * =
*
*   TBMServer by @ignobillium
*       version 1.0
*       %s
* = * = * = * = * = * = * = * = * = * = * = * = * = * = * = * = * =

listening port %s''' % (datetime.now().strftime('%Y'), port)
    return hello_str



if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)-8s [%(asctime)s] %(message)s',
        level=logging.DEBUG,
        filename = 'server.log')

    # TODO:
    #* check `Work in progress` up to start server

    port = config['port']

    print(hello(port))

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
