import asyncio

from parse_league import parse_league
from parse_live import parse_live
from parse_all import parse_all


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


async def main():
    server = await asyncio.start_server(
        handle_request, '127.0.0.1', 8888)

    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()

    asyncio.get_event_loop().stop()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    loop.create_task(main())
    loop.run_forever()

    pending = asyncio.Task.all_tasks(loop=loop)
    group = asyncio.gather(*pending, return_exceptions=True)
    loop.run_until_complete(group)
    loop.close()
