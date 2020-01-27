import asyncio

from gecko_scraper import GeckoScraper
from findlive import findlive


async def handle_request(reader, writer):
    data = await reader.read(1000)
    loop = asyncio.get_event_loop()

    request = data.decode()

    s_requets = request.split()
    task = s_requets[0]

    if task == 'findlive':
        param = s_requets[1].replace('_', ' ')

        loop.create_task(findlive(param))


async def main():
    server = await asyncio.start_server(
        handle_request, '127.0.0.1', 15666)

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
