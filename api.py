import socket
import argparse


# TBM = TheBetsMaster
class TBMApi:
    def __init__(self, port=15555, gecko_port=15666):
        self.port = port
        self.gecko_port = gecko_port

    @staticmethod
    def send_request(request, port):
        if isinstance(request, bytes):
            req = request
        elif isinstance(request, str):
            req = request.encode()

        try:
            sock = socket.socket()
            sock.connect(('localhost', port))
            sock.send(req)
        except:
            print('[!] An exception occurs while sending request =(')
        finally:
            sock.close()

    def parse(self, url):
        TBMApi.send_request('parse %s' % url, self.port)

    def deltask(self, task):
        TBMApi.send_request('deltask %s' % task, self.port)

    def findlive(self, match_name):
        TBMApi.send_request('findlive %s' % match_name, self.gecko_port)

def init_argparser():
    argparser = argparse.ArgumentParser()

    argparser.add_argument('--parse', action='store')
    argparser.add_argument('--deltask', action='store')
    argparser.add_argument('--findlive', action='store')

    return argparser

if __name__ == '__main__':
    api = TBMApi()

    argparser = init_argparser()
    args = argparser.parse_args()

    if args.findlive:
        match_name = args.findlive
        api.findlive(match_name)

    if args.deltask:
        task = args.deltask
        api.deltask(task)

    if args.parse:
        api.parse(args.parse)
