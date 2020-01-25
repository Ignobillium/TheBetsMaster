import socket
import argparse


# TBM = TheBetsMaster
class TBMApi:
    def __init__(self, port=15555):
        self.port = port

    @staticmethod
    def send_request(request, port):
        try:
            sock = socket.socket()
            sock.connect(('localhost', port))
            sock.send(request)
        finally:
            sock.close()

    def parse(self, url):
        TBMApi.send_request('parse %s' % url, self.port)

    def deltask(self, task):
        TBMApi.send_request('deltask %s' % task, self.port)

    def findlive(self, match_name):
        TBMApi.send_request('findlive %s' % match_name, self.port)

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
