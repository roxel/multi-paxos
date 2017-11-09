# -*- coding: utf-8 -*-

import argparse
import yaml

from paxos.client import Client
from paxos.server import Server

DESCRIPTION = 'Run multi-paxos nodes.'


parser = argparse.ArgumentParser(description=DESCRIPTION)

TYPE_CLIENT = 'client'
TYPE_SERVER = 'server'
MODE_READ = 'r'
MODE_WRITE = 'w'

# general
subparsers = parser.add_subparsers(help='Participant type', dest='type')
parser.add_argument(
    '-f', '--file', type=str, default='config.yml', dest='file',
    help="Server address (in YAML format)",
)

# client
parser_client = subparsers.add_parser(TYPE_CLIENT, help='Client')
parser_client.add_argument(
    'key', type=str,
    help="Database key for client's actions",
)
parser_client.add_argument(
    '-v', '--value', type=str, dest='value',
    help="Database value for client's write action",
)

# server
parser_server = subparsers.add_parser(TYPE_SERVER, help='Server')
parser_server.add_argument(
    'address', type=str,
    help="Server address",
)


def load_config(config_file):
    with open(config_file) as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return
    return config


if __name__ == "__main__":
    args = parser.parse_args()
    config = load_config(args.file)
    if not config:
        exit("Terminating: Missing config file.")
    if args.type == TYPE_CLIENT:
        if args.value:
            Client(servers=config['servers']).run(key=args.key, value=args.value)
        else:
            Client(servers=config['servers']).run(key=args.key)
    elif args.type == TYPE_SERVER:
        Server(servers=config['servers'], address=args.address).run()
