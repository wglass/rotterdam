
import argparse
import logging
import sys

from rotterdam.master import Master

parser = argparse.ArgumentParser(description="Rotterdam master process runner")

parser.add_argument(
    "config_file", type=str, help="Location of the config file to load"
)
parser.add_argument(
    "-d", "--debug", help="Enable debugging output", action="store_true"
)


def run():
    args = parser.parse_args(sys.argv[1:])

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    Master(args.config_file).run()
