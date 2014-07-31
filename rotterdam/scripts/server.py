import logging

from rotterdam.config import Config
from rotterdam.master import Master


def run():
    config = Config.create("server")
    config.load()

    if config.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    try:
        Master(config).run()
    except KeyboardInterrupt:
        pass
