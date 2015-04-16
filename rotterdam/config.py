import argparse
import ConfigParser
import importlib
import os
import pkg_resources


version = pkg_resources.get_distribution("rotterdam").version

import settings


DEFAULT_CONFIG_FILENAME = "/etc/rotterdam.conf"


class UnknownConfigError(Exception):
    pass


class Config(object):

    def __init__(self):
        self.settings = {}
        self.parser = None

    @classmethod
    def create(cls, settings_module_name):
        cfg = cls()

        try:
            cfg.load_settings_module(settings_module_name)
        except ImportError:
            raise UnknownConfigError(settings_module_name)

        cfg.create_parser()

        return cfg

    def load_settings_module(self, settings_module_name):
        importlib.import_module("." + settings_module_name, settings.__name__)

        self.load_settings()

    def load_settings(self):
        self.settings = {
            setting_instance.name: setting_instance
            for setting_instance in
            [setting_class() for setting_class in settings.classes]
        }

    def create_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-v", "--version", action="version",
            version="Rotterdam (version " + version + ")\n",
            help="Print the version and exit"
        )
        parser.add_argument("args", nargs="*", help=argparse.SUPPRESS)

        sorted_settings = sorted(self.settings, key=self.settings.__getitem__)

        for setting_name in sorted_settings:
            self.settings[setting_name].add_option(parser)

        self.parser = parser

    def load(self):
        args = self.parser.parse_args()

        if "config_file" in vars(args) and args.config_file:
            self.load_from_file(args.config_file)
        elif "ROTTERDAM_CONFIG_FILE" in os.environ:
            self.load_from_file(os.environ["ROTTERDAM_CONFIG_FILE"])
        elif os.path.exists(DEFAULT_CONFIG_FILENAME):
            self.load_from_file(DEFAULT_CONFIG_FILENAME)

        for arg_name, arg_value in vars(args).iteritems():
            if arg_value is None or arg_name == "args":
                continue

            try:
                self.set(arg_name.lower(), arg_value)
            except AttributeError:
                pass

        return args

    def load_from_file(self, filename):
        parser = ConfigParser.SafeConfigParser()
        parser.read(filename)

        for arg_name, arg_value in parser.items("rotterdam"):
            if arg_name in self.settings:
                self.set(arg_name, arg_value)

    def set(self, name, value):
        if name not in self.settings:
            raise AttributeError("No configuration setting for: %s" % name)

        self.settings[name].set(value)

    def __getattr__(self, name):
        if name not in self.settings:
            raise AttributeError("No configuration setting for: %s" % name)

        return self.settings[name].get()

    def __setattr__(self, name, value):
        if name != "settings" and name in self.settings:
            raise AttributeError("Setting attributes are read-only.")

        super(Config, self).__setattr__(name, value)

    def __contains__(self, name):
        return name in self.settings and self.settings[name].get() is not None

    def __str__(self):
        return str(self.settings)
