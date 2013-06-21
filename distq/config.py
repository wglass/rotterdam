import ConfigParser


class Config(object):

    def __init__(self, filename):
        self.filename = filename

    def load(self):
        parser = ConfigParser.SafeConfigParser()
        parser.read(self.filename)

        for section_name in parser.sections():
            setattr(self, section_name, Section())

            for option_name, option_value in parser.items(section_name):
                if "." in option_value:
                    try:
                        option_value = parser.getfloat(
                            section_name, option_name
                        )
                        setattr(
                            getattr(self, section_name),
                            option_name, option_value
                        )
                        continue
                    except ValueError:
                        pass
                else:
                    try:
                        option_value = parser.getint(
                            section_name, option_name
                        )
                        setattr(
                            getattr(self, section_name),
                            option_name, option_value
                        )
                        continue
                    except ValueError:
                        pass
                try:
                    option_value = parser.getboolean(
                        section_name, option_name
                    )
                    setattr(
                        getattr(self, section_name),
                        option_name, option_value
                    )
                    continue
                except ValueError:
                    pass

                setattr(
                    getattr(self, section_name),
                    option_name, option_value
                )


class Section(object):
    pass
