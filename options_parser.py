
import os
import sys
import logging
from tornado.options import OptionParser, Error

class CustomOptionParser(OptionParser):

    def __init__(self, check_validity=True, **kwargs):
        super().__init__(**kwargs)
        if check_validity:
            self.add_parse_callback(lambda: self.is_valid_config())

    def _normalize_name(self, name):
        return name

    def parse_command_line(self, args=None, final=True, ignore_undefined=True):
        """Override the original parse commandline to ignore params that are not defined
        """
        if args is None:
            args = sys.argv
        remaining = []
        for i in range(1, len(args)):
            # All things after the last option are command line arguments
            if not args[i].startswith("-"):
                remaining = args[i:]
                break
            if args[i] == "--":
                remaining = args[i + 1:]
                break
            arg = args[i].lstrip("-")
            name, equals, value = arg.partition("=")
            if name not in self._options:
                if ignore_undefined:
                    remaining.append(arg)
                    continue
                self.print_help()
                raise Error("Unrecognized command line option: {0}".format(name))
            option = self._options[name]
            if not equals:
                if option.type == bool:
                    value = "true"
                else:
                    raise Error("Option {0} requires a value".format(name))
            try:
                option.parse(value)
            except ValueError as e:
                raise Error("Invalid value for {0} value : {1}".format(name, value))

        if final:
            self.run_parse_callbacks()

        return remaining

    def parse_config_file(self, path, *args, fail_silently=False, **kwargs):
        try:
            super().parse_config_file(path, *args, **kwargs)
        except Exception as e:
            if not fail_silently:
                raise e

    def define(self, name, *args, env_name=None, is_required=False, check=None, **kwargs):
        """Mimic define to provide new functionality

        is_required:        set if this option is required, used in is_valid_config
            is_required can also be a function that takes in the parser in the first param.
        env_name:           set the env name to be used to parse from ENVVAR.
                            if value is not specified, it will be set to name.upper()
        check               function to check if this value is valid.
            check takes in (options, value), where options is the parser and value is the value
            of the key. check function must return True for valid, and False for invalid values

            example: to check that the string is of the format A:B
            def check(options, value):
                values = value.split(":")
                if len(values) == 2:
                    return True
                return False
        """
        super().define(name, *args, **kwargs)
        self._options[name].is_required = is_required
        self._options[name].env_name = env_name or name.upper()
        self._options[name].check = check

    def is_valid_config(self, raise_error=True):
        for name, opt in self._options.items():
            value = getattr(self, name)
            is_required = opt.is_required(self) if callable(opt.is_required) else opt.is_required
            if is_required and value is None:
                if raise_error:
                    raise Error("Option {0} requires a value".format(name))
                else:
                    return False
            if opt.check is not None and not opt.check(self, value):
                if raise_error:
                    raise Error("Option {0} value {1} is invalid".format(name, value))
                else:
                    return False
        return True

    def parse_env_var(self, args=None, final=True):
        """Parse the options from env vars
        """
        if args is None:
            args = os.environ

        for name, opt in self._options.items():
            if opt.env_name in args:
                env_value = args.get(opt.env_name)
                if env_value == "" and opt.type == bool:
                    env_value = "true"
                try:
                    opt.parse(value)
                except ValueError as e:
                    raise Error("Invalid value for {0} value : {1}".format(name, value))

        if final:
            self.run_parse_callbacks()

