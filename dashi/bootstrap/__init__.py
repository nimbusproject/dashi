import os
import sys
import logging
import logging.config

from copy import copy

from config import Config

DEFAULT_CONFIG_FILES = [
    'config/service.yml',
    'config/service.local.yml',
    ]
LOGGING_CONFIG_FILES = [
    'config/logging.yml',
    'config/logging.local.yml',
    ]

class Service(object):
    """Base class for services. Meant to be subclassed

    """

    def __init__(self, config_files=DEFAULT_CONFIG_FILES, logging_config_files=LOGGING_CONFIG_FILES):

        self.CFG = Config(config_files).data
        self.LOGGING_CFG = Config(logging_config_files).data
        
        self.CFG['cli_args'] = self._parse_argv()


    def get_logger(self, name=None):
        """set up logging for a service using the py 2.7 dictConfig
        """

        if not name:
            name = self.__class__.__name__

        logger = logging.getLogger(name)

        # Make log directory if it doesn't exist
        for handler in self.LOGGING_CFG.get('handlers', {}).itervalues(): 
            if 'filename' in handler: 
                log_dir = os.path.dirname(handler['filename']) 
                if not os.path.exists(log_dir): 
                    os.makedirs(log_dir) 
        try:
            #TODO: This requires python 2.7
            logging.config.dictConfig(self.LOGGING_CFG)
        except AttributeError:
            print >> sys.stderr, '"logging.config.dictConfig" doesn\'t seem to be supported in your python'
            raise

        return logger


    def _parse_argv(self, argv=copy(sys.argv)):
        """return argv as a parsed dictionary, looks like the following:

        app --option1 likethis --option2 likethat --flag

        ->

        {'option1': 'likethis', 'option2': 'likethat', 'flag': True}
        """

        cli_args = {}
        while argv:
            arg = argv[0]
            try:
                maybe_val = argv[1]
            except IndexError:
                maybe_val = None

            if arg[0] == '-':
                key = arg.lstrip('-')
                if not maybe_val or maybe_val[0] == '-':
                    val = True
                    argv = argv[1:]
                else:
                    val = maybe_val
                    argv = argv[2:]
                cli_args[key] = val
            else:
                #skip arguments that aren't preceded with -
                argv = argv[1:]

        return cli_args
