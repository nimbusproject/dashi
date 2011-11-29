try:
    import gevent
    import gevent.monkey
except:
    #gevent not available
    pass

import os
import sys
import logging
import logging.config

from copy import copy
from config import Config

from dashi import DashiConnection

DEFAULT_CONFIG_FILES = [
    'config/service.yml',
    'config/service.local.yml',
    ]
LOGGING_CONFIG_FILES = [
    'config/logging.yml',
    'config/logging.local.yml',
    ]

DEFAULT_EXCHANGE = "default_dashi_exchange"
DEFAULT_SERVICE_TOPIC = "dashiservice"

topic = DEFAULT_SERVICE_TOPIC

def configure(config_files=DEFAULT_CONFIG_FILES,
              logging_config_files=LOGGING_CONFIG_FILES):
    CFG = Config(config_files).data
    LOGGING_CFG = Config(logging_config_files).data
    
    CFG['cli_args'] = _parse_argv()

    return CFG


def dashi_connect(topic, CFG=None, amqp_uri=None):

    if not CFG and not amqp_uri:
        #TODO: Some other kind of exception?
        raise Exception("Must provide either a config object or an amqp_uri")

    if not amqp_uri:
        amqp_uri = "amqp://%s:%s@%s/%s" % (
                        CFG.server.amqp.username,
                        CFG.server.amqp.password,
                        CFG.server.amqp.host,
                        CFG.server.amqp.vhost,
                        )
    try:
        dashi_exchange = CFG.dashi.exchange
    except AttributeError:
        dashi_exchange = DEFAULT_EXCHANGE

    return DashiConnection(topic, amqp_uri, dashi_exchange)


def enable_gevent():
    """enables gevent and swaps out standard threading for gevent
    greenlet threading

    throws an exception if gevent isn't available
    """
    gevent.monkey.patch_all()
    _start_methods = _start_methods_gevent

def _start_methods(methods=[], join=True):
    from threading import Thread

    threads = []
    for method in methods:
        thread = Thread(target=method)
        thread.daemon = True
        threads.append(thread)
        thread.start()
    
    if not join:
        return threads

    try:
        while len(threads) > 0:
            for t in threads:
                t.join(1)
                if not t.isAlive():
                    threads.remove(t)
                         
    except KeyboardInterrupt:
        pass

def _start_methods_gevent(methods=[], join=True):

    greenlets = []
    for method in methods:
        glet = gevent.spawn(method)
        greenlets.append(glet)

    if join:
        try:
            gevent.joinall(greenlets)
        except KeyboardInterrupt, gevent.GreenletExit:
            gevent.killall(greenlets)
    else:
        return greenlets

def _parse_argv(argv=copy(sys.argv)):
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


def get_logger(name, CFG=None):
    """set up logging for a service using the py 2.7 dictConfig
    """

    logger = logging.getLogger(name)

    if CFG:
        # Make log directory if it doesn't exist
        for handler in CFG.get('handlers', {}).itervalues(): 
            if 'filename' in handler: 
                log_dir = os.path.dirname(handler['filename']) 
                if not os.path.exists(log_dir): 
                    os.makedirs(log_dir) 
        try:
            #TODO: This requires python 2.7
            logging.config.dictConfig(CFG)
        except AttributeError:
            print >> sys.stderr, '"logging.config.dictConfig" doesn\'t seem to be supported in your python'
            raise

    return logger
