try:
    import gevent
    import gevent.monkey
except:
    #gevent not available
    pass

import os
import re
import sys
import logging
import logging.config

from copy import copy

from dashi import DashiConnection
from config import Config
from containers import dict_merge, DotDict

DEFAULT_CONFIG_FILES = [
    'config/service.yml',
    ]
LOGGING_CONFIG_FILES = [
    'config/logging.yml',
    ]

DEFAULT_EXCHANGE = "default_dashi_exchange"
DEFAULT_SERVICE_TOPIC = "dashiservice"

topic = DEFAULT_SERVICE_TOPIC

def configure(config_files=DEFAULT_CONFIG_FILES,
              logging_config_files=LOGGING_CONFIG_FILES,
              argv=sys.argv):

    cli_cfg, cli_cfg_files = _parse_argv(argv)

    config_files = config_files + cli_cfg_files

    CFG = Config(config_files).data
    CFG = dict_merge(CFG, cli_cfg)

    try:
        # Make log directory if it doesn't exist
        for handler in CFG.logging.get('handlers', {}).itervalues():
            if 'filename' in handler:
                log_dir = os.path.dirname(handler['filename'])
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
        try:
            #TODO: This requires python 2.7
            logging.config.dictConfig(CFG.logging)
        except AttributeError:
            msg = '"logging.config.dictConfig" doesn\'t seem to be supported '
            msg += 'in your python. Try Python 2.7.'
            print >> sys.stderr, msg
            raise
    except AttributeError:
        print >> sys.stderr, "No logging configured, continuing without."
        pass

    return CFG


def dashi_connect(topic, CFG=None, amqp_uri=None, sysname=None):

    if not CFG and not amqp_uri:
        #TODO: Some other kind of exception?
        raise Exception("Must provide either a config object or an amqp_uri")

    if not amqp_uri:
        if 'memory' in CFG.server:
            amqp_uri = "memory://%s" % (CFG.server.memory.name)
        else:
            try:
                port_str = ":%d" % (CFG.server.amqp.port)
            except AttributeError:
                port_str = ""
            amqp_uri = "amqp://%s:%s@%s%s/%s" % (
                        CFG.server.amqp.username,
                        CFG.server.amqp.password,
                        CFG.server.amqp.host,
                        port_str,
                        CFG.server.amqp.vhost,
                        )

    # force small polling interval for in memory transport. This is only
    # used in tests.
    if amqp_uri.startswith('memory://'):
        transport_options = dict(polling_interval=0.01)
    else:
        transport_options = None

    try:
        dashi_exchange = CFG.server.amqp.exchange
    except AttributeError:
        dashi_exchange = DEFAULT_EXCHANGE

    try:
        serializer = CFG.dashi.serializer
    except AttributeError:
        serializer = None

    try:
        ssl = CFG.server.amqp.ssl
        ssl = True
    except AttributeError:
        ssl = False

    try:
        _sysname = CFG.dashi.sysname
    except AttributeError:
        _sysname = None

    # argument overrides config value
    if sysname is not None:
        _sysname = sysname

    return DashiConnection(topic, amqp_uri, dashi_exchange,
            serializer=serializer, transport_options=transport_options, ssl=ssl,
            sysname=_sysname)


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

    cfg = DotDict()
    cfg_files = []

    argv = argv[1:] # Skip command name
    while argv:
        arg = argv.pop(0)

        # split up arg in format --arg=val
        key_val = re.split('=| ', arg)
        arg = key_val[0]
        try:
            val = key_val[1]
        except IndexError:
            if len(argv) > 0 and argv[0][0] != '-':
                val = argv.pop(0)
            else:
                # No val available, probably a flag
                val = None

        if arg[0] == '-':
            key = arg.lstrip('-')
            if not val:
                val = True
            new_cfg = _dict_from_dotted(key, val)
            cfg = dict_merge(cfg, new_cfg)
        else:
            if arg.endswith(".yml"):
                cfg_files.append(arg)

    return cfg, cfg_files

def _dict_from_dotted(key, val):
    """takes a key value pair like:
    key: "this.is.a.key"
    val: "the value"

    and returns a dictionary like:

    {"this":
        {"is":
            {"a":
                {"key":
                    "the value"
                }
            }
        }
    }
    """
    split_key = key.split(".")
    split_key.reverse()
    for key_part in split_key:
        new_dict = DotDict()
        new_dict[key_part] = val
        val = new_dict
    return val


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
