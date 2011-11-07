import traceback
import uuid
import sys

from kombu.connection import BrokerConnection
from kombu.messaging import Consumer
from kombu.pools import connections, producers
from kombu.entity import Queue, Exchange
from kombu.common import maybe_declare

import dashi.util

log = dashi.util.get_logger()

class DashiConnection(object):

    def __init__(self, name, uri, exchange_name):
        self._conn = BrokerConnection(uri)
        self._name = name
        self._exchange_name = exchange_name
        self._exchange = Exchange(name=exchange_name, type='topic',
                                  durable=False, auto_delete=True) # TODO parameterize

        self._consumer_conn = None
        self._consumer = None

    def fire(self, name, operation, **kwargs):
        """Send a message without waiting for a reply
        """
        d = dict(op=operation, args=kwargs)

        with producers[self._conn].acquire(block=True) as producer:
            maybe_declare(self._exchange, producer.channel)
            producer.publish(d, routing_key=name, exchange=self._exchange_name)

    def call(self, name, operation, **kwargs):
        """Send a message and wait for reply
        """

        # create a direct exchange and queue for the reply. This may end up
        # being a bottleneck for performance: each rpc call gets a brand new
        # direct exchange and exclusive queue. However this approach is used
        # in nova.rpc and seems to have carried them pretty far. If/when this
        # becomes a bottleneck we can set up a long-lived backend queue and
        # use correlation_id to deal with concurrent RPC calls. See:
        #   http://www.rabbitmq.com/tutorials/tutorial-six-python.html
        msg_id = uuid.uuid4().hex
        exchange = Exchange(name=msg_id, type='direct',
                            durable=False, auto_delete=True)

        # check out a connection from the pool
        with connections[self._conn].acquire(block=True) as conn:
            channel = conn.channel()
            queue = Queue(channel=channel, name=msg_id, exchange=exchange,
                    routing_key=msg_id, exclusive=True, durable=False,
                    auto_delete=True)
            queue.declare()

            messages = []

            def _callback(body, message):
                messages.append(body)
                message.ack()

            consumer = Consumer(channel=channel, queues=[queue],
                callbacks=[_callback])

            d = dict(op=operation, args=kwargs)
            headers = {'reply-to' : msg_id}

            with producers[self._conn].acquire(block=True) as producer:
                producer.publish(d, routing_key=name, headers=headers)

            with consumer:
                # only expecting one event
                conn.drain_events()

            msg_body = messages[0]
            if msg_body.get('error'):
                raise Exception(*msg_body['error'])
            else:
                return msg_body.get('result')

    def reply(self, msg_id, body):
        with producers[self._conn].acquire(block=True) as producer:
            producer.publish(body, routing_key=msg_id, exchange=msg_id)

    def handle(self, operation, operation_name=None):
        if not self._consumer:
            self._consumer_conn = connections[self._conn].acquire()
            self._consumer = DashiConsumer(self, self._consumer_conn,
                    self._name, self._exchange)
        self._consumer.add_op(operation_name or operation.__name__, operation)

    def consume(self):
        self._consumer.consume()


class DashiConsumer(object):
    def __init__(self, dashi, connection, name, exchange):
        self._dashi = dashi
        self._conn = connection
        self._name = name
        self._exchange = exchange

        self._channel = None
        self._ops = {}

        self.connect()

    def connect(self):
        self._channel = self._conn.channel()

        self._queue = Queue(channel=self._channel, name=self._name,
                exchange=self._exchange, routing_key=self._name)
        self._queue.declare()

        self._consumer = Consumer(self._channel, [self._queue],
                callbacks=[self._callback])
        self._consumer.consume()

    def consume(self):
        while True:
            self._conn.drain_events()

    def _callback(self, body, message):
        reply_to = None
        ret = None
        err = None
        try:
            reply_to = message.headers.get('reply-to')

            try:
                op = str(body['op'])
                args = body.get('args')
            except Exception, e:
                log.warn("Failed to interpret message body: %s", body,
                         exc_info=True)
                raise BadRequestError("Invalid request: %s" % str(e))

            op_fun = self._ops.get(op)
            if not op_fun:
                raise UnknownOperationError("Unknown operation: %s", op)

            ret = op_fun(**args)

        except Exception:
            err = sys.exc_info()
        finally:
            if reply_to:
                if err:
                    tb = traceback.format_exception(*err)
                    err = (err[0].__name__, str(err[1]), tb)
                reply = dict(result=ret, error=err)
                self._dashi.reply(reply_to, reply)

            message.ack()

    def add_op(self, name, fun):
        if not callable(fun):
            raise ValueError("operation function must be callable")
        
        self._ops[name] = fun


class DashiError(Exception):
    pass

class BadRequestError(DashiError):
    pass

class UnknownOperationError(DashiError):
    pass
