import traceback
import uuid

from kombu.connection import BrokerConnection
from kombu.messaging import Consumer
from kombu.pools import connections, producers
from kombu.entity import Queue, Exchange

import sys

class Dashi(object):

    def __init__(self, name, uri, exchange_name):
        self._conn = BrokerConnection(uri)
        self._name = name
        self._exchange_name = exchange_name

        self._consumer_conn = None
        self._consumer = None

    def fire(self, name, op, *args, **kwargs):
        d = dict(op=op, args=args, kwargs=kwargs)

        with producers[self._conn].acquire(block=True) as producer:
            producer.publish(d, routing_key=name)

    def call(self, name, op, *args, **kwargs):
        # create a direct exchange and queue for the reply
        # TODO probably better to pool these or something?
        msg_id = uuid.uuid4().hex
        exchange = Exchange(name=msg_id, type='direct',
                durable=False, auto_delete=True) #TODO parameterize

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

            d = dict(op=op, args=args, kwargs=kwargs)
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
            producer.publish(body, routing_key=msg_id)

    def handle(self, op, opname=None):
        if not self._consumer:
            self._consumer_conn = connections[self._conn].acquire()
            self._consumer = DashiConsumer(self, self._consumer_conn,
                    self._name, self._exchange_name)
            self._consumer.add_op(opname or op.__name__, op)

    def consume(self):
        self._consumer.consume()


class DashiConsumer(object):
    def __init__(self, dashi, connection, name, exchange_name):
        self._dashi = dashi
        self._conn = connection
        self._name = name

        self._exchange = Exchange(name=exchange_name, type='topic',
                durable=False, auto_delete=True) #TODO parameterize

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
        reply_to = message.headers.get('reply-to')

        #TODO error handling for message format
        op = body['op']
        args = body['args']
        kwargs = body['kwargs']

        #TODO error handling for unknown op
        op_fun = self._ops[op]

        ret, err = None, None
        try:
            ret = op_fun(*args, **kwargs)
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
        self._ops[name] = fun



