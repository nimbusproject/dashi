import socket
import threading
import traceback
import uuid
import sys
import logging

from kombu.connection import BrokerConnection
from kombu.messaging import Consumer
from kombu.pools import connections, producers
from kombu.entity import Queue, Exchange
from kombu.common import maybe_declare

log = logging.getLogger(__name__)

class DashiConnection(object):

    consumer_timeout = 1.0

    #TODO support connection info instead of uri

    def __init__(self, name, uri, exchange, durable=False, auto_delete=True, serializer=None):
        self._conn = BrokerConnection(uri)
        self._name = name
        self._exchange_name = exchange
        self._exchange = Exchange(name=exchange, type='topic',
                                  durable=durable, auto_delete=auto_delete)

        # visible attributes
        self.durable = durable
        self.auto_delete = auto_delete

        self._consumer_conn = None
        self._consumer = None

        self._serializer = serializer

    @property
    def name(self):
        return self._name

    def fire(self, name, operation, **kwargs):
        """Send a message without waiting for a reply
        """
        d = dict(op=operation, args=kwargs)

        with producers[self._conn].acquire(block=True) as producer:
            maybe_declare(self._exchange, producer.channel)
            producer.publish(d, routing_key=name, exchange=self._exchange_name)

    def call(self, name, operation, timeout=5, args=None, **kwargs):
        """Send a message and wait for reply

        @param name: name of destination service queue
        @param operation: name of service operation to invoke
        @param timeout: RPC timeout to await a reply
        @param args: dictionary of keyword args to pass to operation.
                     Use this OR kwargs.
        @param kwargs: additional args to pass to operation
        """

        if args:
            if kwargs:
                raise TypeError("specify args dict or keyword arguments, not both")
        else:
            args = kwargs


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
            queue = Queue(name=msg_id, exchange=exchange, routing_key=msg_id,
                          exclusive=True, durable=False, auto_delete=True)
            log.debug("declared call() reply queue %s", msg_id)

            messages = []

            def _callback(body, message):
                messages.append(body)
                message.ack()

            consumer = Consumer(conn, queues=(queue,), callbacks=(_callback,))
            consumer.declare()

            d = dict(op=operation, args=args)
            headers = {'reply-to' : msg_id}

            with producers[self._conn].acquire(block=True) as producer:
                maybe_declare(self._exchange, producer.channel)
                log.debug("sending call to %s:%s", name, operation)
                producer.publish(d, routing_key=name, headers=headers,
                                 exchange=self._exchange, serializer=self._serializer)

            with consumer:
                log.debug("awaiting call reply on %s", msg_id)
                # only expecting one event
                conn.drain_events(timeout=timeout)

            msg_body = messages[0]
            if msg_body.get('error'):
                raise_error(msg_body['error'])
            else:
                return msg_body.get('result')

    def reply(self, msg_id, body):
        with producers[self._conn].acquire(block=True) as producer:
            try:
                producer.publish(body, routing_key=msg_id, exchange=msg_id, serializer=self._serializer)
            except self._conn.channel_errors:
                log.exception("Failed to reply to msg %s", msg_id)

    def handle(self, operation, operation_name=None):
        if not self._consumer:
            self._consumer_conn = connections[self._conn].acquire()
            self._consumer = DashiConsumer(self, self._consumer_conn,
                    self._name, self._exchange)
        self._consumer.add_op(operation_name or operation.__name__, operation)

    def consume(self, count=None, timeout=None):
        self._consumer.consume(count, timeout)

    def cancel(self, block=True):
        if self._consumer:
            self._consumer.cancel(block=block)


class DashiConsumer(object):
    def __init__(self, dashi, connection, name, exchange):
        self._dashi = dashi
        self._conn = connection
        self._name = name
        self._exchange = exchange

        self._channel = None
        self._ops = {}
        self._cancelled = False
        self._consumer_lock = threading.Lock()

        self.connect()

    def connect(self):
        self._channel = self._conn.channel()

        self._queue = Queue(channel=self._channel, name=self._name,
                exchange=self._exchange, routing_key=self._name,
                durable=self._dashi.durable,
                auto_delete=self._dashi.auto_delete)
        self._queue.declare()

        self._consumer = Consumer(self._channel, [self._queue],
                callbacks=[self._callback])
        self._consumer.consume()

    def consume(self, count=None, timeout=None):

        # hold a lock for the duration of the consuming. this prevents
        # multiple consumers and allows cancel to detect when consuming
        # has ended.
        if not self._consumer_lock.acquire(False):
            raise Exception("only one consumer thread may run concurrently")

        try:
            if count:
                i = 0
                while i < count and not self._cancelled:
                    self._consume_one(timeout)
                    i += 1
            else:
                while not self._cancelled:
                    self._consume_one(timeout)
        finally:
            self._consumer_lock.release()
            self._cancelled = False

    def _consume_one(self, timeout=None):

        # do consuming in a busy-ish loop, checking for cancel. There doesn't
        # seem to be an easy way to interrupt drain_events other than the
        # timeout. This could probably be added to kombu if needed. In
        # practice cancellation is likely infrequent (except in tests) so this
        # should hold for now. Can use a long timeout for production and a
        # short one for tests.

        inner_timeout = self._dashi.consumer_timeout
        elapsed = 0

        # keep trying until a single event is drained or timeout hit
        while not self._cancelled:
            try:
                self._conn.drain_events(timeout=inner_timeout)
                break

            except socket.timeout:
                if timeout:
                    elapsed += inner_timeout
                    if elapsed >= timeout:
                        raise

                    if elapsed + inner_timeout > timeout:
                        inner_timeout = timeout - elapsed


    def cancel(self, block=True):
        self._cancelled = True
        if block:
            # acquire the lock and release it immediately
            with self._consumer_lock:
                pass

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
                raise UnknownOperationError("Unknown operation: " + op)

            try:
                ret = op_fun(**args)
            except TypeError, e:
                log.exception("Type error with handler for %s:%s", self._name, op)
                raise BadRequestError("Type error: %s" % str(e))
            except Exception:
                log.exception("Error in handler for %s:%s", self._name, op)
                raise

        except Exception:
            err = sys.exc_info()
        finally:
            if reply_to:
                if err:
                    tb = "".join(traceback.format_exception(*err))

                    # some error types are specific to dashi (not underlying
                    # service code). These get raised with the same type on
                    # the client side. Identify them by prefixing the package
                    # name on the exc_type.

                    exc_type = err[0]
                    known_type = ERROR_TYPE_MAP.get(exc_type.__name__)
                    if known_type and exc_type is known_type:
                        exc_type_name = ERROR_PREFIX + exc_type.__name__
                    else:
                        exc_type_name = exc_type.__name__

                    err = dict(exc_type=exc_type_name, value=str(err[1]),
                               traceback=tb)

                reply = dict(result=ret, error=err)
                self._dashi.reply(reply_to, reply)

            message.ack()

    def add_op(self, name, fun):
        if not callable(fun):
            raise ValueError("operation function must be callable")
        
        self._ops[name] = fun


def raise_error(error):
    """Intakes a dict of remote error information and raises a DashiError
    """
    exc_type = error.get('exc_type')
    if exc_type and exc_type.startswith(ERROR_PREFIX):
        exc_type = exc_type[len(ERROR_PREFIX):]
        exc_cls = ERROR_TYPE_MAP.get(exc_type, DashiError)
    else:
        exc_cls = DashiError

    raise exc_cls(**error)


class DashiError(Exception):
    def __init__(self, message=None, exc_type=None, value=None, traceback=None, **kwargs):
        self.exc_type = exc_type
        self.value = value
        self.traceback = traceback

        if message is None:
            if exc_type:
                if value:
                    message = "%s: %s" % (exc_type, value)
                else:
                    message = exc_type
            elif value:
                message = value
            else:
                message = ""
            if traceback:
                message += "\n" + str(traceback)
        super(DashiError, self).__init__(message)


class BadRequestError(DashiError):
    pass


class UnknownOperationError(DashiError):
    pass

ERROR_PREFIX = "dashi."
ERROR_TYPES = (BadRequestError, UnknownOperationError)
ERROR_TYPE_MAP = dict((cls.__name__, cls) for cls in ERROR_TYPES)