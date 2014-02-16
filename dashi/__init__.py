from collections import namedtuple
import socket
import threading
import traceback
import uuid
import sys
import time
import logging
import itertools
from datetime import datetime, timedelta

from kombu.connection import Connection
from kombu.messaging import Consumer, Producer
from kombu.pools import connections
from kombu.entity import Queue, Exchange

from .exceptions import DashiError, BadRequestError, NotFoundError, \
    UnknownOperationError, WriteConflictError
from .util import Countdown, RetryBackoff

__version__ = '0.3.0'

log = logging.getLogger(__name__)

DEFAULT_HEARTBEAT = 30
DEFAULT_QUEUE_EXPIRATION = 60.0


class Dashi(object):

    consumer_timeout = 1.0

    timeout_error = socket.timeout

    def __init__(self, name, uri, exchange, durable=False, auto_delete=False,
                 serializer=None, transport_options=None, ssl=False,
                 heartbeat=DEFAULT_HEARTBEAT, sysname=None, retry=None,
                 errback=None):
        """Set up a Dashi connection

        @param name: name of destination service queue used by consumers
        @param uri: broker URI (e.g. 'amqp://guest:guest@localhost:5672//')
        @param exchange: name of exchange to create and use
        @param durable: if True, destination service queue and exchange will be
        created as durable
        @param auto_delete: if True, destination service queue and exchange
        will be deleted when all consumers are gone
        @param serializer: specify a serializer for message encoding
        @param transport_options: custom parameter dict for the transport backend
        @param heartbeat: amqp heartbeat interval
        @param sysname: a prefix for exchanges and queues for namespacing
        @param retry: a RetryBackoff object, or None to use defaults
        @param errback: callback called within except block of connection failures
        """

        self._heartbeat_interval = heartbeat
        self._conn = Connection(uri, transport_options=transport_options,
                ssl=ssl, heartbeat=self._heartbeat_interval)
        if heartbeat:
            # create a connection template for pooled connections. These cannot
            # have heartbeat enabled.
            self._pool_conn = Connection(uri, transport_options=transport_options,
                    ssl=ssl)
        else:
            self._pool_conn = self._conn

        self._name = name
        self._sysname = sysname
        if self._sysname is not None:
            self._exchange_name = "%s.%s" % (self._sysname, exchange)
        else:
            self._exchange_name = exchange
        self._exchange = Exchange(name=self._exchange_name, type='direct',
                                  durable=durable, auto_delete=auto_delete)

        # visible attributes
        self.durable = durable
        self.auto_delete = auto_delete

        self._consumer = None

        self._linked_exceptions = {}

        self._serializer = serializer

        if retry is None:
            self.retry = RetryBackoff()
        else:
            self.retry = retry

        self._errback = errback

    @property
    def sysname(self):
        return self._sysname

    @property
    def name(self):
        return self._name

    def fire(self, name, operation, args=None, **kwargs):
        """Send a message without waiting for a reply

        @param name: name of destination service queue
        @param operation: name of service operation to invoke
        @param args: dictionary of keyword args to pass to operation.
                     Use this OR kwargs.
        @param kwargs: additional args to pass to operation
        """

        if args:
            if kwargs:
                raise TypeError("specify args dict or keyword arguments, not both")
        else:
            args = kwargs

        d = dict(op=operation, args=args)
        headers = {'sender': self.add_sysname(self.name)}

        dest = self.add_sysname(name)

        def _fire(channel):
            with Producer(channel) as producer:
                producer.publish(d, routing_key=dest,
                    headers=headers, serializer=self._serializer,
                    exchange=self._exchange, declare=[self._exchange])

        log.debug("sending message to %s", dest)
        with connections[self._pool_conn].acquire(block=True) as conn:
            _, channel = self.ensure(conn, _fire)
            conn.maybe_close_channel(channel)

    def call(self, name, operation, timeout=10, args=None, **kwargs):
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

        # create a direct queue for the reply. This may end up being a
        # bottleneck for performance: each rpc call gets a brand new
        # exclusive queue. However this approach is used nova.rpc and
        # seems to have carried them pretty far. If/when this
        # becomes a bottleneck we can set up a long-lived backend queue and
        # use correlation_id to deal with concurrent RPC calls. See:
        #   http://www.rabbitmq.com/tutorials/tutorial-six-python.html
        msg_id = uuid.uuid4().hex

        # expire the reply queue shortly after the timeout. it will be
        # (lazily) deleted by the broker if we don't clean it up first
        queue_arguments = {'x-expires': int((timeout + 1) * 1000)}
        queue = Queue(name=msg_id, exchange=self._exchange, routing_key=msg_id,
                      durable=False, queue_arguments=queue_arguments)

        messages = []
        event = threading.Event()

        def _callback(body, message):
            messages.append(body)
            message.ack()
            event.set()

        d = dict(op=operation, args=args)
        headers = {'reply-to': msg_id, 'sender': self.add_sysname(self.name)}
        dest = self.add_sysname(name)

        def _declare_and_send(channel):
            consumer = Consumer(channel, (queue,), callbacks=(_callback,))
            with Producer(channel) as producer:
                producer.publish(d, routing_key=dest, headers=headers,
                    exchange=self._exchange, serializer=self._serializer)
            return consumer

        log.debug("sending call to %s:%s", dest, operation)
        with connections[self._pool_conn].acquire(block=True) as conn:
            consumer, channel = self.ensure(conn, _declare_and_send)
            try:
                self._consume(conn, consumer, timeout=timeout, until_event=event)

                # try to delete queue, but don't worry if it fails (will expire)
                try:
                    queue = queue.bind(channel)
                    queue.delete(nowait=True)
                except Exception:
                    log.exception("error deleting queue")

            finally:
                conn.maybe_close_channel(channel)

        msg_body = messages[0]
        if msg_body.get('error'):
            raise_error(msg_body['error'])
        else:
            return msg_body.get('result')

    def _consume(self, connection, consumer, count=None, timeout=None, until_event=None):
        if count is not None:
            if count <= 0:
                raise ValueError("count must be >= 1")
            consumed = itertools.count(1)

        inner_timeout = self.consumer_timeout
        if timeout is not None:
            timeout = Countdown.from_value(timeout)
            inner_timeout = min(timeout.timeleft, inner_timeout)

        if until_event and until_event.is_set():
            return

        needs_heartbeat = connection.heartbeat and connection.supports_heartbeats
        if needs_heartbeat:
            time_between_tics = timedelta(seconds=connection.heartbeat / 2.0)
            if self.consumer_timeout > time_between_tics.seconds:
                msg = "dashi consumer timeout (%s) must be half or smaller than the heartbeat interval %s" % (
                        self.consumer_timeout, connection.heartbeat)
                raise DashiError(msg)
            last_heartbeat_check = datetime.min

        reconnect = False
        declare = True
        while 1:
            try:
                if declare:
                    consumer.consume()
                    declare = False

                if needs_heartbeat:
                    if datetime.now() - last_heartbeat_check > time_between_tics:
                        last_heartbeat_check = datetime.now()
                        connection.heartbeat_check()

                connection.drain_events(timeout=inner_timeout)
                if count and next(consumed) == count:
                    return

            except socket.timeout:
                pass
            except (connection.connection_errors, IOError):
                log.debug("Received error consuming", exc_info=True)
                self._call_errback()
                reconnect = True

            if until_event is not None and until_event.is_set():
                return

            if timeout:
                inner_timeout = min(inner_timeout, timeout.timeleft)
                if not inner_timeout:
                    raise self.timeout_error()

            if reconnect:
                self.connect(connection, (consumer,), timeout=timeout)
                reconnect = False
                declare = True

    def reply(self, connection, msg_id, body):
        def _reply(channel):
            with Producer(channel) as producer:
                producer.publish(body, routing_key=msg_id, exchange=self._exchange,
                    serializer=self._serializer)

        log.debug("replying to %s", msg_id)
        _, channel = self.ensure(connection, _reply)
        connection.maybe_close_channel(channel)

    def handle(self, operation, operation_name=None, sender_kwarg=None):
        """Handle an operation using the specified function

        @param operation: function to call for this operation
        @param operation_name: operation name. if unspecified operation.__name__ is used
        @param sender_kwarg: optional keyword arg on operation to feed in sender name
        """
        if not self._consumer:
            self._consumer = DashiConsumer(self, self._conn,
                    self._name, self._exchange, sysname=self._sysname)
        self._consumer.add_op(operation_name or operation.__name__, operation,
                              sender_kwarg=sender_kwarg)

    def consume(self, count=None, timeout=None):
        """Consume operations from the queue

        @param count: number of messages to consume before returning
        @param timeout: time in seconds to wait without receiving a message
        """
        self._consumer.consume(count, timeout)

    def cancel(self, block=True):
        """Cancel a call to consume() happening in another thread

        This could take up to DashiConnection.consumer_timeout to complete.

        @param block: if True, waits until the consumer has returned
        """
        if self._consumer:
            self._consumer.cancel(block=block)

    def disconnect(self):
        """Disconnects a consumer binding if exists
        """
        if self._consumer:
            self._consumer.disconnect()

    def link_exceptions(self, custom_exception=None, dashi_exception=None):
        """Link a custom exception thrown on the receiver to a dashi exception
        """
        if custom_exception is None:
            raise ValueError("custom_exception must be set")
        if dashi_exception is None:
            raise ValueError("dashi_exception must be set")

        self._linked_exceptions[custom_exception] = dashi_exception

    def _call_errback(self):
        if not self._errback:
            return
        try:
            self._errback()
        except Exception:
            log.exception("error calling errback..")

    def add_sysname(self, name):
        if self.sysname is not None:
            return "%s.%s" % (self.sysname, name)
        else:
            return name

    def connect(self, connection, entities=None, timeout=None):
        if timeout is not None:
            timeout = Countdown.from_value(timeout)
        backoff = iter(self.retry)
        while 1:

            this_backoff = next(backoff, False)

            try:
                channel = self._connect(connection)
                if entities:
                    for entity in entities:
                        entity.revive(channel)
                return channel

            except (connection.connection_errors, IOError):
                if this_backoff is False:
                    log.exception("Error connecting to broker. Giving up.")
                    raise
                self._call_errback()

            if timeout:
                timeleft = timeout.timeleft
                if not timeleft:
                    raise self.timeout_error()
                elif timeleft < this_backoff:
                    this_backoff = timeleft

            log.exception("Error connecting to broker. Retrying in %ss", this_backoff)
            time.sleep(this_backoff)

    def _connect(self, connection):
        # close out previous connection first
        try:
            #dirty: breaking into kombu to force close the connection
            connection._close()
        except connection.connection_errors:
            pass

        connection.connect()
        return connection.channel()

    def ensure(self, connection, func, *args, **kwargs):
        """Perform an operation until success

        Repeats in the face of connection errors, pursuant to retry policy.
        """
        channel = None
        while 1:
            try:
                if channel is None:
                    channel = connection.channel()
                return func(channel, *args, **kwargs), channel
            except (connection.connection_errors, IOError):
                self._call_errback()

            channel = self.connect(connection)

# alias for compatibility
DashiConnection = Dashi

_OpSpec = namedtuple('_OpSpec', ['function', 'sender_kwarg'])


class DashiConsumer(object):
    def __init__(self, dashi, connection, name, exchange, sysname=None):
        self._dashi = dashi
        self._conn = connection
        self._name = name
        self._exchange = exchange
        self._sysname = sysname

        self._ops = {}
        self._cancelled = threading.Event()
        self._consumer_lock = threading.Lock()

        if self._sysname is not None:
            self._queue_name = "%s.%s" % (self._sysname, self._name)
        else:
            self._queue_name = self._name

        self._queue_kwargs = dict(
            name=self._queue_name,
            exchange=self._exchange,
            routing_key=self._queue_name,
            durable=self._dashi.durable,
            auto_delete=self._dashi.auto_delete,
            queue_arguments={'x-expires': int(DEFAULT_QUEUE_EXPIRATION * 1000)})

        self.connect()

    def connect(self):
        self._dashi.ensure(self._conn, self._connect)

    def _connect(self, channel):
        self._queue = Queue(channel=channel, **self._queue_kwargs)
        self._queue.declare()

        self._consumer = Consumer(channel, [self._queue],
            callbacks=[self._callback])
        self._consumer.consume()

    def disconnect(self):
        self._consumer.cancel()
        self._conn.release()

    def consume(self, count=None, timeout=None):

        # hold a lock for the duration of the consuming. this prevents
        # multiple consumers and allows cancel to detect when consuming
        # has ended.
        if not self._consumer_lock.acquire(False):
            raise Exception("only one consumer thread may run concurrently")

        try:
            self._dashi._consume(self._conn, self._consumer, count=count,
                                timeout=timeout, until_event=self._cancelled)
        finally:
            self._consumer_lock.release()
            self._cancelled.clear()

    def cancel(self, block=True):
        self._cancelled.set()
        if block:
            # acquire the lock and release it immediately
            with self._consumer_lock:
                pass

    def _callback(self, body, message):
        reply_to = None
        ret = None
        err = None
        err_dict = None
        try:
            reply_to = message.headers.get('reply-to')

            try:
                op = str(body['op'])
                args = body.get('args')
            except Exception, e:
                log.warn("Failed to interpret message body: %s", body,
                         exc_info=True)
                raise BadRequestError("Invalid request: %s" % str(e))

            op_spec = self._ops.get(op)
            if not op_spec:
                raise UnknownOperationError("Unknown operation: " + op)
            op_fun = op_spec.function

            # stick the sender into kwargs if handler requested it
            if op_spec.sender_kwarg:
                sender = message.headers.get('sender')
                args[op_spec.sender_kwarg] = sender

            try:
                ret = op_fun(**args)
            except TypeError, e:
                log.exception("Type error with handler for %s:%s", self._name, op)
                raise BadRequestError("Type error: %s" % str(e))
            except Exception:
                raise

        except Exception:
            err = sys.exc_info()
        finally:
            if err:
                err_dict, is_known_error = self._wrap_error(err)
                if is_known_error:
                    exc_type = err_dict['exc_type']
                    if exc_type and exc_type.startswith(ERROR_PREFIX):
                        exc_type = exc_type[len(ERROR_PREFIX):]
                    log.info("Known '%s' error in handler %s:%s: %s", exc_type,
                        self._name, op, err_dict['value'])
                else:
                    log.error("Unknown '%s' error in handler %s:%s: %s",
                        err_dict['exc_type'], self._name, op,
                        err_dict['value'], exc_info=err)

            if reply_to:
                reply = dict(result=ret, error=err_dict)
                self._dashi.reply(self._conn, reply_to, reply)

            message.ack()

    def _wrap_error(self, exc_info):
        tb = "".join(traceback.format_exception(*exc_info))

        # some error types are specific to dashi (not underlying
        # service code). These get raised with the same type on
        # the client side. Identify them by prefixing the package
        # name on the exc_type.

        exc_type = exc_info[0]

        # Check if there is a dashi exception linked to this custom exception
        linked_exception = self._dashi._linked_exceptions.get(exc_type)
        if linked_exception:
            exc_type = linked_exception

        known_type = ERROR_TYPE_MAP.get(exc_type.__name__)
        is_known = known_type and exc_type is known_type
        if is_known:
            exc_type_name = ERROR_PREFIX + exc_type.__name__
        else:
            exc_type_name = exc_type.__name__

        return dict(exc_type=exc_type_name, value=str(exc_info[1]),
                   traceback=tb), is_known

    def add_op(self, name, fun, sender_kwarg=None):
        if not callable(fun):
            raise ValueError("operation function must be callable")

        self._ops[name] = _OpSpec(fun, sender_kwarg)


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

ERROR_PREFIX = "dashi.exceptions."
ERROR_TYPES = (BadRequestError, NotFoundError, UnknownOperationError, WriteConflictError)
ERROR_TYPE_MAP = dict((cls.__name__, cls) for cls in ERROR_TYPES)
