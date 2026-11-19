"""Use pika with the Asyncio EventLoop."""

from __future__ import annotations

import asyncio
import collections
import contextlib
import functools
import inspect
import logging
import sys
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Sequence, cast

from pika import channel as pika_channel
from pika import exceptions, frame, spec
from pika._utils import as_bytes, override
from pika.adapters import base_connection
from pika.adapters.utils import connection_workflow, io_services_utils, nbio_interface

if TYPE_CHECKING:
    from pika import connection
    from pika.channel import Channel

LOGGER = logging.getLogger(__name__)

# `AsyncioConnection.publish` frames a larger body in the loop's default
# executor.
_EXECUTOR_BODY_SIZE = 64 * 1024

# `AsyncioConnection.publish` holds messages while the transport buffers this
# many bytes or more.
_WRITE_BUFFER_LIMIT = 1024 * 1024


class AsyncioConnection(base_connection.BaseConnection):
    """The AsyncioConnection runs on the Asyncio EventLoop."""

    def __init__(
            self,
            parameters: connection.Parameters | None = None,
            on_open_callback: Callable[[AsyncioConnection], None] | None = None,
            on_open_error_callback: Callable[[AsyncioConnection, BaseException],
                                             None] | None = None,
            on_close_callback: Callable[[AsyncioConnection, BaseException],
                                        None] | None = None,
            custom_ioloop: asyncio.AbstractEventLoop |
        nbio_interface.AbstractIOServices | None = None,
            internal_connection_workflow: bool = True) -> None:
        """
        Create a new instance of the AsyncioConnection class, connecting to RabbitMQ automatically.

        :param parameters: Connection parameters
        :param on_open_callback: Called with this AsyncioConnection when it is open.
        :param on_open_error_callback: Callback (or None) with signature
            ``(AsyncioConnection, BaseException) -> None``; called if the connection
            can't be established or connection establishment is interrupted by
            `Connection.close()`: on_open_error_callback(AsyncioConnection, exception).
        :param on_close_callback: Callback (or None) with signature
            ``(AsyncioConnection, BaseException) -> None``; called when a previously fully
            open connection is closed:
            `on_close_callback(AsyncioConnection, exception)`, where `exception` is
            either an instance of `exceptions.ConnectionClosed` if closed by
            user or broker or exception of another type that describes the cause
            of connection failure.
        :param custom_ioloop: Optional custom loop object
            (``asyncio.AbstractEventLoop`` or
            ``nbio_interface.AbstractIOServices``). Defaults to the running event loop, or a new event
            loop when none is running.
        :param internal_connection_workflow: True for autonomous connection
            establishment which is default; False for externally-managed
            connection workflow via the `create_connection()` factory.
        """
        if isinstance(custom_ioloop, nbio_interface.AbstractIOServices):
            nbio = custom_ioloop
        else:
            nbio = _AsyncioIOServicesAdapter(custom_ioloop)

        self._publisher: _Publisher | None = None
        self._consumer_tasks: set[asyncio.Future[Any]] = set()
        self._stream_termination_task: asyncio.Task[None] | None = None

        super().__init__(
            parameters,
            cast(Any, on_open_callback),
            cast(Any, on_open_error_callback),
            cast(Any, on_close_callback),
            nbio,
            internal_connection_workflow=internal_connection_workflow)

    @classmethod
    @override
    def create_connection(
        cls,
        connection_configs: Sequence[connection.Parameters],
        on_done: Callable[[(connection.Connection |
                            connection_workflow.AMQPConnectorException)], None],
        custom_ioloop: asyncio.AbstractEventLoop | None = None,
        workflow: connection_workflow.AbstractAMQPConnectionWorkflow |
        None = None
    ) -> connection_workflow.AbstractAMQPConnectionWorkflow:
        """
        :param connection_configs: One or more connection parameter objects
        :param on_done: Callback to report when connection workflow is done
        :param custom_ioloop: Optional custom event loop to use for the connection workflow
        :param workflow: Optional connection workflow instance to use; if None,
            a default workflow will be created
        """
        nbio = _AsyncioIOServicesAdapter(custom_ioloop)

        def connection_factory(params) -> AsyncioConnection:
            """
            Connection factory.

            :param params: Connection parameters
            """
            if params is None:
                raise ValueError('Expected pika.connection.Parameters '
                                 'instance, but got None in params arg.')
            return cls(parameters=params,
                       custom_ioloop=nbio,
                       internal_connection_workflow=False)

        return cls._start_connection_workflow(
            connection_configs=connection_configs,
            connection_factory=connection_factory,
            nbio=nbio,
            workflow=workflow,
            on_done=on_done)

    def publish(self,
                channel: Channel,
                exchange: str,
                routing_key: str,
                body: str | bytes,
                properties: spec.BasicProperties | None = None,
                mandatory: bool = False) -> asyncio.Future[None]:
        """
        Publish to a channel of this connection, framing a large body off the event loop.

        Call it on the event loop's thread, like the rest of this connection's API. The method and
        properties are encoded before it returns, so the caller may reuse *properties* at once. The
        body is framed later: in the loop's default executor when it is larger than 64 KiB, on the
        loop otherwise.

        Messages published through this method are written to the transport in call order, each as
        one unbroken run of frames, so the messages on a channel are never interleaved. A
        `Channel.basic_publish` is written at once, so it can overtake messages this method still
        holds.

        While the transport buffers 1 MiB or more of unsent data, messages wait unframed, so this
        method's own writes take the buffer past 1 MiB by at most one message. Other writes, such as
        `Channel.basic_publish`, are not held back. A waiting message keeps its body, so a caller
        bounds the memory held by awaiting its publishes.

        The returned future resolves to None once the message's frames are in the transport's write
        buffer, whether or not publisher confirms are enabled; the broker's Basic.Ack or Basic.Nack
        still arrives through `Channel.confirm_delivery`. Cancelling the future before then
        withdraws the message.

        Failures are set on the future rather than raised. A message fails with
        `exceptions.ChannelWrongStateError` if the channel is not open when it is published or when
        its turn to be written comes, and with the channel's closing reason if it is still waiting
        when the channel closes. A close the caller starts therefore fails the messages not yet
        written, so await the futures before calling `Channel.close` or `close`. *body* other than
        `bytes` or `str` fails with `TypeError`, and the method and properties with whatever
        encoding them raises, such as `pika.exceptions.UnsupportedAMQPFieldException` for a header
        value with no field-table type.

        :param channel: An open channel of this connection
        :param exchange: The exchange to publish to
        :param routing_key: The routing key to bind on
        :param body: The message body
        :param properties: Basic.properties
        :param mandatory: The mandatory flag
        :returns: A future that resolves once the message is queued for sending
        """
        if self._publisher is None:
            self._publisher = _Publisher(self)
        return self._publisher.publish(channel, exchange, routing_key, body,
                                       properties, mandatory)

    @override
    def channel(
        self,
        channel_number: int | None = None,
        on_open_callback: Callable[[_AsyncioChannel], Any] | None = None
    ) -> _AsyncioChannel:
        """Create a channel with AsyncioConnection's coroutine consumer support."""
        return cast(
            _AsyncioChannel,
            super().channel(channel_number, cast(Any, on_open_callback)))

    @override
    def _notify_connection_closed(self, reason: Exception) -> None:
        if (isinstance(reason, exceptions.ConnectionClosedByClient) and
                self._consumer_tasks):
            if self._stream_termination_task is None:
                self._stream_termination_task = self.ioloop.create_task(
                    self._notify_after_consumer_tasks(reason))
            return

        if self._stream_termination_task is not None:
            self._stream_termination_task.cancel()
            self._stream_termination_task = None
        super()._notify_connection_closed(reason)

    async def _notify_after_consumer_tasks(self, reason: Exception) -> None:
        tasks = tuple(self._consumer_tasks)
        if tasks:
            await asyncio.wait(tasks)

        self._stream_termination_task = None
        super()._notify_connection_closed(reason)

    @override
    def _create_channel(
            self, channel_number: int,
            on_open_callback: Callable[[Channel], Any] | None
    ) -> _AsyncioChannel:
        """Create a channel that can schedule coroutine delivery handlers."""
        return _AsyncioChannel(self, channel_number, on_open_callback)


class _PendingPublish:
    """A message `AsyncioConnection.publish` has encoded but not yet written."""

    __slots__ = ('body', 'channel', 'future', 'head')

    def __init__(self, channel: Channel, head: list[bytes], body: bytes,
                 future: asyncio.Future[None]) -> None:
        """
        :param channel: The channel to publish on
        :param head: The marshaled method and header frames
        :param body: The message body
        :param future: The future `AsyncioConnection.publish` returned
        """
        self.channel = channel
        self.head = head
        self.body = body
        self.future = future


class _Publisher:
    """Writes the messages `AsyncioConnection.publish` accepts, one at a time and in call order."""

    def __init__(self, conn: AsyncioConnection) -> None:
        """
        :param conn: The connection to publish on
        """
        self._connection = conn
        self._loop: asyncio.AbstractEventLoop = conn.ioloop
        self._pending: collections.deque[_PendingPublish] = collections.deque()
        self._watched_channels: set[Channel] = set()
        self._capacity: asyncio.Future[None] | None = None
        self._writer: asyncio.Task[None] | None = None

    def publish(self, channel: Channel, exchange: str, routing_key: str,
                body: str | bytes, properties: spec.BasicProperties | None,
                mandatory: bool) -> asyncio.Future[None]:
        """
        See `AsyncioConnection.publish`.

        :param channel: An open channel of this connection
        :param exchange: The exchange to publish to
        :param routing_key: The routing key to bind on
        :param body: The message body
        :param properties: Basic.properties
        :param mandatory: The mandatory flag
        :returns: A future that resolves once the message is queued for sending
        """
        future: asyncio.Future[None] = self._loop.create_future()
        try:
            channel._raise_if_not_open()
            body = as_bytes(body, 'publish body')
            head = self._connection._marshal_message_head(
                channel.channel_number,
                spec.Basic.Publish(exchange=exchange,
                                   routing_key=routing_key,
                                   mandatory=mandatory), properties or
                spec.BasicProperties(), len(body))
        except Exception as error:
            future.set_exception(error)
            return future

        if channel not in self._watched_channels:
            self._watched_channels.add(channel)
            channel.add_on_close_callback(self._on_channel_closed)

        item = _PendingPublish(channel, head, body, future)
        self._pending.append(item)
        future.add_done_callback(functools.partial(self._withdraw, item))
        if self._writer is None:
            self._writer = self._loop.create_task(self._write_pending())
        return future

    async def _write_pending(self) -> None:
        """Write the pending messages in order, holding each until the transport has capacity."""
        try:
            while self._pending:
                transport = self._connection._transport
                assert isinstance(transport,
                                  io_services_utils._AsyncTransportBase)
                if transport.get_write_buffer_size() >= _WRITE_BUFFER_LIMIT:
                    transport._on_write_progress = self._wake
                    self._capacity = self._loop.create_future()
                    await self._capacity
                    continue

                # The message stays queued while it is framed, so a channel
                # close or a cancellation in the meantime settles it.
                item = self._pending[0]
                if not item.future.done():
                    await self._write(item)
                self._discard(item)
        finally:
            self._writer = None

    async def _write(self, item: _PendingPublish) -> None:
        """
        Frame a message's body and write the message, settling its future.

        :param item: The message
        """
        try:
            if len(item.body) > _EXECUTOR_BODY_SIZE:
                body_frames = await self._loop.run_in_executor(
                    None, self._connection._marshal_body,
                    item.channel.channel_number, item.body)
            else:
                body_frames = self._connection._marshal_body(
                    item.channel.channel_number, item.body)
            if item.future.done():
                return
            item.channel._publish_marshaled(item.head + body_frames)
        except Exception as error:
            if not item.future.done():
                item.future.set_exception(error)
        else:
            item.future.set_result(None)

    def _wake(self) -> None:
        """Let `_write_pending` check the transport and the queue again."""
        if self._capacity is not None and not self._capacity.done():
            self._capacity.set_result(None)

    def _withdraw(self, item: _PendingPublish,
                  future: asyncio.Future[None]) -> None:
        """
        Drop a message whose future was cancelled before it was written.

        :param item: The message
        :param future: Its future, now done
        """
        if future.cancelled():
            self._discard(item)

    def _discard(self, item: _PendingPublish) -> None:
        """
        Remove a message from the queue if it is still there.

        :param item: The message
        """
        with contextlib.suppress(ValueError):
            self._pending.remove(item)

    def _on_channel_closed(self, channel: Channel, reason: Exception) -> None:
        """
        Fail the messages pending on a channel that closed.

        :param channel: The closed channel
        :param reason: Why it closed
        """
        self._watched_channels.discard(channel)
        remaining: collections.deque[_PendingPublish] = collections.deque()
        for item in self._pending:
            if item.channel is not channel:
                remaining.append(item)
            elif not item.future.done():
                item.future.set_exception(reason)
        self._pending = remaining
        self._wake()


class _AsyncioChannel(pika_channel.Channel):
    """Channel that runs awaitable consumer callbacks as loop tasks."""

    def __init__(self, connection: AsyncioConnection, channel_number: int,
                 on_open_callback: Callable[[Channel], Any] | None) -> None:
        super().__init__(connection, channel_number, on_open_callback)
        self._asyncio_connection = connection
        self._consumer_tasks: set[asyncio.Future[Any]] = set()

    @override
    def basic_consume(
            self,
            queue: str,
            on_message_callback: Callable[
                [Channel, spec.Basic.Deliver, spec.BasicProperties, bytes],
                Any],
            auto_ack: bool = False,
            exclusive: bool = False,
            consumer_tag: str | None = None,
            arguments: dict[str, Any] | None = None,
            callback: Callable[..., Any] | None = None) -> str:
        return super().basic_consume(queue, cast(Any, on_message_callback),
                                     auto_ack, exclusive, consumer_tag,
                                     arguments, callback)

    @override
    def _on_deliver(self, method_frame: frame.Method,
                    header_frame: frame.Header, body: bytes) -> None:
        consumer_tag = method_frame.method.consumer_tag
        callback = self._consumers.get(consumer_tag)
        if callback is None or not self.is_open:
            super()._on_deliver(method_frame, header_frame, body)
            return

        if not self._is_coroutine_callback(callback):
            callback(self, method_frame.method, header_frame.properties, body)
            return

        result = callback(self, method_frame.method, header_frame.properties,
                          body)
        if inspect.isawaitable(result):
            task = asyncio.ensure_future(result,
                                         loop=self._asyncio_connection.ioloop)
            self._consumer_tasks.add(task)
            self._asyncio_connection._consumer_tasks.add(task)
            task.add_done_callback(self._on_consumer_task_done)

    @staticmethod
    def _is_coroutine_callback(callback: Callable[..., Any]) -> bool:
        while isinstance(callback, functools.partial):
            callback = callback.func
        return (inspect.iscoroutinefunction(callback) or
                inspect.iscoroutinefunction(type(callback).__call__))

    def _on_consumer_task_done(self, task: asyncio.Future[Any]) -> None:
        self._consumer_tasks.discard(task)
        self._asyncio_connection._consumer_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            LOGGER.error('Coroutine consumer handler failed on %s',
                         self,
                         exc_info=(type(error), error, error.__traceback__))

    @override
    def _cleanup(self) -> None:
        for task in self._consumer_tasks:
            task.cancel()
        super()._cleanup()


class _AsyncioIOServicesAdapter(io_services_utils.SocketConnectionMixin,
                                io_services_utils.StreamingConnectionMixin,
                                nbio_interface.AbstractFileDescriptorIOServices
                               ):
    """Implements
    `AbstractIOServices` interface
    on top of `asyncio`.

    NOTE:
    `AbstractFileDescriptorServices`
    interface is only required by the mixins.

    """

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """
        :param loop: If None, uses the
            running event loop via asyncio.get_running_loop(), or creates
            a new one via asyncio.new_event_loop() when no loop is running
            (e.g. when called from a non-async thread).

        """
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                if sys.platform == 'win32':
                    loop = asyncio.SelectorEventLoop()
                else:
                    loop = asyncio.new_event_loop()
        if sys.platform == 'win32' and not isinstance(
                loop, asyncio.SelectorEventLoop):
            raise TypeError(
                'On Windows, Pika requires an asyncio.SelectorEventLoop '
                'because it uses add_reader/add_writer. The running loop '
                f'is {type(loop).__name__}. Either pass a SelectorEventLoop '
                'via custom_ioloop, or run with '
                'asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop).')
        self._loop = loop

    @override
    def get_native_ioloop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @override
    def close(self) -> None:
        self._loop.close()

    @override
    def run(self) -> None:
        self._loop.run_forever()

    @override
    def stop(self) -> None:
        self._loop.stop()

    @override
    def add_callback_threadsafe(self, callback: Callable[[], None]) -> None:
        """
        :param callback: The callback to call from the thread
        """
        self._loop.call_soon_threadsafe(callback)

    @override
    def call_later(self, delay: float,
                   callback: Callable[[], None]) -> _TimerHandle:
        """
        :param delay: Delay in seconds
        :param callback: The callback to call after the delay
        """
        return _TimerHandle(self._loop.call_later(delay, callback))

    @override
    def getaddrinfo(self,
                    host: str,
                    port: int,
                    on_done: Callable[..., None],
                    family: int = 0,
                    socktype: int = 0,
                    proto: int = 0,
                    flags: int = 0) -> nbio_interface.AbstractIOReference:
        """
        :param host: Hostname or IP address
        :param port: TCP port number
        :param on_done: The callback to call with the result of getaddrinfo
        :param family: Socket address family (e.g. ``socket.AF_INET``)
        :param socktype: Socket type (e.g. ``socket.SOCK_STREAM``)
        :param proto: Protocol number (0 for default)
        :param flags: `socket.getaddrinfo` flags
        """
        return self._schedule_and_wrap_in_io_ref(
            self._loop.getaddrinfo(host,
                                   port,
                                   family=family,
                                   type=socktype,
                                   proto=proto,
                                   flags=flags), on_done)

    @override
    def set_reader(self, fd: int, on_readable: Callable[[], None]) -> None:
        """
        :param fd: File descriptor
        :param on_readable: The callback to call when the file descriptor is readable
        """
        self._loop.add_reader(fd, on_readable)
        LOGGER.debug('set_reader(%s, _)', fd)

    @override
    def remove_reader(self, fd: int) -> bool:
        """
        :param fd: File descriptor
        """
        LOGGER.debug('remove_reader(%s)', fd)
        return self._loop.remove_reader(fd)

    @override
    def set_writer(self, fd: int, on_writable: Callable[[], None]) -> None:
        """
        :param fd: File descriptor
        :param on_writable: The callback to call when the file descriptor is writable
        """
        self._loop.add_writer(fd, on_writable)
        LOGGER.debug('set_writer(%s, _)', fd)

    @override
    def remove_writer(self, fd: int) -> bool:
        """
        :param fd: File descriptor
        """
        LOGGER.debug('remove_writer(%s)', fd)
        return self._loop.remove_writer(fd)

    def _schedule_and_wrap_in_io_ref(
        self, coro: Awaitable[Any],
        on_done: Callable[[base_connection.BaseConnection | BaseException],
                          None]
    ) -> _AsyncioIOReference:
        """
        Schedule the coroutine to run and return _AsyncioIOReference.

        :param coro: Coroutine to schedule.
        :param on_done: User callback that takes the completion result or exception as its only arg.
            It will not be called if the operation was cancelled.
        """
        if not callable(on_done):
            raise TypeError(
                f'on_done arg must be callable, but got {on_done!r}')

        return _AsyncioIOReference(asyncio.ensure_future(coro, loop=self._loop),
                                   on_done)


class _TimerHandle(nbio_interface.AbstractTimerReference):
    """This module's adaptation of `nbio_interface.AbstractTimerReference`."""

    def __init__(self, handle: asyncio.Handle) -> None:
        """

        :param handle:
        """
        self._handle: asyncio.Handle | None = handle

    @override
    def cancel(self) -> None:
        """Cancel the timer handle."""
        if self._handle is not None:
            self._handle.cancel()
            self._handle = None


class _AsyncioIOReference(nbio_interface.AbstractIOReference):
    """This module's adaptation of `nbio_interface.AbstractIOReference`."""

    def __init__(
        self, future: asyncio.Future,
        on_done: Callable[[base_connection.BaseConnection | BaseException],
                          None]
    ) -> None:
        """
        :param future:
        :param on_done: user callback that takes the completion result
            or exception as its only arg. It will not be called if the operation
            was cancelled.

        """
        if not callable(on_done):
            raise TypeError(
                f'on_done arg must be callable, but got {on_done!r}')

        self._future = future

        def on_done_adapter(future: asyncio.Future) -> None:
            """Handle completion callback from the future instance
            :param future: The future that completed
            """
            # NOTE: Asyncio schedules callback for cancelled futures, but pika
            # doesn't want that
            if not future.cancelled():
                on_done(future.exception() or future.result())

        future.add_done_callback(on_done_adapter)

    @override
    def cancel(self) -> bool:
        """
        Cancel pending operation.

        :returns: False if was already done or cancelled; True otherwise
        """
        return self._future.cancel()
