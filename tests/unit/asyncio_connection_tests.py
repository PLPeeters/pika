"""Tests for pika.adapters.asyncio_connection."""

import asyncio
import functools
import socket
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from pika import callback, connection, exceptions, frame, spec
from pika.adapters.asyncio_connection import AsyncioConnection, _AsyncioIOServicesAdapter
from tests.base.asyncio_loop import new_pika_asyncio_loop


class AsyncioIOServicesAdapterLoopInitTests(unittest.TestCase):
    """Tests for _AsyncioIOServicesAdapter loop initialisation logic."""

    def test_explicit_loop_is_used_as_is(self):
        loop = new_pika_asyncio_loop()
        try:
            adapter = _AsyncioIOServicesAdapter(loop)
            self.assertIs(adapter.get_native_ioloop(), loop)
        finally:
            loop.close()

    def test_no_loop_uses_running_loop_when_available(self):
        results = []

        async def _run():
            adapter = _AsyncioIOServicesAdapter()
            results.append(adapter.get_native_ioloop())

        loop = new_pika_asyncio_loop()
        try:
            loop.run_until_complete(_run())
            self.assertIs(results[0], loop)
        finally:
            loop.close()

    def test_no_loop_creates_new_loop_on_main_thread_sync_context(self):
        # Simulates the Python 3.14 breakage: a plain synchronous call with no
        # running event loop (e.g. top-level script before any loop is started).
        adapter = _AsyncioIOServicesAdapter()
        loop = adapter.get_native_ioloop()
        try:
            self.assertIsInstance(loop, asyncio.AbstractEventLoop)
        finally:
            loop.close()

    def test_no_loop_creates_new_loop_when_none_running(self):
        results = []
        errors = []

        def _thread_target():
            try:
                adapter = _AsyncioIOServicesAdapter()
                results.append(adapter.get_native_ioloop())
                adapter.get_native_ioloop().close()
            except Exception as exc:
                errors.append(exc)

        t = threading.Thread(target=_thread_target)
        t.start()
        t.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], asyncio.AbstractEventLoop)

    @patch('pika.adapters.asyncio_connection.sys')
    def test_windows_proactor_loop_raises_type_error(self, mock_sys):
        mock_sys.platform = 'win32'
        mock_sys.version_info = (3, 12)
        if not hasattr(asyncio, 'ProactorEventLoop'):
            self.skipTest('ProactorEventLoop not available on this OS')
        loop = asyncio.ProactorEventLoop()
        try:
            with self.assertRaises(TypeError) as ctx:
                _AsyncioIOServicesAdapter(loop)
            self.assertIn('SelectorEventLoop', str(ctx.exception))
        finally:
            loop.close()

    @patch('pika.adapters.asyncio_connection.sys')
    def test_windows_selector_loop_accepted(self, mock_sys):
        mock_sys.platform = 'win32'
        mock_sys.version_info = (3, 12)
        loop = asyncio.SelectorEventLoop()
        try:
            adapter = _AsyncioIOServicesAdapter(loop)
            self.assertIs(adapter.get_native_ioloop(), loop)
        finally:
            loop.close()

    @patch('pika.adapters.asyncio_connection.sys')
    def test_windows_no_loop_creates_selector_event_loop(self, mock_sys):
        mock_sys.platform = 'win32'
        mock_sys.version_info = (3, 12)
        results = []
        errors = []

        def _thread_target():
            try:
                adapter = _AsyncioIOServicesAdapter()
                results.append(adapter.get_native_ioloop())
            except Exception as exc:
                errors.append(exc)

        t = threading.Thread(target=_thread_target)
        t.start()
        t.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], asyncio.SelectorEventLoop)
        results[0].close()


class AsyncioCoroutineConsumerTests(unittest.TestCase):

    def setUp(self):
        self.loop = new_pika_asyncio_loop()
        self.connection = SimpleNamespace(callbacks=callback.CallbackManager(),
                                          _nbio=_AsyncioIOServicesAdapter(
                                              self.loop),
                                          _consumer_tasks=set(),
                                          ioloop=self.loop)
        self.channel = AsyncioConnection._create_channel(
            self.connection, 1, None)
        self.channel._set_state(self.channel.OPEN)
        self.channel._rpc = lambda *args, **kwargs: None

    def tearDown(self):
        self.loop.run_until_complete(asyncio.sleep(0))
        self.loop.close()

    def _deliver(self, consumer_tag, delivery_tag):
        method = frame.Method(1, spec.Basic.Deliver(consumer_tag, delivery_tag))
        header = frame.Header(1, 0, spec.BasicProperties())
        self.channel._on_deliver(method, header, b'body')

    def _make_open_connection(self, on_close_callback=None):
        connection_obj = AsyncioConnection(connection.Parameters(),
                                           on_close_callback=on_close_callback,
                                           custom_ioloop=self.loop,
                                           internal_connection_workflow=False)
        connection_obj._opened = True
        connection_obj._set_connection_state(connection_obj.CONNECTION_OPEN)
        connection_obj._send_connection_close = (
            lambda _code, _text: connection_obj._on_connection_close_ok(
                frame.Method(0, spec.Connection.CloseOk())))
        connection_obj._adapter_disconnect_stream = (
            lambda: connection_obj._on_stream_terminated(None))
        return connection_obj

    def _add_channel(self, connection_obj):
        channel = connection_obj._create_channel(1, None)
        connection_obj._channels[1] = channel
        connection_obj._add_channel_callbacks(1)
        channel._set_state(channel.OPEN)

        def rpc(method, callback=None, _replies=None):
            if isinstance(method, spec.Channel.Close):
                self.loop.call_soon(callback,
                                    frame.Method(1, spec.Channel.CloseOk()))

        channel._rpc = rpc
        return channel

    def test_coroutine_consumer_can_ack_after_await_without_blocking_loop(self):

        async def scenario():
            started = asyncio.Event()
            ack_sent = asyncio.Event()
            acknowledgements = []
            timer_ran = asyncio.Event()
            io_callback_ran = asyncio.Event()
            awaited_reader, awaited_writer = socket.socketpair()
            ready_reader, ready_writer = socket.socketpair()
            awaited_reader.setblocking(False)

            async def on_message(channel, method, _properties, _body):
                started.set()
                await self.loop.sock_recv(awaited_reader, 1)
                channel.basic_ack(method.delivery_tag)
                ack_sent.set()

            def on_ready_io():
                ready_reader.recv(1)
                io_callback_ran.set()

            def send_method(method):
                acknowledgements.append(method)

            self.channel._send_method = send_method
            self.channel.basic_consume('queue', on_message, consumer_tag='c')
            self._deliver('c', 7)
            try:
                await asyncio.wait_for(started.wait(), 1)
                self.loop.add_reader(ready_reader, on_ready_io)
                self.loop.call_later(0, timer_ran.set)
                ready_writer.send(b'ready')
                await asyncio.wait_for(timer_ran.wait(), 1)
                await asyncio.wait_for(io_callback_ran.wait(), 1)
                self.assertEqual(acknowledgements, [])
                awaited_writer.send(b'x')
                await asyncio.wait_for(ack_sent.wait(), 1)
            finally:
                self.loop.remove_reader(ready_reader)
                awaited_reader.close()
                awaited_writer.close()
                ready_reader.close()
                ready_writer.close()

            self.assertEqual(len(acknowledgements), 1)
            self.assertIsInstance(acknowledgements[0], spec.Basic.Ack)
            self.assertEqual(acknowledgements[0].delivery_tag, 7)

        self.loop.run_until_complete(scenario())

    def test_synchronous_consumer_still_runs_inline(self):
        received = []
        self.channel.basic_consume('queue',
                                   lambda *_args: received.append('delivery'),
                                   consumer_tag='c')

        self._deliver('c', 1)

        self.assertEqual(received, ['delivery'])
        self.assertEqual(self.channel._consumer_tasks, set())

    def test_sync_consumer_returning_a_future_is_not_adopted(self):
        future = self.loop.create_future()
        received = []

        def on_message(*_args):
            received.append('delivery')
            return future

        self.channel.basic_consume('queue', on_message, consumer_tag='c')
        self._deliver('c', 1)

        self.assertEqual(received, ['delivery'])
        self.assertEqual(self.channel._consumer_tasks, set())
        self.assertFalse(future.cancelled())
        future.set_result(None)

    def test_partial_and_async_callable_consumers_are_scheduled(self):

        async def scenario():
            received = []
            finished = asyncio.Event()

            async def on_partial(_channel, method, _properties, _body, prefix):
                received.append(prefix + str(method.delivery_tag))

            class CallableConsumer:

                async def __call__(self, _channel, method, _properties, _body):
                    received.append(str(method.delivery_tag))
                    if len(received) == 2:
                        finished.set()

            self.channel.basic_consume('queue',
                                       functools.partial(on_partial,
                                                         prefix='partial'),
                                       consumer_tag='partial')
            self.channel.basic_consume('queue',
                                       CallableConsumer(),
                                       consumer_tag='callable')
            self._deliver('partial', 1)
            self._deliver('callable', 2)

            await asyncio.wait_for(finished.wait(), 1)
            self.assertEqual(received, ['partial1', '2'])

        self.loop.run_until_complete(scenario())

    def test_coroutine_function_wrapping_a_sync_function_is_scheduled(self):

        async def scenario():
            finished = asyncio.Event()

            def sync_template(_channel, _method, _properties, _body):
                return None

            @functools.wraps(sync_template)
            async def on_message(_channel, _method, _properties, _body):
                await asyncio.sleep(0)
                finished.set()

            self.channel.basic_consume('queue', on_message, consumer_tag='c')
            self._deliver('c', 1)
            await asyncio.wait_for(finished.wait(), 1)

        self.loop.run_until_complete(scenario())

    def test_sync_wrapper_returning_an_owned_task_is_not_adopted(self):

        async def scenario():
            started = asyncio.Event()
            release = asyncio.Event()
            finished = asyncio.Event()
            cancelled = asyncio.Event()
            owned_tasks = []

            async def async_template(_channel, _method, _properties, _body):
                return None

            async def worker():
                started.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise
                finished.set()

            @functools.wraps(async_template)
            def on_message(_channel, _method, _properties, _body):
                task = self.loop.create_task(worker())
                owned_tasks.append(task)
                return task

            self.channel.basic_consume('queue', on_message, consumer_tag='c')
            self._deliver('c', 1)
            await asyncio.wait_for(started.wait(), 1)
            self.channel._on_close_meta(RuntimeError('channel closed'))
            await asyncio.sleep(0)

            self.assertFalse(cancelled.is_set())
            release.set()
            await asyncio.wait_for(finished.wait(), 1)
            self.assertFalse(owned_tasks[0].cancelled())

        self.loop.run_until_complete(scenario())

    def test_burst_handlers_start_in_delivery_order_and_run_concurrently(self):

        async def scenario():
            started = []
            release = {tag: asyncio.Event() for tag in (1, 2, 3)}
            finished = []

            async def on_message(_channel, method, _properties, _body):
                started.append(method.delivery_tag)
                await release[method.delivery_tag].wait()
                finished.append(method.delivery_tag)

            self.channel.basic_consume('queue', on_message, consumer_tag='c')
            for tag in (1, 2, 3):
                self._deliver('c', tag)

            await asyncio.wait_for(self._until(lambda: len(started) == 3), 1)
            self.assertEqual(started, [1, 2, 3])
            for tag in (3, 2, 1):
                release[tag].set()
                await asyncio.wait_for(
                    self._until(
                        lambda expected=4 - tag: len(finished) == expected), 1)
            await asyncio.wait_for(self._until(lambda: len(finished) == 3), 1)
            self.assertEqual(finished, [3, 2, 1])

        self.loop.run_until_complete(scenario())

    def test_consumer_cancel_leaves_started_handler_running(self):

        async def scenario():
            started = asyncio.Event()
            release = asyncio.Event()
            completed = asyncio.Event()

            async def on_message(_channel, _method, _properties, _body):
                started.set()
                await release.wait()
                completed.set()

            cancel_replies = []

            def rpc(method, callback=None, _replies=None):
                if isinstance(method, spec.Basic.Cancel):
                    callback(
                        frame.Method(1, spec.Basic.CancelOk(consumer_tag='c')))

            self.channel._rpc = rpc
            self.channel.basic_consume('queue', on_message, consumer_tag='c')
            self._deliver('c', 1)
            await asyncio.wait_for(started.wait(), 1)
            self.channel.basic_cancel(
                'c', callback=lambda method: cancel_replies.append(method))
            self.assertEqual(len(cancel_replies), 1)
            release.set()
            await asyncio.wait_for(completed.wait(), 1)

        self.loop.run_until_complete(scenario())

    def test_channel_closeok_cancels_and_accounts_for_pending_handlers(self):

        async def scenario():
            started = asyncio.Event()
            cancelled = asyncio.Event()

            async def on_message(_channel, _method, _properties, _body):
                started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

            self.channel.basic_consume('queue', on_message, consumer_tag='c')
            self._deliver('c', 1)
            await asyncio.wait_for(started.wait(), 1)

            def rpc(method, callback=None, _replies=None):
                if isinstance(method, spec.Channel.Close):
                    self.loop.call_soon(callback,
                                        frame.Method(1, spec.Channel.CloseOk()))

            self.channel._rpc = rpc
            self.channel.close()
            await asyncio.wait_for(cancelled.wait(), 1)
            await asyncio.sleep(0)
            self.assertEqual(self.channel._consumer_tasks, set())

        self.loop.run_until_complete(scenario())

    def test_handler_failure_is_logged_and_task_is_accounted_for(self):

        async def scenario():
            acknowledgements = []

            async def on_message(_channel, _method, _properties, _body):
                raise RuntimeError('consumer failed')

            self.channel._send_method = acknowledgements.append
            self.channel.basic_consume('queue', on_message, consumer_tag='c')
            with self.assertLogs('pika.adapters.asyncio_connection',
                                 level='ERROR') as captured:
                self._deliver('c', 1)
                await asyncio.wait_for(
                    self._until(lambda: not self.channel._consumer_tasks), 1)

            self.assertIn('consumer failed', '\n'.join(captured.output))
            self.assertEqual(acknowledgements, [])

        self.loop.run_until_complete(scenario())

    def test_application_close_waits_for_handler_cleanup_before_callback(self):
        started = asyncio.Event()
        cancellation_suppressed = asyncio.Event()
        release = asyncio.Event()
        finalized = asyncio.Event()
        close_callbacks = []
        extra_close_callbacks = []

        async def on_message(_channel, _method, _properties, _body):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_suppressed.set()
                await release.wait()
                raise
            finally:
                finalized.set()

        def on_close(closed_connection, reason):
            close_callbacks.append((closed_connection, reason))
            self.loop.stop()

        connection_obj = self._make_open_connection(on_close)
        channel = self._add_channel(connection_obj)
        channel.basic_consume('queue', on_message, consumer_tag='c')
        method = frame.Method(1, spec.Basic.Deliver('c', 1))
        header = frame.Header(1, 0, spec.BasicProperties())
        channel._on_deliver(method, header, b'body')
        self.loop.run_until_complete(asyncio.wait_for(started.wait(), 1))

        self.loop.call_later(0.01, release.set)
        connection_obj.close()
        connection_obj.add_on_close_callback(
            lambda conn, reason: extra_close_callbacks.append((conn, reason)))
        self.loop.call_later(1, self.loop.stop)
        self.loop.run_forever()

        self.assertTrue(cancellation_suppressed.is_set())
        self.assertTrue(finalized.is_set())
        self.assertEqual(len(close_callbacks), 1)
        self.assertEqual(len(extra_close_callbacks), 1)
        self.assertEqual(connection_obj._consumer_tasks, set())

    def test_connection_loss_notifies_before_handler_finally_finishes(self):
        started = asyncio.Event()
        release = asyncio.Event()
        finalized = asyncio.Event()
        close_callbacks = []

        async def on_message(_channel, _method, _properties, _body):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()
                raise
            finally:
                finalized.set()

        connection_obj = self._make_open_connection(
            lambda conn, reason: close_callbacks.append((conn, reason)))
        channel = self._add_channel(connection_obj)
        channel.basic_consume('queue', on_message, consumer_tag='c')
        method = frame.Method(1, spec.Basic.Deliver('c', 1))
        header = frame.Header(1, 0, spec.BasicProperties())
        channel._on_deliver(method, header, b'body')
        self.loop.run_until_complete(asyncio.wait_for(started.wait(), 1))

        connection_obj._on_stream_terminated(RuntimeError('socket lost'))

        self.assertEqual(len(close_callbacks), 1)
        self.assertFalse(finalized.is_set())
        release.set()
        self.loop.run_until_complete(asyncio.wait_for(finalized.wait(), 1))

    def test_application_close_without_pending_handlers_notifies_inline(self):
        close_callbacks = []
        connection_obj = self._make_open_connection(
            lambda conn, reason: close_callbacks.append((conn, reason)))

        connection_obj.close()

        self.assertEqual(len(close_callbacks), 1)
        self.assertTrue(connection_obj.is_closed)

    def test_blocked_timeout_is_retired_before_delayed_close_notification(self):
        started = asyncio.Event()
        release = asyncio.Event()
        finalized = asyncio.Event()
        close_notified = asyncio.Event()
        close_reasons = []
        loop_errors = []
        connection_close_sent = asyncio.Event()
        transport_lost = asyncio.Event()
        sent_methods = []

        async def on_message(_channel, _method, _properties, _body):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()
                raise
            finally:
                finalized.set()

        def on_close(_connection, reason):
            close_reasons.append(reason)
            close_notified.set()

        connection_obj = AsyncioConnection(
            connection.ConnectionParameters(blocked_connection_timeout=0.3),
            on_close_callback=on_close,
            custom_ioloop=self.loop,
            internal_connection_workflow=False)
        connection_obj._opened = True
        connection_obj._set_connection_state(connection_obj.CONNECTION_OPEN)

        class Timer:

            def __init__(self, callback):
                self.callback = callback
                self.cancelled = False

            def cancel(self):
                self.cancelled = True

            def fire(self):
                if not self.cancelled:
                    self.callback()

        timers = []

        def call_later(_delay, callback):
            timer = Timer(callback)
            timers.append(timer)
            return timer

        connection_obj._adapter_call_later = call_later
        connection_obj._adapter_remove_timeout = lambda timer: timer.cancel()

        class Transport:

            def __init__(self):
                self.abort_calls = 0

            def abort(self):
                self.abort_calls += 1
                transport_lost.set()
                connection_obj._proto_connection_lost(None)

        transport = Transport()
        connection_obj._transport = transport

        def send_method(channel_number, method, _content=None):
            sent_methods.append((channel_number, method))
            if isinstance(method, spec.Basic.Consume):
                reply = frame.Method(
                    channel_number,
                    spec.Basic.ConsumeOk(consumer_tag=method.consumer_tag))
                self.loop.call_soon(connection_obj._process_frame, reply)
            elif isinstance(method, spec.Channel.Close):
                reply = frame.Method(channel_number, spec.Channel.CloseOk())
                self.loop.call_soon(connection_obj._process_frame, reply)
            elif isinstance(method, spec.Connection.Close):
                connection_close_sent.set()
                reply = frame.Method(0, spec.Connection.CloseOk())
                self.loop.call_soon(connection_obj._process_frame, reply)

        connection_obj._send_method = send_method
        disconnect = connection_obj._adapter_disconnect_stream
        disconnect_calls = []

        def count_disconnect():
            disconnect_calls.append(None)
            disconnect()

        connection_obj._adapter_disconnect_stream = count_disconnect
        channel = connection_obj._create_channel(1, None)
        connection_obj._channels[1] = channel
        connection_obj._add_channel_callbacks(1)
        channel._set_state(channel.OPEN)
        channel.basic_consume('queue', on_message, consumer_tag='c')
        method = frame.Method(1, spec.Basic.Deliver('c', 1))
        header = frame.Header(1, 0, spec.BasicProperties())
        channel._on_deliver(method, header, b'body')
        self.loop.run_until_complete(asyncio.wait_for(started.wait(), 1))

        connection_obj._process_frame(
            frame.Method(0, spec.Connection.Blocked('broker overloaded')))
        self.assertIsNotNone(connection_obj._blocked_conn_timer)
        self.assertEqual(len(timers), 1)

        self.loop.set_exception_handler(
            lambda _loop, context: loop_errors.append(context))
        self.assertTrue(connection_obj._opened)
        self.assertTrue(connection_obj.is_open)
        self.assertIn(1, connection_obj._channels)
        self.assertTrue(channel.is_open)
        connection_obj.close()
        self.assertEqual(channel._state, channel.CLOSING)
        self.assertTrue(
            any(
                isinstance(method, spec.Channel.Close)
                for _, method in sent_methods), sent_methods)
        self.loop.run_until_complete(
            asyncio.wait_for(connection_close_sent.wait(), 1))
        self.loop.run_until_complete(asyncio.wait_for(transport_lost.wait(), 1))
        self.assertTrue(connection_obj.is_closed)
        self.assertIsNone(connection_obj._blocked_conn_timer)
        self.assertTrue(timers[0].cancelled)
        timers[0].fire()
        self.assertEqual(close_reasons, [])
        self.assertFalse(finalized.is_set())
        release.set()
        self.loop.run_until_complete(asyncio.wait_for(finalized.wait(), 1))
        self.loop.run_until_complete(asyncio.wait_for(close_notified.wait(), 1))
        self.loop.set_exception_handler(None)

        self.assertEqual(len(close_reasons), 1)
        self.assertIsInstance(close_reasons[0],
                              exceptions.ConnectionClosedByClient)
        self.assertEqual(disconnect_calls, [None])
        self.assertEqual(transport.abort_calls, 1)
        self.assertEqual(loop_errors, [])
        self.assertIsNone(connection_obj._blocked_conn_timer)
        self.assertTrue(finalized.is_set())

    async def _until(self, predicate):
        while not predicate():
            await asyncio.sleep(0)
