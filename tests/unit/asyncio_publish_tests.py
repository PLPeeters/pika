"""Tests for `AsyncioConnection.publish`, against a scripted broker on a socket pair."""

from __future__ import annotations

import asyncio
import socket
import threading
import time
import unittest

import pika
from pika import exceptions, frame, spec
from pika.adapters import asyncio_connection, base_connection
from pika.adapters.utils import io_services_utils
from tests.base.asyncio_loop import new_pika_asyncio_loop

_FRAME_MAX = 131072


class _Broker:
    """The peer end of the socket pair: sends methods and decodes what the client wrote."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.reading = True
        self._received = bytearray()
        self._offset = 0
        self.frames: list = []

    def send(self, channel_number, method):
        self.sock.sendall(frame.Method(channel_number, method).marshal())

    def read(self):
        while self.reading:
            try:
                data = self.sock.recv(1 << 20)
            except BlockingIOError:
                break
            if not data:
                break
            self._received += data
        while True:
            consumed, decoded = frame.decode_frame(self._received, self._offset)
            if not consumed:
                break
            self._offset += consumed
            self.frames.append(decoded)

    def messages(self, channel_number=None):
        """
        :returns: (channel number, routing key, body) per complete message, checking that each
            message's frames are contiguous
        """
        result = []
        current = None
        for item in self.frames:
            if (isinstance(item, frame.Method) and
                    isinstance(item.method, spec.Basic.Publish)):
                assert current is None, 'a message started inside another'
                current = [
                    item.channel_number, item.method.routing_key, None,
                    bytearray()
                ]
            elif isinstance(item, frame.Header) and current is not None:
                assert item.channel_number == current[0]
                current[2] = item.body_size
            elif isinstance(item, frame.Body):
                assert current is not None, 'a body frame outside a message'
                assert item.channel_number == current[0], 'interleaved frames'
                current[3] += item.fragment
            else:
                assert current is None, f'{item!r} inside a message'
                continue
            if current is not None and len(current[3]) == current[2]:
                result.append((current[0], current[1], bytes(current[3])))
                current = None
        if channel_number is None:
            return result
        return [message for message in result if message[0] == channel_number]


class PublishTestCase(unittest.TestCase):

    def setUp(self):
        self.loop = new_pika_asyncio_loop()
        self.addCleanup(self.loop.close)
        local, peer = socket.socketpair()
        for sock in (local, peer):
            sock.setblocking(False)
            self.addCleanup(sock.close)
        self.broker = _Broker(peer)

        self.connection = asyncio_connection.AsyncioConnection(
            pika.ConnectionParameters(heartbeat=0, frame_max=_FRAME_MAX),
            custom_ioloop=self.loop,
            internal_connection_workflow=False)
        protocol = base_connection._StreamingProtocolShim(self.connection)
        transport = io_services_utils._AsyncPlaintextTransport(
            local, protocol, self.connection._nbio)
        protocol.connection_made(transport)
        self.transport = transport

        self.broker.send(
            0,
            spec.Connection.Start(0, 9, {
                'capabilities': {
                    'publisher_confirms': True,
                    'basic.nack': True
                }
            }))
        self.broker.send(0, spec.Connection.Tune(2047, _FRAME_MAX, 0))
        self.broker.send(0, spec.Connection.OpenOk())
        self.run_until(lambda: self.connection.is_open)
        self.channel = self.open_channel(1)

    def tearDown(self):
        writer = (self.connection._publisher and
                  self.connection._publisher._writer)
        if writer is not None:
            writer.cancel()
            self.loop.run_until_complete(
                asyncio.gather(writer, return_exceptions=True))

    def open_channel(self, channel_number):
        channel = self.connection.channel(channel_number)
        self.broker.send(channel_number, spec.Channel.OpenOk())
        self.run_until(lambda: channel.is_open)
        return channel

    def run_until(self, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while True:
            self.broker.read()
            if predicate():
                return
            if time.monotonic() > deadline:
                self.fail(f'timed out waiting for {predicate!r}')
            self.loop.run_until_complete(asyncio.sleep(0.001))

    def settle(self, future):
        self.run_until(future.done)
        return future


class PublishTests(PublishTestCase):

    def test_completes_once_queued_without_confirms_or_a_broker_read(self):
        self.broker.reading = False
        frames_sent = self.connection.frames_sent
        future = self.settle(
            self.connection.publish(self.channel, 'x', 'key', b'body'))
        self.assertIsNone(future.result())
        self.assertEqual(self.connection.frames_sent, frames_sent + 3)
        self.assertEqual(self.broker.messages(), [])

        self.broker.reading = True
        self.run_until(lambda: self.broker.messages())
        self.assertEqual(self.broker.messages(), [(1, 'key', b'body')])

    def test_large_body_is_framed_off_the_loop_thread(self):
        threads = []
        marshal_body = self.connection._marshal_body

        def recording_marshal_body(channel_number, body):
            threads.append(threading.current_thread())
            return marshal_body(channel_number, body)

        self.connection._marshal_body = recording_marshal_body
        body = bytes(
            range(256)) * (asyncio_connection._EXECUTOR_BODY_SIZE // 256 + 1)
        future = self.settle(
            self.connection.publish(self.channel, 'x', 'big', body))
        self.assertIsNone(future.result())
        self.assertEqual(len(threads), 1)
        self.assertIsNot(threads[0], threading.current_thread())
        self.run_until(lambda: self.broker.messages())
        self.assertEqual(self.broker.messages(), [(1, 'big', body)])

    def test_basic_publish_still_frames_and_queues_before_returning(self):
        self.broker.reading = False
        frames_sent = self.connection.frames_sent
        body = b'x' * (asyncio_connection._EXECUTOR_BODY_SIZE + 1)
        self.channel.basic_publish('x', 'sync', body)
        self.assertEqual(self.connection.frames_sent, frames_sent + 3)

    def test_reused_properties_do_not_change_a_queued_message(self):
        properties = spec.BasicProperties(headers={'n': 1})
        futures = [
            self.connection.publish(self.channel, 'x', 'key', b'one',
                                    properties)
        ]
        properties.headers['n'] = 2
        futures.append(
            self.connection.publish(self.channel, 'x', 'key', b'two',
                                    properties))
        self.settle(futures[1])
        self.run_until(lambda: len(self.broker.messages()) == 2)
        headers = [
            item.properties.headers['n']
            for item in self.broker.frames
            if isinstance(item, frame.Header)
        ]
        self.assertEqual(headers, [1, 2])

    def test_concurrent_messages_are_written_whole_and_in_call_order(self):
        other = self.open_channel(2)
        small = b's' * 100
        large = bytes(range(256)) * (3 * _FRAME_MAX // 256)
        published = []
        futures = []
        for index in range(12):
            channel = (self.channel, other)[index % 2]
            body = (small, large, b'')[index % 3]
            futures.append(
                self.connection.publish(channel, 'x', str(index), body))
            published.append((channel.channel_number, str(index), body))

        self.run_until(lambda: len(self.broker.messages()) == 12)
        self.assertTrue(all(future.done() for future in futures))
        self.assertEqual(self.broker.messages(), published)


class BackpressureTests(PublishTestCase):

    def setUp(self):
        super().setUp()
        self.marshaled = []
        marshal_body = self.connection._marshal_body

        def counting_marshal_body(channel_number, body):
            self.marshaled.append(len(body))
            return marshal_body(channel_number, body)

        self.connection._marshal_body = counting_marshal_body

    def test_messages_wait_for_capacity_then_all_arrive_in_order(self):
        self.broker.reading = False
        size = 2 * asyncio_connection._WRITE_BUFFER_LIMIT
        bodies = [bytes([index]) * size for index in range(6)]
        futures = [
            self.connection.publish(self.channel, 'x', str(index), body)
            for index, body in enumerate(bodies)
        ]
        self.run_until(futures[0].done)
        # Let the writer reach its wait, then check nothing more is taken on.
        self.loop.run_until_complete(asyncio.sleep(0.05))
        completed = sum(future.done() for future in futures)
        self.assertLess(completed, len(futures))
        self.assertLessEqual(len(self.marshaled), completed + 1)
        self.assertLessEqual(self.transport.get_write_buffer_size(),
                             asyncio_connection._WRITE_BUFFER_LIMIT + size)

        self.broker.reading = True
        self.run_until(lambda: len(self.broker.messages()) == len(bodies))
        self.assertEqual([body for _, _, body in self.broker.messages()],
                         bodies)
        self.assertTrue(all(future.done() for future in futures))

    def test_cancelling_a_held_message_withdraws_it(self):
        self.broker.reading = False
        size = 2 * asyncio_connection._WRITE_BUFFER_LIMIT
        first = self.connection.publish(self.channel, 'x', 'first', b'1' * size)
        held = self.connection.publish(self.channel, 'x', 'held', b'2')
        last = self.connection.publish(self.channel, 'x', 'last', b'3')
        self.settle(first)
        self.assertFalse(held.done())

        held.cancel()
        self.loop.run_until_complete(asyncio.sleep(0))
        self.broker.reading = True
        self.settle(last)
        self.run_until(lambda: len(self.broker.messages()) == 2)
        self.assertEqual([key for _, key, _ in self.broker.messages()],
                         ['first', 'last'])


class FailureTests(PublishTestCase):

    def test_unencodable_method_or_properties_fail_only_that_message(self):
        cases = [
            ('k', spec.BasicProperties(headers={1: 'x'}), TypeError),
            ('k', spec.BasicProperties(headers={'k': object()}),
             exceptions.UnsupportedAMQPFieldException),
            ('k' * 256, None, exceptions.ShortStringTooLong),
        ]
        for routing_key, properties, error in cases:
            with self.subTest(error=error.__name__):
                bad = self.connection.publish(self.channel, 'x', routing_key,
                                              b'body', properties)
                good = self.connection.publish(self.channel, 'x', 'good',
                                               b'body')
                self.assertIsInstance(bad.exception(), error)
                self.assertIsNone(self.settle(good).result())
        self.assertEqual([key for _, key, _ in self.broker.messages()],
                         ['good'] * len(cases))

    def test_body_of_another_type_fails(self):
        future = self.connection.publish(self.channel, 'x', 'key',
                                         bytearray(b'body'))
        self.assertIsInstance(future.exception(), TypeError)

    def test_publishing_on_a_closed_channel_fails(self):
        self.channel.close()
        future = self.connection.publish(self.channel, 'x', 'key', b'body')
        self.assertIsInstance(future.exception(),
                              exceptions.ChannelWrongStateError)

    def test_closing_the_channel_before_the_write_fails_the_message(self):
        future = self.connection.publish(self.channel, 'x', 'key', b'body')
        self.channel.close()
        self.settle(future)
        self.assertIsInstance(future.exception(),
                              exceptions.ChannelWrongStateError)
        self.assertEqual(self.broker.messages(), [])

    def test_broker_channel_close_fails_that_channels_held_messages(self):
        other = self.open_channel(2)
        self.broker.reading = False
        size = 2 * asyncio_connection._WRITE_BUFFER_LIMIT
        first = self.connection.publish(other, 'x', 'first', b'1' * size)
        held = [
            self.connection.publish(self.channel, 'x', str(index), b'body')
            for index in range(3)
        ]
        survivor = self.connection.publish(other, 'x', 'survivor', b'body')
        self.settle(first)

        self.broker.send(1, spec.Channel.Close(404, 'NOT_FOUND', 0, 0))
        for future in held:
            self.settle(future)
        self.assertFalse(survivor.done())
        self.broker.reading = True
        self.settle(survivor)
        self.assertIsNone(survivor.result())
        for future in held:
            self.assertIsInstance(future.exception(),
                                  exceptions.ChannelClosedByBroker)

    def test_connection_loss_fails_held_messages_and_stops_the_writer(self):
        self.broker.reading = False
        size = 2 * asyncio_connection._WRITE_BUFFER_LIMIT
        first = self.connection.publish(self.channel, 'x', 'first', b'1' * size)
        held = self.connection.publish(self.channel, 'x', 'held', b'body')
        self.settle(first)

        self.broker.sock.close()
        self.settle(held)
        self.assertIsInstance(held.exception(), exceptions.StreamLostError)
        self.run_until(lambda: self.connection._publisher._writer is None)
        later = self.connection.publish(self.channel, 'x', 'later', b'body')
        self.assertIsInstance(later.exception(),
                              exceptions.ChannelWrongStateError)


class ConfirmTests(PublishTestCase):

    def test_acks_arrive_through_confirm_delivery(self):
        acks = []
        self.channel.confirm_delivery(acks.append)
        self.broker.send(1, spec.Confirm.SelectOk())
        self.run_until(lambda: any(
            isinstance(item, frame.Method) and isinstance(
                item.method, spec.Confirm.Select)
            for item in self.broker.frames))

        future = self.settle(
            self.connection.publish(self.channel, 'x', 'key', b'body'))
        self.assertIsNone(future.result())
        self.assertEqual(acks, [])
        self.broker.send(1, spec.Basic.Ack(1))
        self.run_until(lambda: acks)
        self.assertEqual(acks[0].method.delivery_tag, 1)


if __name__ == '__main__':
    unittest.main()
