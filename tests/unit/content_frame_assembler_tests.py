"""Tests for pika.channel.ContentFrameAssembler."""

import io
import marshal
import unittest
from unittest import mock

from pika import channel, exceptions, frame, spec


class ContentFrameAssemblerTests(unittest.TestCase):

    def setUp(self):
        self.obj = channel.ContentFrameAssembler()

    def test_init_method_frame(self):
        self.assertEqual(self.obj._method_frame, None)

    def test_init_header_frame(self):
        self.assertEqual(self.obj._header_frame, None)

    def test_init_seen_so_far(self):
        self.assertEqual(self.obj._seen_so_far, 0)

    def test_init_body_accumulator(self):
        self.assertIsNone(self.obj._body)

    def test_process_with_basic_deliver(self):
        value = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(value)
        self.assertEqual(self.obj._method_frame, value)

    def test_process_with_content_header(self):
        value = frame.Header(1, 100, spec.BasicProperties())
        self.obj.process(value)
        self.assertEqual(self.obj._header_frame, value)

    def test_process_with_body_frame_partial(self):
        value = frame.Header(1, 100, spec.BasicProperties())
        self.obj.process(value)
        value = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(value)
        value = frame.Body(1, b'abc123')
        self.obj.process(value)
        self.assertIs(self.obj._body, value.fragment)

    def test_process_with_full_message(self):
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 6, spec.BasicProperties())
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'abc123')
        response = self.obj.process(body_frame)
        self.assertEqual(response, (method_frame, header_frame, b'abc123'))

    def test_process_with_three_body_frames_returns_immutable_bytes(self):
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 6, spec.BasicProperties())
        self.obj.process(header_frame)
        self.obj.process(frame.Body(1, b'ab'))
        self.obj.process(frame.Body(1, b'cd'))
        response = self.obj.process(frame.Body(1, b'ef'))
        self.assertEqual(response, (method_frame, header_frame, b'abcdef'))
        self.assertIsInstance(response[2], bytes)

    def test_process_with_body_frame_six_bytes(self):
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 10, spec.BasicProperties())
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'abc123')
        self.obj.process(body_frame)
        self.assertEqual(self.obj._seen_so_far, 6)

    def test_process_with_body_frame_too_big(self):
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 6, spec.BasicProperties())
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'abcd1234')
        self.assertRaises(exceptions.BodyTooLongError, self.obj.process,
                          body_frame)

    def test_oversized_body_fragment_fails_before_accumulator_growth(self):
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 6, spec.BasicProperties())
        self.obj.process(header_frame)
        self.obj.process(frame.Body(1, b'abc'))

        with mock.patch('pika.channel.io.BytesIO', wraps=io.BytesIO) as buffer:
            with self.assertRaises(exceptions.BodyTooLongError) as raised:
                self.obj.process(frame.Body(1, b'defgh'))

        self.assertEqual((8, 6), raised.exception.args)
        buffer.assert_not_called()
        self.assertEqual(8, self.obj._seen_so_far)

    def test_process_with_unexpected_frame_type(self):
        value = frame.Method(1, spec.Basic.Qos())
        self.assertRaises(exceptions.UnexpectedFrameError, self.obj.process,
                          value)

    def test_reset_method_frame(self):
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 10, spec.BasicProperties())
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'abc123')
        self.obj.process(body_frame)
        self.obj._reset()
        self.assertEqual(self.obj._method_frame, None)

    def test_reset_header_frame(self):
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 10, spec.BasicProperties())
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'abc123')
        self.obj.process(body_frame)
        self.obj._reset()
        self.assertEqual(self.obj._header_frame, None)

    def test_reset_seen_so_far(self):
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 10, spec.BasicProperties())
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'abc123')
        self.obj.process(body_frame)
        self.obj._reset()
        self.assertEqual(self.obj._seen_so_far, 0)

    def test_reset_body_accumulator(self):
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 10, spec.BasicProperties())
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'abc123')
        self.obj.process(body_frame)
        self.obj._reset()
        self.assertIsNone(self.obj._body)

    def test_ascii_bytes_body_instance(self):
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 11, spec.BasicProperties())
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'foo-bar-baz')
        result = self.obj.process(body_frame)
        assert result is not None
        method_frame, header_frame, body_value = result
        self.assertIsInstance(body_value, bytes)

    def test_ascii_body_value(self):
        expectation = b'foo-bar-baz'
        self.obj = channel.ContentFrameAssembler()
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        header_frame = frame.Header(1, 11, spec.BasicProperties())
        self.obj.process(header_frame)
        body_frame = frame.Body(1, b'foo-bar-baz')
        result = self.obj.process(body_frame)
        assert result is not None
        method_frame, header_frame, body_value = result
        self.assertEqual(body_value, expectation)
        self.assertIsInstance(body_value, bytes)

    def test_binary_non_unicode_value(self):
        expectation = ('a', 0.8)
        self.obj = channel.ContentFrameAssembler()
        method_frame = frame.Method(1, spec.Basic.Deliver())
        self.obj.process(method_frame)
        marshalled_body = marshal.dumps(expectation)
        header_frame = frame.Header(1, len(marshalled_body),
                                    spec.BasicProperties())
        self.obj.process(header_frame)
        body_frame = frame.Body(1, marshalled_body)
        result = self.obj.process(body_frame)
        assert result is not None
        method_frame, header_frame, body_value = result
        self.assertEqual(marshal.loads(body_value), expectation)
