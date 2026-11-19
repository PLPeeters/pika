"""A downstream coroutine consumer on AsyncioConnection."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pika.adapters.asyncio_connection import AsyncioConnection

if TYPE_CHECKING:
    from pika import spec
    from pika.channel import Channel


async def _on_message(channel: Channel, method: spec.Basic.Deliver,
                      properties: spec.BasicProperties, body: bytes) -> None:
    assert method.delivery_tag is not None
    channel.basic_ack(method.delivery_tag)


def accepts_a_coroutine_consumer(connection: AsyncioConnection) -> None:
    channel = connection.channel()
    channel.basic_consume('queue', _on_message)

    connection.channel(on_open_callback=lambda opened_channel: opened_channel.
                       basic_consume('queue', _on_message))


def _on_close(connection: AsyncioConnection, _reason: BaseException) -> None:
    connection.ioloop.stop()


def accepts_typed_close_callback() -> None:
    AsyncioConnection(on_close_callback=_on_close)
