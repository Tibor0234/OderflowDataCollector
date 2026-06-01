import pytest
import asyncio
from db.writer_queue import WriterQueue


@pytest.mark.asyncio
async def test_push_single_trade():
    queue = asyncio.Queue()

    writer = WriterQueue(queue)

    await writer.push(
        "trades",
        {
            "p": "100",
            "q": "1"
        },
        123
    )

    item = await queue.get()

    assert item["table"] == "trades"
    assert item["data"]["session_pair_id"] == 123
    assert item["data"]["p"] == "100"


@pytest.mark.asyncio
async def test_push_batch_trades():
    queue = asyncio.Queue()
    writer = WriterQueue(queue)

    data = [
        {"p": "100", "q": "1"},
        {"p": "101", "q": "2"},
        {"p": "102", "q": "3"},
    ]

    await writer.push("trades", data, 123)

    assert queue.qsize() == 3

    items = [await queue.get() for _ in range(3)]

    for item in items:
        assert item["table"] == "trades"
        assert item["data"]["session_pair_id"] == 123