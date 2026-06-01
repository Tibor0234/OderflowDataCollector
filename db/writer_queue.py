import asyncio


class WriterQueue:
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue

    async def push(self, table: str, data: dict | list, session_pair_id: int):
        if not isinstance(data, list):
            data = [data]
        
        for record in data:
            await self.queue.put({
                "table": table,
                "data": {
                    **record,
                    "session_pair_id": session_pair_id
                }
            })