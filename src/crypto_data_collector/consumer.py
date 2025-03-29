import asyncio

class Consumer:
    def __init__(self):
        pass
        
    async def consumer_delegator(self, data_queue):    
        while True:
            data = await data_queue.get()
            await self.consumer_1(data)

    async def consumer_1(self, data:dict):
        print("Consumer 1 Ran")
        print("Data:", data)