import datetime

async def data_producer(
    data_queue,
    symbol_name: str,
    stream_method,
    stream_name:str,
    stream_options: dict[str, any],
    key_str:str):
    while True:
        data = await stream_method(symbol_name, **stream_options)
        now_ms = int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)
        # Inject meta data
        full_data = {
            "symbol_name":symbol_name,
            "stream_name":stream_name,
            "key_str":key_str,
            "data":data,
            "time_receive":now_ms
        }
        await data_queue.put(full_data)

def create_producers(data_queue, config, exchange_objects, consumers, producers):
    for exchange_name, exchange in exchange_objects.items():
        for symbol_name, symbol_streams in config['exchanges'][exchange_name]['symbols'].items():
            for streams in symbol_streams['streams']:
                for stream_name, stream_options in streams.items():
                    
                    key_str = (
                        f"{exchange_name}*"
                        f"{symbol_name}*"
                        f"{stream_name}"
                        )
                    
                    stream_options = stream_options['options']
                    stream_method = getattr(exchange, stream_name)
                    producers.append(data_producer(
                        data_queue=data_queue,
                        symbol_name=symbol_name,
                        stream_method=stream_method,
                        stream_name=stream_name,
                        stream_options=stream_options,
                        key_str=key_str
                        ))
    return producers