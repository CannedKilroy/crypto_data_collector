# Custom Exceptions
class UnregisteredExchange(Exception):
	def __init__(self, exchange: str):
		message = f"Exchange '{exchange}' is not registered"
		super().__init__(message)
		self.exchange = exchange
class UnregisteredSymbol(Exception):
	def __init__(self, symbol: str, exchange: str):
		message = f"Symbol '{symbol}' is not registered for exchange '{exchange}'"
		super().__init__(message)
		self.symbol = symbol
		self.exchange = exchange
class UnregisteredStream(Exception):
	def __init__(self, stream:str, symbol: str, exchange: str):
		message = f"Stream: '{stream}' for Symbol '{symbol}' is not registered for exchange '{exchange}'"
		super().__init__(message)
		self.stream = stream
		self.symbol = symbol
		self.exchange = exchange