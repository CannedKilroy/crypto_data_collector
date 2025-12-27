import ccxt.pro
import logging

from pprint import pformat
from typing import Callable, Optional, Dict, Any

from crypto_data_collector.exceptions import UnregisteredExchange, UnregisteredStream, UnregisteredSymbol
from crypto_data_collector.producer import DataProducer

logger = logging.getLogger(__name__)


class Registry:
    """
    Configuration registry for ccxt.pro exchanges, symbols, and streams.
    Keeps a nested dict of validated Exchanges -> Symbols -> Streams,
    along with the initialized exchange objects and stream methods.

    Registered streams do not need to be running or even created,
    this is simply a helper class to hold config data for verifying 
    valid producer data and creating producer instances
    """

    def __init__(self) -> None:
        self.registered = {"exchanges":{}}
        logger.info("Registry created")

# Register methods
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
    async def register_exchange(
        self,
        exchange_name:str,
        exchange_overrides: Dict[str, Any] = {}
        ) -> None:
        """
        Register an exchange by name, optionally with custom initialization parameters.

        Args:
            exchange_name (str): The ccxt.pro exchange name (e.g., 'binance').
            exchange_overrides (Dict[str, Any], optional): Any configuration overrides.

        Raises:
            Exception: If exchange initialization or market loading fails.
        
        Returns:
            None
        """
        if self.exchange_registered(exchange_name):
            logger.info("Exchange [%s] already registered. Skipping ...", exchange_name)
            return
        
        logger.info("Registering exchange [%s] with overrides: %s", exchange_name, exchange_overrides)
        exchange_class = getattr(ccxt.pro, exchange_name)
        try:
            exchange_obj = exchange_class(exchange_overrides)
            await exchange_obj.load_markets()
        except Exception as e:
            logger.exception("Failed to register exchange: [%s]: %s", exchange_name, e)
            raise e

        self.registered["exchanges"][exchange_name] = {
            "object" : exchange_obj,
            "overrides": exchange_overrides,
            "symbols" : {}
            }
        logger.info("Exchange [%s] registered", exchange_name)


    async def register_symbol(
        self,
        exchange_name:str,
        symbol:str
        ) -> None:
            """
            Register a symbol to an exchange.

            Args:
                exchange_name (str): Name of the registered exchange.
                symbol (str): Trading symbol (e.g., 'BTC/USDT').

            Raises:
                UnregisteredExchange: If the exchange is not registered.
                AttributeError: If the symbol is not supported by the exchange.
            
            Returns:
                None
            """
            if not self.exchange_registered(exchange_name):
                logger.exception("Exchange: [%s] not registered", exchange_name)
                raise UnregisteredExchange(exchange_name)
            if self.symbol_registered(symbol, exchange_name):
                logger.info("Symbol: [%s] already registered to exchange: [%s]", symbol, exchange_name)
                return

            exchange_obj = self.registered["exchanges"][exchange_name]["object"]
            if symbol not in exchange_obj.symbols:
                logger.exception("Symbol: [%s] not a valid symbol for exchange: [%s]", symbol, exchange_name)
                raise AttributeError(f"Invalid symbol: {symbol} for exchange: {exchange_name}")
            self.registered["exchanges"][exchange_name]["symbols"][symbol] = {
                "streams" : {},
                }
            logger.info("Symbol: [%s] registered to exchange [%s]", symbol, exchange_name)


    async def register_stream(
        self,
        exchange_name: str,
        symbol: str,
        stream_name: str,
        stream_options:Optional[Dict[str, Any]] = None,
        stream_consumer_options:Optional[Dict[str, Any]] = None
        ) -> None:
        """
        Register a stream (e.g., watch_ticker, watch_trades) for a symbol on an exchange.

        Args:
            exchange_name (str): Exchange name.
            symbol (str): Trading symbol.
            stream_name (str): Name of the stream method in ccxt.pro.
            stream_options (Dict[str, Any], optional): Arguments passed to the stream method.

        Raises:
            UnregisteredExchange: If exchange is not registered.
            UnregisteredSymbol: If symbol is not registered.
            AttributeError: If stream is unsupported.
        
        Returns:
            None
        """
        if not self.exchange_registered(exchange_name):
            logger.exception("Exchange: [%s] not registered", exchange_name)
            raise UnregisteredExchange(exchange_name)
        if not self.register_symbol(exchange_name, symbol):
            logger.exception("Symbol: [%s] of exchange: [%s] not registered", symbol, exchange_name)
            raise UnregisteredSymbol(symbol, exchange_name)            
        if self.stream_registered(stream_name, symbol, exchange_name):
            logger.info("Stream: [%s] already registered to Symbol: [%s] Exchange: [%s]", stream_name, symbol, exchange_name)
            return
        
        exchange_obj = self.registered["exchanges"][exchange_name]["object"]
        support = exchange_obj.has.get(stream_name, None)
        if support is None or False:
            logger.exception("Stream: [%s] for Symbol: [%s] for Exchange: [%s] is not yet implemented in CCXT / undefined / not supported", stream_name, symbol, exchange_name)
            raise AttributeError(f"Stream: '{stream_name}' for Symbol: '{symbol}' for Exchange: '{exchange_name}' is not yet implemented in CCXT / undefined / not supported")

        stream_method = getattr(exchange_obj, stream_name)
        
        self.registered["exchanges"][exchange_name]["symbols"][symbol]["streams"][stream_name] = {
            "stream_method" : stream_method,
            "stream_options" : stream_options or {},
            "stream_consumer_options" : stream_consumer_options or {}
            }
        logger.info("Stream: [%s] for Symbol: [%s] registered to exchange [%s]", stream_name, symbol, exchange_name)


# Registry checks
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
    def exchange_registered(self, exchange_name:str) -> bool:
        """
        Checks if exchange is registered

        Args:
            exchange_name (str): Exchange name to check
        
        Returns:
            Bool
        """
        if self.registered["exchanges"].get(exchange_name, None) is not None:
            return True
        else:
            return False
    
    def symbol_registered(self, symbol:str, exchange_name:str) -> bool:
        """
        Checks if symbol is registered for an exchange
        Exchange is expected to be registered, raises unregistered if not

        Args:
            exchange_name (str): Exchange name
            symbol (str): Symbol to check for exchange
        
        Returns:
            Bool
        """
        if not self.exchange_registered(exchange_name):
            raise UnregisteredExchange(exchange_name)

        if self.registered["exchanges"][exchange_name]["symbols"].get(symbol, None) is not None:
            return True
        else:
            return False

    def stream_registered(self, stream_name:str, symbol:str, exchange_name:str) -> bool:
        """
        Checks if stream is registered for symbol / exchange
        Registered Symbol / Exchange are expected with exceptions raised if not 
        Args:
            exchange_name (str): Exchange name
            symbol (str): Symbol name
            stream (str): Stream to check
        
        Returns:
            Bool
        """
        if not self.exchange_registered(exchange_name):
            raise UnregisteredExchange(exchange_name)
        
        if not self.symbol_registered(symbol, exchange_name):
            raise UnregisteredSymbol(symbol, exchange_name)

        if self.registered["exchanges"][exchange_name]["symbols"][symbol]["streams"].get(stream_name, None) is not None:
            return True
        else:
            return False


# Registry helper methods
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
    def has_registered_streams(self, exchange_name:str) -> bool:
        """
        Checks if an exchange has registered streams
        Args:
            exchange_name(str): Exchange to check
        Return:
            Bool
        """
        if not self.exchange_registered(exchange_name):
            raise UnregisteredExchange(exchange_name)
        symbols = self.registered["exchanges"][exchange_name]["symbols"]
        for symbol_info in symbols.values():
            streams = symbol_info.get("streams", {})
            if streams:
                return True
        return False
    
    def get_exchange_object(self, exchange_name:str) -> ccxt.pro.Exchange:
        """
        Gets the exchange object for a specific registered exchange
        Note:
            - This is simply a helper for creating the data producer task,
            do not use for exchange cleanup
        Args:
            exchange_name(str): Name of exchange name to get
        Return:
            bool
        """
        if not self.exchange_registered(exchange_name):
            raise UnregisteredExchange(exchange_name)
        return self.registered["exchanges"][exchange_name]["object"]
    
    def get_stream_method(
        self,
        exchange_name:str,
        symbol:str,
        stream_name:str
    ) -> Callable[..., Any]:
        """
        Gets the stream method for a specific registered exchange
        Args:
            exchange_name(str): Name of exchange name to get
            symbol(str): Name of symbol to get
            stream_name(str): Name of stream name to get
        Return:
            Callable
        """
        if not self.stream_registered(stream_name, symbol, exchange_name):
            raise UnregisteredStream(stream_name, symbol, exchange)
        return self.registered["exchanges"][exchange_name]["symbols"][symbol]["streams"][stream_name]["stream_method"]
    
    def get_stream_options(
        self,
        exchange_name,
        symbol,
        stream_name
        ):
        if not self.stream_registered(stream_name, symbol, exchange_name):
            raise UnregisteredStream(stream_name, symbol, exchange)
        return self.registered["exchanges"][exchange_name]["symbols"][symbol]["streams"][stream_name]["stream_options"]



# Registry Unregister methods
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
    def unregister_stream(
        self,
        exchange_name: str,
        symbol: str,
        stream_name: str
        ) -> None:
        """
        Unregister a stream from a symbol on a given exchange.

        This removes the stream's metadata from the registry, but does not stop any
        running producer. It is the caller's responsibility to ensure the associated
        data producer is stopped separately, if one is active.

        Args:
            exchange_name (str): The name of the exchange.
            symbol (str): The symbol associated with the stream.
            stream_name (str): The name of the stream (e.g., 'watch_trades').

        Raises:
            UnregisteredStream: If the stream is not registered under the given exchange and symbol.
        """
        if not self.stream_registered(stream_name, symbol, exchange_name):
            logger.error(
                "Attempted to unregister nonexistant stream: [%s.%s.%s]",
                exchange_name,
                symbol,
                stream_name
                )
            raise UnregisteredStream(stream_name, symbol, exchange_name)
        
        self.registered["exchanges"][exchange_name]["symbols"][symbol]["streams"].pop(stream_name)
        logger.info("Unregistered stream [%s.%s.%s]", exchange_name, symbol, stream_name)


    def unregister_symbol(
        self,
        exchange_name: str,
        symbol: str,
        force: bool = False
        ) -> None:
        """
        Unregister a symbol from a registered exchange.

        If any streams are still registered under this symbol, the operation will fail
        unless `force=True` is provided. This method only affects the registry and does
        not stop any running producers.

        Args:
            exchange_name (str): The name of the exchange.
            symbol (str): The symbol to unregister (e.g., 'BTC/USDT').
            force (bool, optional): If True, forcefully removes the symbol even if streams are registered.

        Raises:
            UnregisteredSymbol: If the symbol is not registered to the given exchange.
            RuntimeError: If streams are still registered under the symbol and `force=False`.
        """
        if not self.symbol_registered(symbol, exchange_name):
            logger.exception(
                "Attempted to unregister nonexistant symbol: [%s] for exchange: [%s]",
                exchange_name,
                symbol,
                )
            raise UnregisteredSymbol(symbol, exchange_name)
        if self.registered["exchanges"][exchange_name]["symbols"][symbol]["streams"] and not force:
            logger.warning(
                "Attempt to unregister symbol [%s] on exchange [%s] blocked — active streams still registered. Use force=True to override.",
            symbol,
            exchange_name
            )
            raise RuntimeError(f"Exchange [{exchange_name}] still has streams registered to symbol [{symbol}]. Use force=True to override.")

        self.registered["exchanges"][exchange_name]["symbols"].pop(symbol)
        logger.info("Unregistered symbol [%s] on exchange [%s]", symbol, exchange_name)


    def unregister_exchange(
        self,
        exchange_name: str,
        force: bool = False
        ) -> None:
        """
        Unregister an exchange from the registry.

        If the exchange still has symbols (and possibly streams) registered under it,
        the operation will fail unless `force=True` is provided. This only updates the
        internal registry and does not stop or close the exchange connection object.

        Args:
            exchange_name (str): The name of the exchange to unregister.
            force (bool, optional): If True, forcefully removes the exchange even if symbols remain.

        Raises:
            UnregisteredExchange: If the exchange is not currently registered.
            RuntimeError: If the exchange still has symbols registered and `force=False`.
        """
        if not self.exchange_registered(exchange_name):
            logger.exception(
                "Attempted to unregister nonexistant exchange: [%s]", exchange_name
                )
            raise UnregisteredExchange(exchange_name)
        if self.registered["exchanges"][exchange_name]["symbols"] and not force:
            logger.warning(
                "Attempt to unregister exchange [%s] blocked — active symbols still registered. Use force=True to override.",
                exchange_name
                )
            raise RuntimeError(f"Exchange [{exchange_name}] still has symbols registered. Use force=True to override.")

        self.registered["exchanges"].pop(exchange_name)
        logger.info("Unregistered Exchange [%s]", exchange_name)

    def __str__(self) -> str:
        return pformat(self.registered, indent=2)
