import heapq
from binance.depthcache import DepthCache
from binance.depthcache import FuturesDepthCacheManager
from binance.client import *


def patch_Client():
    '''
    
    Remove time.sleep(1)
    
        # sleep after every 3rd call to be kind to the API
        if idx % 3 == 0:
            time.sleep(1)
    
    Use this patch if you need to call historical_kline faster. 
    BE CAREFUL NOT TO EXCEED THE LIMIT.
            
    '''    
    
    def _historical_klines(self, symbol, interval, start_str, end_str=None, limit=500,
                           klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT):
        """Get Historical Klines from Binance (spot or futures)

        See dateparser docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/

        If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"

        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str
        :param start_str: Start date string in UTC format or timestamp in milliseconds
        :type start_str: str|int
        :param end_str: optional - end date string in UTC format or timestamp in milliseconds (default will fetch everything up to now)
        :type end_str: None|str|int
        :param limit: Default 500; max 1000.
        :type limit: int
        :param limit: Default 500; max 1000.
        :type limit: int
        :param klines_type: Historical klines type: SPOT or FUTURES
        :type klines_type: HistoricalKlinesType

        :return: list of OHLCV values

        """
        # init our list
        output_data = []

        # convert interval to useful value in seconds
        timeframe = interval_to_milliseconds(interval)

        start_ts = convert_ts_str(start_str)

        # establish first available start timestamp
        first_valid_ts = self._get_earliest_valid_timestamp(symbol, interval, klines_type)
        start_ts = max(start_ts, first_valid_ts)

        # if an end time was passed convert it
        end_ts = convert_ts_str(end_str)

        idx = 0
        while True:
            # fetch the klines from start_ts up to max 500 entries or the end_ts if set
            temp_data = self._klines(
                klines_type=klines_type,
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_ts,
                endTime=end_ts
            )

            # handle the case where exactly the limit amount of data was returned last loop
            if not len(temp_data):
                break

            # append this loops data to our output data
            output_data += temp_data

            # set our start timestamp using the last value in the array
            start_ts = temp_data[-1][0]

            idx += 1
            # check if we received less than the required limit and exit the loop
            if len(temp_data) < limit:
                # exit the while loop
                break

            # increment next call by our timeframe
            start_ts += timeframe

            # sleep after every 3rd call to be kind to the API
            # if idx % 10 == 0:
            #     time.sleep(1)

        return output_data
    
    Client._historical_klines = _historical_klines


def patch_AsyncClient():
    '''
    Occasionally, ping() or get_server_time() is not responded which makes the program fall into infinite wait.
    so I skipped those two coroutines from create method.
    '''
    
    @classmethod
    async def create(
        cls, 
        api_key=None, 
        api_secret=None,
        requests_params=None, 
        tld='com',
        testnet=False, 
        loop=None
    ):

        self = cls(api_key, api_secret, requests_params, tld, testnet, loop)
        return self

    AsyncClient.create = create


def patch_DepthCache():
    '''
    
    A few changes described bellow.
    
    1. Modified to explicitly convert the type.
    
    2. Make the code for removing cache which has size 0 actually work (add_bid, add_ask)
    
    3. Make it to use heap sort for performance (get_bids, get_asks)
    
    4. Make it to use default 100 depth.
    
    '''
    
    def add_bid(self, bid):
        price, size = float(bid[0]), float(bid[1])
        self._bids[price] = size
        if size==0:
            del self._bids[price]

    def add_ask(self, ask):
        price, size = float(ask[0]), float(ask[1])
        self._asks[price] = size
        if size==0:
            del self._asks[price]

    def get_bids(self, n=100):
        keys = heapq.nlargest(n, self._bids)
        return [[k,self._bids[k]] for k in keys]

    def get_asks(self, n=100):
        keys = heapq.nsmallest(n, self._asks)
        return [[k,self._asks[k]] for k in keys]
    
    DepthCache.add_ask = add_ask
    DepthCache.add_bid = add_bid
    DepthCache.get_asks = get_asks
    DepthCache.get_bids = get_bids


def patch_FuturesDepthCacheManager():
    '''
    
    A few changes described bellow.
    
    _get_socket
        modified to utilize depth parameter (forced to use default 0 within the original code.)
        
    _init_cache
        modified to use future order book NOT SPOT ORDER BOOK
        (it was using spot order book within the original code even though it was FuturesDepthCacheManager.)
        
    _apply_orders
        modified to update all depth level NOT ONLY FIRST DEPTH LEVEL
        
    '''
    
    def _get_socket(self):
        sock = self._bm.futures_depth_socket(self._symbol, depth='')
        return sock

    async def _init_cache(self):
        """Initialise the depth cache calling REST endpoint

        :return:
        """
        self._last_update_id = None
        self._depth_message_buffer = []

        res = await self._client.futures_order_book(symbol=self._symbol, limit=self._limit)

        # initialise or clear depth cache
        await super(self.__class__,self)._init_cache()

        # process bid and asks from the order book
        self._apply_orders(res)
        for bid in res['bids']:
            self._depth_cache.add_bid(bid)
        for ask in res['asks']:
            self._depth_cache.add_ask(ask)

        # set first update id
        self._last_update_id = res['lastUpdateId']

        # Apply any updates from the websocket
        for msg in self._depth_message_buffer:
            await self._process_depth_message(msg)

        # clear the depth buffer
        self._depth_message_buffer = []

    def _apply_orders(self, msg):
        for bid in msg.get('b', []) + msg.get('bids', []):
            self._depth_cache.add_bid(bid)
        for ask in msg.get('a', []) + msg.get('asks', []):
            self._depth_cache.add_ask(ask)

        # keeping update time
        self._depth_cache.update_time = msg.get('E') or msg.get('lastUpdateId')
    
    FuturesDepthCacheManager._get_socket = _get_socket
    FuturesDepthCacheManager._init_cache = _init_cache
    FuturesDepthCacheManager._apply_orders = _apply_orders
    


patch_AsyncClient()
patch_FuturesDepthCacheManager()
patch_DepthCache()