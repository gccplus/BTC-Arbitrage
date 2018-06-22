import websocket
import time
from threading import Thread
import json
import hashlib


class OkexSocketManager():
    def __init__(self, api_key, secret_key, callback):
        # threading.Thread.__init__(self)
        self.api_key = api_key
        self.secret_key = secret_key

        #websocket.enableTrace(True)
        self._user_callback = callback
        self.sub_events = []
        self.ws = websocket.WebSocketApp("wss://real.okex.com:10441/websocket",
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close,
                                         on_open=self.on_open)

        self.thread = Thread(target=self.ws.run_forever, kwargs={'ping_interval': 20})
        # self.thread.start()

    def start(self):
        self.thread.start()

    def start_user_socket(self):
        params = {'api_key': self.api_key}
        sign = ''
        for key in sorted(params.keys()):
            sign += key + '=' + str(params[key]) + '&'
        params['sign'] = hashlib.md5((sign + 'secret_key=' + self.secret_key).encode("utf-8")).hexdigest().upper()
        data = {
            'event': 'login',
            'parameters': params
        }
        self.sub_events.append(data)

        data = {
            'event': 'addChannel',
            'channel': 'ok_sub_spot_btc_usdt_order'
        }
        self.sub_events.append(data)

    def start_future_depth_socket(self, symbol, callback):
        data = "{'event':'addChannel','channel':'ok_sub_futureusd_%s_ticker_quarter'}" % symbol
        self.sub_events.append(data)
        self._user_callback = callback

    def start_spot_depth_socket(self, symbols):
        for symbol in symbols:
            data1 = {'event': 'addChannel', 'channel': 'ok_sub_spot_%s_depth_5' % symbol}
            data2 = {'event': 'addChannel', 'channel': 'ok_sub_spot_%s_order' % symbol}
            self.sub_events.append(data1)
            self.sub_events.append(data2)

        # for coin in ['okb', 'btc', 'eth', 'usdt']:
        #     data = {'event': 'addChannel', 'channel': 'ok_sub_spot_%s_balance' % coin}
        #     self.sub_events.append(data)
        # self.ws.send(json.dumps(self.sub_events))

    def on_error(self, ws, error):
        print 'error: %s' % error

    def on_close(self, ws):
        print 'closed'
        print 'reconnect'
        if self.thread and self.thread.isAlive():
            self.ws.close()
            self.thread.join()
        self.ws = websocket.WebSocketApp("wss://real.okex.com:10441/websocket",
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close,
                                         on_open=self.on_open)
        self.thread = Thread(target=self.ws.run_forever, kwargs={'ping_interval': 20})
        self.thread.start()

    def on_open(self, ws):
        print 'opened'
        self.ws.send(json.dumps(self.sub_events))

    def on_message(self, ws, messages):
        self._user_callback(messages)

    def run(self):
        try:
            # self.setDaemon(True)
            self.ws.run_forever(ping_interval=20, ping_timeout=5)
        except Exception as e:
            print e


if __name__ == "__main__":
    def on_message(msg):
        print msg

    websocket.enableTrace(True)
    manager = OkexSocketManager('ee02b2e1-b183-410d-883f-c22afc81b12e', '983676E3B392EF4AA67F1F7CF06E2ABB', on_message)
    manager.start_user_socket()
    manager.start_spot_depth_socket(['btc_usdt', 'okb_btc'])
    manager.start()

