import websocket
from threading import Thread
import json
import zlib


class HuobiSocketManager:
    def __init__(self, callback):
        # websocket.enableTrace(True)
        self._user_callback = callback
        self.sub_events = []
        self.ws = websocket.WebSocketApp("wss://api.huobi.pro/ws",
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close,
                                         on_open=self.on_open)

        self.thread = Thread(target=self.ws.run_forever, kwargs={'ping_interval': 20})
        # self.thread.start()

    def start(self):
        self.thread.start()

    def start_depth_socket(self, symbol, callback):
        pass

    def on_error(self, ws, error):
        print 'error: %s' % error

    def on_close(self, ws):
        print 'closed'
        print 'reconnect'
        # if self.thread and self.thread.isAlive():
        #     self.ws.close()
        #     self.thread.join()
        self.ws = websocket.WebSocketApp("wss://api.huobi.pro/ws",
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close,
                                         on_open=self.on_open)
        self.thread = Thread(target=self.ws.run_forever, kwargs={'ping_interval': 20})
        self.thread.start()

    def on_open(self, ws):
        print 'opened'
        symbol = {
            'sub': 'market.btcusdt.depth.step5',
            'id': 'idc'
        }
        self.ws.send(json.dumps(symbol, ensure_ascii=False).encode('utf8'))

    def on_message(self, ws, message):
        try:
            unzipped_data = zlib.decompress(message, 16 + zlib.MAX_WBITS)
            payload_obj = json.loads(unzipped_data)
        except ValueError:
            pass
        else:
            if 'ping' in payload_obj:
                pong = {'pong': payload_obj['ping']}
                print pong
                self.ws.send(json.dumps(pong, ensure_ascii=False).encode('utf8'))
            elif 'subbed' in payload_obj:
                print payload_obj
            else:
                self._user_callback(payload_obj)


if __name__ == "__main__":
    def on_message(msg):
        print msg


    websocket.enableTrace(True)
    manager = HuobiSocketManager(on_message)
    manager.start()
