import threading
import logging

from protocol import read_message, write_message

POOL_WIDTH, POOL_HEIGHT = 800, 600
BUBBLE_MAX_RADIUS = 20
BUBBLE_MIN_RADIUS = 10
BUBBLE_MIN_VALUE = 1
BUBBLE_MAX_VALUE = 20
BUBBLE_MIN_LIFETIME_SEC, BUBBLE_MAX_LIFETIME_SEC = 3, 6

class Session:

    def __init__(self, socket, remote_address, callback):
        self.socket = socket
        self.remote_address = remote_address
        self.output_messages = []
        self.write_thread = threading.Thread(target=self._write, args=(), daemon=True)
        self.write_thread.start()
        self.callback = callback
        self.read_thread = threading.Thread(target=self._read, args=(), daemon=True)
        self.read_thread.start()

    def _write(self):
        try:
            while True:
                while self.output_messages:
                    message = self.output_messages.pop(0)
                    write_message(self.socket.send, message)
        except BrokenPipeError:
            logging.info(f'{self.remote_address} disconnected')
    
    def write_message(self, message):
        self.output_messages.append(message)

    def _read(self):
        try:
            while True:
                message = read_message(self.socket.recv)
                self.callback(self, message)
        except BrokenPipeError:
            logging.info(f'{self.remote_address} disconnected')

    def close(self):
        pass