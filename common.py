import threading
import logging

from protocol import read_message, write_message

POOL_WIDTH, POOL_HEIGHT = 800, 600
BUBBLE_MAX_RADIUS = 20
BUBBLE_MIN_RADIUS = 10
BUBBLE_MIN_VALUE = 1
BUBBLE_MAX_VALUE = 20
BUBBLE_MIN_LIFETIME_SEC, BUBBLE_MAX_LIFETIME_SEC = 3, 6

class SessionException(Exception):
    '''
    Session exception
    '''

class Session:

    def __init__(self, socket, remote_address, read_callback, close_callback=None):
        self.socket = socket
        self.remote_address = remote_address
        self.output_messages = []
        self.is_active = True
        self.close_callback = close_callback
        self.write_thread = threading.Thread(target=self._write, args=(), daemon=True)
        self.write_thread.start()
        self.read_callback = read_callback
        self.read_thread = threading.Thread(target=self._read, args=(), daemon=True)
        self.read_thread.start()

    def _write(self):
        try:
            while self.is_active:
                while self.output_messages:
                    message = self.output_messages.pop(0)
                    write_message(self.socket.send, message)
        except Exception as e:
            msg = f'{self.remote_address} disconnected with exception in write: {e}'
            logging.info(msg)
            self.close()
    
    def write_message(self, message):
        if self.is_active:
            self.output_messages.append(message)
        else:
            msg = f'session {self.remote_address} is closed'
            logging.warning(msg)
            raise SessionException(msg)

    def _read(self):
        try:
            while self.is_active:
                message = read_message(self.socket.recv)
                self.read_callback(self, message)
        except Exception as e:
            msg = f'{self.remote_address} disconnected with exception in read: {e}'
            logging.info(msg)
            self.close()

    def close(self):
        self.is_active = False
        if self.close_callback:
            self.close_callback(self)
            self.close_callback = None
