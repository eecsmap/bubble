import logging
import socket
import threading
from collections import deque

from protocol import read_message, write_message

class SessionException(Exception):
    '''
    Session exception
    '''

class Session:

    def __str__(self):
        return f'Session {self.remote_address}'

    def __init__(self, socket, remote_address, handle_message):
        self.socket = socket
        self.remote_address = remote_address
        self.output_messages = deque() # deque is thread safe for append() and popleft()
        self.handle_message = handle_message
        self.lock = threading.Lock()
        self.is_active = True
        self.read_thread = threading.Thread(target=self._read, args=(), daemon=True)
        self.write_thread = threading.Thread(target=self._write, args=(), daemon=True)
        self.read_thread.start()
        self.write_thread.start()

    def _write(self):
        try:
            while self.is_active:
                while self.output_messages:
                    message = self.output_messages.popleft()
                    write_message(self.socket.send, message)
        except:
            pass # logging.warning(f'{self} disconnected with exception in write')
            self.close()
    
    def write_message(self, message):
        if self.is_active:
            self.output_messages.append(message)
        else:
            # if you think caller should check is_active() first before calling write_message,
            # it is not guaranteed the session is still active when you write message to it after the check.

            # we should raise the exception so that caller can remove the session
            # instead of relying on close callback
            raise SessionException(f'{self} is closed')

    def _read(self):
        try:
            while self.is_active:
                message = read_message(self.socket.recv)
                try:
                    self.handle_message(self, message)
                except:
                    pass # logging.exception(f'exception raised when caller is handling {message} from {self}')
        except:
            pass # logging.warning(f'{self} disconnected with exception in read')
            self.close()

    def close(self):
        # multiple threads might call close() at the same time
        with self.lock:
            if self.is_active:
                self.is_active = False
                try:
                    self.socket.shutdown(socket.SHUT_RDWR)
                except:
                    pass # logging.warning(f'exception when calling socket.shutdown() of {self}')
                finally:
                    try:
                        self.socket.close()
                    except:
                        pass

                # since the threads are daemon threads, no need to block the caller by joining them
                #self.read_thread.join()
                #self.write_thread.join()
