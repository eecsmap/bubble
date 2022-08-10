import logging
import socket
import threading

from protocol import read_message, write_message

class SessionException(Exception):
    '''
    Session exception
    '''

class Session:

    def __init__(self, socket, remote_address, read_callback, close_callback=None):
        self.socket = socket
        self.remote_address = remote_address
        self.output_messages = []
        self.read_callback = read_callback
        self.close_callback = close_callback
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
                    message = self.output_messages.pop(0)
                    write_message(self.socket.send, message)
        except:
            logging.warning(f'{self.remote_address} disconnected with exception in write')
            self.close()
    
    def write_message(self, message):
        if self.is_active:
            self.output_messages.append(message)
        else:
            # if you think caller should check is_active() first before calling write_message,
            # it is not guaranteed the session is still active when you write message to it after the check.

            # we should raise the exception so that caller can remove the session
            # instead of relying on close callback
            raise SessionException(f'session of {self.remote_address} is closed')

    def _read(self):
        try:
            while self.is_active:
                message = read_message(self.socket.recv)
                try:
                    # exception from callback should not break session threads
                    self.read_callback(self, message)
                except:
                    logging.warning('exception raised when calling read callback')
        except:
            logging.warning(f'{self.remote_address} disconnected with exception in read')
            self.close()

    def close(self):
        # multiple threads might call close() at the same time
        with self.lock:
            if self.is_active:
                self.is_active = False
                try:
                    self.socket.shutdown(socket.SHUT_RDWR)
                except:
                    logging.warning('exception when calling session socket shutdown()')
                finally:
                    try:
                        self.socket.close()
                    except:
                        pass

                if self.close_callback:
                    try:
                        self.close_callback(self)
                    except:
                        logging.exception('exception raised when calling close callback')
                # since the threads are daemon threads, no need to block the caller by joining them
                #self.read_thread.join()
                #self.write_thread.join()
