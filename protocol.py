import json
import struct
import logging

def read_n_bytes(read, n):
    data = bytearray(read(n))
    while len(data) < n:
        data += read(n - len(data))
    assert len(data) == n
    return data

def read_message(read):
    # read message size as a four-byte integer value in network order
    size = struct.unpack('!i', read_n_bytes(read, 4))[0]
    # read message as json data
    message = json.loads(read_n_bytes(read, size))
    pass # logging.debug(f'read message: {message}')
    return message

def write_message(write, message):
    pass # logging.debug(f'write message: {message}')
    data = json.dumps(message).encode()
    write(struct.pack('!i', len(data)) + data)
