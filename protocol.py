import json
import struct

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
    return json.loads(read_n_bytes(read, size))

def write_message(write, message):
    data = json.dumps(message).encode()
    write(struct.pack('!i', len(data)) + data)
