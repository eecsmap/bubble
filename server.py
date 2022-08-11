import socket
import threading
import time
import random
import logging
from collections import deque

from session import Session, SessionException
from config import (
    BUBBLE_MIN_LIFETIME_SEC, BUBBLE_MAX_LIFETIME_SEC,
    BUBBLE_MIN_VALUE, BUBBLE_MAX_VALUE,
    BUBBLE_MAX_RADIUS, BUBBLE_MIN_RADIUS,
    POOL_WIDTH, POOL_HEIGHT)

class BubbleManager:
    '''
    bubble manager to create, expire, and consume bubbles
    '''

    def __init__(self, server):
        self.is_active = False
        self._next_id = 0
        self.bubbles = {}
        self.server = server

    def next_id(self):
        result = self._next_id
        self._next_id += 1
        return result

    def create_new_bubble(self):
        id = self.next_id()
        position = random.randint(0, POOL_WIDTH), random.randint(0, POOL_HEIGHT)
        value = random.randint(BUBBLE_MIN_VALUE, BUBBLE_MAX_VALUE)
        radius = BUBBLE_MIN_RADIUS + ((value - BUBBLE_MIN_VALUE) / (BUBBLE_MAX_VALUE - BUBBLE_MIN_VALUE) * (BUBBLE_MAX_RADIUS - BUBBLE_MIN_RADIUS))
        expire_time_s = time.time() + random.randint(BUBBLE_MIN_LIFETIME_SEC, BUBBLE_MAX_LIFETIME_SEC)
        color = random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)
        hold_time_ms = random.randint(100, 2000)
        # DEFINE A BUBBLE
        bubble = {
            'id': id,
            'position': position,
            'radius': radius,
            'color': color,
            'expire_time_s': expire_time_s,
            'locked_by': None,
            'hold_time_ms': hold_time_ms,
            'lock_time:': None,
            'value': value,
        }
        self.bubbles[id] = bubble
        self.server.bubble_added(bubble)

    def create_bubble(self):
        while self.is_active:
            if self.server.has_sessions():
                self.create_new_bubble()
            time.sleep(random.randint(10, 20) / 10)

    def expire_bubble(self):
        while self.is_active:
            now = time.time()
            expired_bubbles = []
            for bubble_id in list(self.bubbles):
                if self.bubbles[bubble_id]['expire_time_s'] <= now:
                    expired_bubbles.append(bubble_id)
            for bubble_id in expired_bubbles:
                self.server.bubble_expired(bubble_id)
                del self.bubbles[bubble_id]
            time.sleep(0.1)

    def start(self):
        self.is_active = True
        self.create_bubble_thread = threading.Thread(target=self.create_bubble, args=(), daemon=True)
        self.create_bubble_thread.start()
        self.expire_bubble_thread = threading.Thread(target=self.expire_bubble, args=(), daemon=True)
        self.expire_bubble_thread.start()
        self.check_bubble_thread = threading.Thread(target=self.check_bubble, args=(), daemon=True)
        self.check_bubble_thread.start()

    def check_bubble(self):
        while self.is_active:
            now = time.time()
            for bubble_id in list(self.bubbles):
                bubble = self.bubbles.get(bubble_id)
                if not bubble:
                    continue
                if bubble['locked_by'] is None:
                    continue
                if bubble['lock_time'] is None:
                    continue
                if now - bubble['lock_time'] >= bubble['hold_time_ms'] / 1000:
                    player_id = bubble['locked_by']
                    del self.bubbles[bubble_id]
                    self.server.consume_bubble(player_id, bubble)
                    pass # logging.debug(f'player {player_id} consumed bubble {bubble_id}')

    def get_value(self, bubble_id):
        return self.bubbles[bubble_id]['value']

    def try_lock(self, bubble_id, player_id):
        if bubble_id not in self.bubbles:
            # the bubble is expired
            # or is an invalid bubble does exist
            pass # logging.debug(f'bubble {bubble_id} does not exist')
            return
        
        locked_by = self.bubbles[bubble_id]['locked_by']
        if locked_by:
            if locked_by != player_id:
                pass # logging.debug(f'bubble {bubble_id} is already locked by another player {locked_by}')
                self.server.lock_failed(player_id, bubble_id)
            else:
                pass # logging.debug(f'bubble {bubble_id} is already locked by same player {player_id}')
            return

        assert locked_by is None

        for id in list(self.bubbles):
            if self.bubbles[id]['locked_by'] == player_id:
                assert bubble_id != id
                self.bubbles[id]['locked_by'] = None
                # only one bubble can be locked by a player at a time
                # so no need to continue searching
                pass # logging.debug(f'release previously locked bubble {id}')
                # TODO: send unlock message to clients?
                break
        
        self.bubbles[bubble_id]['locked_by'] = player_id
        self.bubbles[bubble_id]['lock_time'] = time.time()
        pass # logging.debug(f'player {player_id} locks bubble {bubble_id}')
        self.server.lock_bubble(bubble_id, player_id)
        return True


class Server:

    def __init__(self, port):
        self.port = port
        self.listen_socket = socket.socket()
        self.listen_socket.bind(('0.0.0.0', port))
        self.listen_socket.listen()
        
        self.sessions = {}

        # start a thread to accept clients
        self.messages_from_clients = deque()
        self._accept_client_thread = threading.Thread(target=self._accept_client, args=(), daemon=True)
        self._accept_client_thread.start()

        self.bubble_manager = BubbleManager(self)
        self.bubble_manager.start()

        self._status_thread = threading.Thread(target=self._status, args=(), daemon=True)
        self._status_thread.start()

        # start a thread to handle client messages    
        self.players = {}
        self._handle_messages_thread = threading.Thread(target=self._handle_messages, args=())
        self._handle_messages_thread.start()
        self._handle_messages_thread.join()

    def _status(self):
        while True:
            print(f'#sessions: {len(self.sessions)}, #bubbles: {len(self.bubble_manager.bubbles)}, #messages: {len(self.messages_from_clients)}\r', end='')

    def has_sessions(self):
        return len(self.sessions) > 0

    def bubble_added(self, bubble):
        '''
        used by bubble manager to notify clients of new bubble
        '''
        message = {
            'action': 'bubble_added',
            **bubble,
        }
        self.broadcast(message)

    def bubble_expired(self, bubble_id):
        message = {
            'action': 'bubble_expired',
            'bubble_id': bubble_id,
        }
        self.broadcast(message)

    def remove_session(self, session):
        # do not throw exceptions here!
        pass # logging.debug(f'remove {session}')
        self.sessions.pop(session.remote_address, None)
        for player_id in list(self.players):
            if self.players[player_id]['session'] == session:
                pass # logging.debug(f'remove player {player_id}')
                self.players.pop(player_id, None)

    def write_message(self, session, messasge):
        try:
            session.write_message(messasge)
        except SessionException:
            self.remove_session(session)

    def _accept_client(self):
        while True:
            socket, client_address = self.listen_socket.accept()
            session = Session(socket, client_address,
                lambda session, message: self.messages_from_clients.append((session, message)))
            self.sessions[client_address] = session
            pass # logging.info(f'{client_address} connected')

    def lock_failed(self, player_id, bubble_id):
        self.write_message(self.players[player_id]['session'], {
            'action': 'bubble_lock_failed',
            'bubble_id': bubble_id,
        })

    def broadcast(self, message):
        for session in list(self.sessions.values()):
            self.write_message(session, message)

    def lock_bubble(self, bubble_id, player_id):
        # we do not need to broadcast the unlock message
        # for the previous bubble locked by the player
        message = {
            'action': 'bubble_locked',
            'bubble_id': bubble_id,
            'player_id': player_id,
        }
        self.broadcast(message)

    def consume_bubble(self, player_id, bubble):
        self.players[player_id]['score'] += bubble['value']
        message = {
            'action': 'bubble_consumed',
            'bubble_id': bubble['id'],
            'player_id': player_id,
        }
        self.broadcast(message)

    def try_lock(self, bubble_id, player_id):
        pass # logging.debug(f'{player_id} try_lock {bubble_id}')
        self.bubble_manager.try_lock(bubble_id, player_id)

    def create_player(self, session):
        return ':'.join(map(str, session.remote_address))

    def _handle_message(self, session, message):
        action = message.get('action', None)
        if action == 'ping':
            self.write_message(session, message)
        elif action == 'login':
            player_id = self.create_player(session)
            if player_id in self.players:
                old_session = self.players[player_id]['session']
                old_session.close()
                for client_address in list(self.sessions):
                    if self.sessions[client_address] == old_session:
                        del self.sessions[client_address]
                        break
            self.players[player_id] = {}
            self.players[player_id]['session'] = session
            self.players[player_id]['score'] = 0
            message = {
                'action': 'login',
                'player_id': player_id,
            }
            self.write_message(session, message)
        elif action == 'lock':
            bubble_id = message['bubble_id']
            player_id = message['player_id']
            self.try_lock(bubble_id, player_id)
        elif action == 'status':
            message = {
                'action': 'status',
                'players': {},
            }
            for player_id in list(self.players):
                message['players'][player_id] = {}
                message['players'][player_id]['score'] = self.players[player_id]['score']
            self.write_message(session, message)

    def _handle_messages(self):
        while True:
            while self.messages_from_clients:
                session, message = self.messages_from_clients.popleft()
                self._handle_message(session, message)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    Server(80)
