import pygame
import socket
import time

from config import POOL_WIDTH, POOL_HEIGHT
from session import Session

STATUS_PANEL_WIDTH = 400
WIDTH, HEIGHT = POOL_WIDTH + STATUS_PANEL_WIDTH, POOL_HEIGHT

class Bubble:

    def __str__(self):
        return str({
            'id': self.id,
            'position': self.position,
            'radius': self.radius,
            'color': self.color,
            'locked': self.locked,
            'locked_by': self.locked_by,
            'locked_by_others': self.locked_by_others,
        })

    def __init__(self, config):
        self.id = config['id']
        self.position = config['position']
        self.radius = config['radius']
        self.color = config['color']
        self.locked = False
        self.locked_by_others = False
        self.locked_by = None

    def draw(self, screen):
        pygame.draw.circle(
            screen,
            self.color,
            self.position,
            self.radius)
        if self.locked:
            pygame.draw.circle(
                screen,
                'red' if self.locked_by_others else 'green',
                self.position,
                self.radius,
                2)
    
class BubblePanel:
    '''
    Manage bubbles
    '''
    def __init__(self, surface):
        self.bubbles = {}
        self.surface = surface

    def draw(self):
        '''
        draw all bubbles
        '''
        for b in self.bubbles.values():
            b.draw(self.surface)

def in_bubble(position, bubble):
    pos1 = position
    pos2 = bubble.position
    radius = bubble.radius
    return (pos1[0] - pos2[0]) ** 2 + (pos1[1] - pos2[1]) ** 2 <= radius ** 2

def centered(parent, target):
    x = (parent.get_width() - target.get_width()) / 2
    y = (parent.get_height() - target.get_height()) / 2
    return x, y


class StatusPanel:

    def __init__(self, client, surface):
        self.client = client
        self.surface = surface
        self.color = (50, 50, 50)
        self.font = pygame.font.Font(None, 30)

    def draw(self):
        self.surface.fill(self.color)
        position = (10, 10)
        for player_id, player_score in self.client.get_status():
            text = self.font.render(f'{player_id}: {player_score}', True, 'white')
            self.surface.blit(text, position)
            position = (position[0], position[1] + 30)
        delay_text = self.font.render(f'delay: {self.client.get_delay()}ms', True, 'white')
        self.surface.blit(delay_text, (10, self.surface.get_height() - 30))


class Client:
    def __init__(self, server_addr, screen):
        self.socket = socket.socket()
        self.socket.connect(server_addr)
        self.input_messages = []
        self.session = Session(self.socket, server_addr, self.handle_message)
        self.player_id = None
        self.player_score = 0
        self.players = {}
        self.delay = 0

        self.login()

        self.screen = screen
        self.bubble_panel = BubblePanel(self.screen.subsurface((0, 0, POOL_WIDTH, POOL_HEIGHT)))
        self.status_panel = StatusPanel(self, self.screen.subsurface((POOL_WIDTH, 0, STATUS_PANEL_WIDTH, HEIGHT)))

        self.sync_delay = 0

    def get_delay(self):
        return int(self.delay * 1000)

    def login(self):
        self.session.write_message({
            'action': 'login'
        })

    def handle_message(self, session, message):
        action = message.get('action', None)
        if action == 'ping':
            self.delay = time.time() - message['timestamp']
        elif action == 'login':
            self.player_id = message['player_id']
        elif action == 'bubble_added':
            bubble = Bubble(message)
            self.bubble_panel.bubbles[bubble.id] = bubble
        elif action == 'bubble_expired':
            bubble_id = message['bubble_id']
            if bubble_id in self.bubble_panel.bubbles:
                del self.bubble_panel.bubbles[message['bubble_id']]
        elif action == 'bubble_locked':
            for bubble_id in self.bubble_panel.bubbles:
                bubble = self.bubble_panel.bubbles[bubble_id]
                if bubble_id == message['bubble_id']:
                    bubble.locked = True
                    bubble.locked_by_others = message['player_id'] != self.player_id
                    bubble.locked_by = message['player_id']
                elif bubble.locked_by == message['player_id']:
                    bubble.locked = False
                    bubble.locked_by_others = False
                    bubble.locked_by = None
        elif action == 'bubble_consumed':
            bubble_id = message['bubble_id']
            del self.bubble_panel.bubbles[bubble_id]
        elif action == 'bubble_lock_failed':
            pass
        elif action == 'status':
            self.players = message['players']
        else:
            print('unknown message:', message)

    def write_message(self, message):
        self.session.write_message(message)

    def update(self, tick_in_ms):
        self.sync_delay += tick_in_ms
        if self.sync_delay >= 1000:
            self.sync_delay = 0
            self.write_message({
                'action': 'status'
            })
            self.write_message({
                'action': 'ping',
                'timestamp': time.time()
            })

    def draw(self):
        self.screen.fill('black')
        self.status_panel.draw()
        self.bubble_panel.draw()
    
    def get_bubble_at(self, pos):
        for b in self.bubble_panel.bubbles.values():
            if in_bubble(pos, b):
                return b
        return None
    
    def lock_bubble(self, bubble):
        print(f'lock bubble: {bubble.id}')
        self.write_message({
            'action': 'lock',
            'bubble_id': bubble.id,
            'player_id': self.player_id,
        })

    def get_status(self):
        status = []
        for player_id in self.players:
            status.append((player_id, self.players[player_id]['score']))
        return status

def main(server_address):
    pygame.init()

    pygame.display.set_mode((WIDTH, HEIGHT))
    pygame.display.set_caption("Client")

    screen = pygame.display.get_surface()
    client = Client(server_address, screen)

    FPS = 60
    clock = pygame.time.Clock()
    running = True

    while running:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False
            elif event.type == pygame.MOUSEBUTTONDOWN:
                position = pygame.mouse.get_pos()
                bubble = client.get_bubble_at(position)
                if bubble:
                    client.lock_bubble(bubble)

        tick_in_ms = clock.tick(FPS)
        client.update(tick_in_ms)
        client.draw()
        pygame.display.update()

    pygame.quit()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('server', nargs='?', default='localhost')
    parser.add_argument('--port', default=80, type=int)
    args = parser.parse_args()
    main((args.server, args.port))
