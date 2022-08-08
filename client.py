import pygame
import socket
import random

from common import BUBBLE_MAX_RADIUS, BUBBLE_MIN_RADIUS, POOL_WIDTH, POOL_HEIGHT
from common import Session

pygame.init()

STATUS_PANEL_WIDTH = 400
WIDTH, HEIGHT = POOL_WIDTH + STATUS_PANEL_WIDTH, POOL_HEIGHT

pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("Client")

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
    
class BubbleManager:
    '''
    Manage bubbles
    '''
    def __init__(self, surface):
        self.bubbles = {}
        self.surface = surface
        self.surface.fill('green')

    def draw(self):
        '''
        draw all bubbles
        '''
        for b in self.bubbles.values():
            b.draw(self.surface)
    
    def unlock_all(self):
        for b in self.bubbles.values():
            b.unlock()
    
    def lock(self, bubble):
        bubble.lock()

def in_bubble(position, bubble):
    pos1 = position
    pos2 = bubble.position
    radius = bubble.radius
    return (pos1[0] - pos2[0]) ** 2 + (pos1[1] - pos2[1]) ** 2 <= radius ** 2

def centered(parent, target):
    x = (parent.get_width() - target.get_width()) / 2
    y = (parent.get_height() - target.get_height()) / 2
    return x, y

class LoginPanel:

    def __init__(self):
        characters = list('abcdefghijklmnopqrstuvwxyz0123456789')
        random.shuffle(characters)
        self.player_name = ''.join(characters[0:random.randint(3, 10)])

    def update(self, tick):
        pass

    def draw(self, screen):
        #font = pygame.font.SysFont(fontname, font_height)
        font = pygame.font.Font(None, 30)
        text = font.render(f'login as {self.player_name}', True, 'white')
        screen.blit(text, centered(screen, text))

global_font = pygame.font.Font(None, 30)

class StatusPanel:

    def __init__(self, client, surface):
        self.client = client
        self.surface = surface
        self.color = (50, 50, 50)

    def draw(self):
        self.surface.fill(self.color)
        position = (10, 10)
        for player_id, player_score in self.client.get_status():
            text = global_font.render(f'{player_id}: {player_score}', True, 'white')
            self.surface.blit(text, position)
            position = (position[0], position[1] + 30)

class BubblePanel:

    def __init__(self, client, surface):
        self.client = client
        self.surface = surface

    def draw(self):
        self.surface.fill('yellow')

class Client:
    def __init__(self, server_addr, screen):
        self.socket = socket.socket()
        self.socket.connect(server_addr)
        self.input_messages = []
        self.session = Session(self.socket, server_addr, self.handle_message)
        self.logged_in = False
        self.player_id = None
        self.login_panel = LoginPanel()
        self.player_id = None
        self.player_score = 0
        self.players = {}

        self.login()

        self.screen = screen
        self.bubble_manager = BubbleManager(self.screen.subsurface((0, 0, POOL_WIDTH, POOL_HEIGHT)))
        self.status_panel = StatusPanel(self, self.screen.subsurface((POOL_WIDTH, 0, STATUS_PANEL_WIDTH, HEIGHT)))
        self.bubble_panel = BubblePanel(self, self.screen.subsurface((100, 0, WIDTH - 100, HEIGHT)))

        self.sync_delay = 0

    def login(self):
        self.session.write_message({
            'action': 'login'
        })

    def handle_message(self, session, message):
        if message.get('action') != 'status':
            print(message)
        match message.get('action', None):
            case 'login':
                self.player_id = message['player_id']
                self.logged_in = True
            case 'bubble_added':
                bubble = Bubble(message)
                self.bubble_manager.bubbles[bubble.id] = bubble
            case 'bubble_expired':
                bubble_id = message['bubble_id']
                if bubble_id in self.bubble_manager.bubbles:
                    del self.bubble_manager.bubbles[message['bubble_id']]
            case 'bubble_locked':
                for bubble_id in self.bubble_manager.bubbles:
                    bubble = self.bubble_manager.bubbles[bubble_id]
                    if bubble_id == message['bubble_id']:
                        bubble.locked = True
                        bubble.locked_by_others = message['player_id'] != self.player_id
                        bubble.locked_by = message['player_id']
                    elif bubble.locked_by == message['player_id']:
                        bubble.locked = False
                        bubble.locked_by_others = False
                        bubble.locked_by = None

            case 'bubble_unlocked':
                pass
            case 'bubble_consumed':
                bubble_id = message['bubble_id']
                del self.bubble_manager.bubbles[bubble_id]
            case 'bubble_lock_failed':
                pass
            case 'heartbeat':
                pass
            case 'status':
                self.players = message['players']
            case _:
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

    def draw(self):
        self.screen.fill('black')
        self.status_panel.draw()
        self.bubble_manager.draw()
        #self.bubble_panel.draw()
    
    def get_bubble_at(self, pos):
        for b in self.bubble_manager.bubbles.values():
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
    import sys
    if len(sys.argv) != 3:
        print('usage: python3 client.py server_ip server_port')
        sys.exit(1)
    main((sys.argv[1], int(sys.argv[2])))
