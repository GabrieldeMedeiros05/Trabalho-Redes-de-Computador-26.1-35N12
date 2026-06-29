import time
import threading
import socket

import protocol


class PeerManager:

    def __init__(self, peer_timeout=30):
        self._peers = {}  # node_id -> dict com info do peer
        self._lock = threading.Lock()
        self.peer_timeout = peer_timeout  # segundos sem responder = considerado offline

    def add_or_update_peer(self, node_id, ip, tcp_port):
        """
        Adiciona um peer novo ou atualiza o last_seen de um existente.
        Chamado quando recebemos HELLO, HELLO_ACK ou PONG de alguém.
        """
        with self._lock:
            self._peers[node_id] = {
                "node_id": node_id,
                "ip": ip,
                "tcp_port": tcp_port,
                "last_seen": time.time(),
            }

    def remove_peer(self, node_id):
        with self._lock:
            self._peers.pop(node_id, None)

    def get_all_peers(self):
        """Retorna uma lista (cópia) de todos os peers conhecidos atualmente."""
        with self._lock:
            return list(self._peers.values())

    def get_peer(self, node_id):
        with self._lock:
            return self._peers.get(node_id)

    def remove_stale_peers(self):
        now = time.time()
        removed = []
        with self._lock:
            for node_id, info in list(self._peers.items()):
                if now - info["last_seen"] > self.peer_timeout:
                    del self._peers[node_id]
                    removed.append(node_id)
        return removed

    def start_cleanup_loop(self, interval=5):
        def loop():
            while True:
                time.sleep(interval)
                removed = self.remove_stale_peers()
                for node_id in removed:
                    print(f"[peers] Peer {node_id} removido (timeout)")

        t = threading.Thread(target=loop, daemon=True)
        t.start()
        return t

    def start_heartbeat_loop(self, node_id, interval=10, timeout=3):
        def ping_one_peer(peer):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            try:
                sock.connect((peer["ip"], peer["tcp_port"]))
                ping_msg = protocol.build_ping(node_id)
                sock.sendall(protocol.frame_message(ping_msg))

                response = protocol.read_framed_message(sock)
                if response and response.get("type") == protocol.PONG:
                    # Peer respondeu -> está vivo, atualiza last_seen
                    self.add_or_update_peer(
                        peer["node_id"], peer["ip"], peer["tcp_port"]
                    )
            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                # Peer não respondeu - não removemos aqui na hora.
                # remove_stale_peers vai cuidar disso quando o
                # last_seen ficar velho demais (peer_timeout).
                print(f"[peers] PING sem resposta de {peer['node_id']}: {e}")
            finally:
                sock.close()

        def loop():
            while True:
                time.sleep(interval)
                for peer in self.get_all_peers():
                    # Cada PING roda em sua própria thread para não
                    # bloquear o heartbeat inteiro caso um peer esteja
                    # lento ou inacessível.
                    threading.Thread(
                        target=ping_one_peer, args=(peer,), daemon=True
                    ).start()

        t = threading.Thread(target=loop, daemon=True)
        t.start()
        return t
