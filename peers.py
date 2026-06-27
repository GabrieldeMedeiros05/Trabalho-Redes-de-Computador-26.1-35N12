"""
peers.py
Responsável: Gustavo (Descoberta e gerenciamento de peers)

Mantém a lista de nós conhecidos na rede, junto com informações
de "última vez visto" (para detectar quedas via heartbeat/PING).

Este módulo é consultado por:
- discovery.py (adiciona peers quando descobertos)
- server.py (consulta peers para saber para quem enviar FILE_DATA)
- sync.py (idem)
"""

import time
import threading
import socket

import protocol


class PeerManager:
    """
    Estrutura de um peer (dict):
    {
        "node_id": str,
        "ip": str,
        "tcp_port": int,
        "last_seen": float (timestamp),
    }
    """

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
        """
        Remove peers cujo last_seen não é atualizado há mais de
        self.peer_timeout segundos (ou seja, pararam de responder
        ao heartbeat - ver start_heartbeat_loop). Chamado periodicamente
        por start_cleanup_loop. Retorna lista de node_ids removidos,
        útil para logging/relatório.
        """
        now = time.time()
        removed = []
        with self._lock:
            for node_id, info in list(self._peers.items()):
                if now - info["last_seen"] > self.peer_timeout:
                    del self._peers[node_id]
                    removed.append(node_id)
        return removed

    def start_cleanup_loop(self, interval=5):
        """
        Roda em thread separada (daemon=True), chamada a partir do node.py.
        Faz a limpeza periódica de peers mortos (baseado no last_seen,
        que é atualizado pelo heartbeat em start_heartbeat_loop).
        """
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
        """
        Heartbeat ativo: a cada `interval` segundos, manda um PING via TCP
        para cada peer conhecido. Se o PONG voltar, atualiza o last_seen
        (mantém o peer "vivo"). Se a conexão falhar ou der timeout, não faz
        nada aqui - quem efetivamente remove o peer é remove_stale_peers,
        baseado no last_seen que parou de ser atualizado.

        Import de protocol é feito aqui dentro (não no topo do módulo)
        para evitar qualquer risco de import circular com server.py.
        """
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
