
import sys
import time
import os

import discovery
import server
from peers import PeerManager
from sync import FileSynchronizer


def main():
    if len(sys.argv) < 3:
        print("Uso: python node.py <node_id> <tcp_port> [pasta_compartilhada]")
        sys.exit(1)

    node_id = sys.argv[1]
    tcp_port = int(sys.argv[2])
    shared_folder = sys.argv[3] if len(sys.argv) > 3 else "./shared"

    if not os.path.exists(shared_folder):
        os.makedirs(shared_folder)

    print(f"=== Iniciando nó {node_id} na porta {tcp_port}, pasta: {shared_folder} ===")

    # 1. Gerenciador de peers (Pessoa 1)
    peer_manager = PeerManager(peer_timeout=30)
    peer_manager.start_cleanup_loop(interval=5)
    peer_manager.start_heartbeat_loop(node_id=node_id, interval=10)
    sync_manager = FileSynchronizer(
        node_id=node_id,
        shared_folder=shared_folder,
        peer_manager=peer_manager,
    )

    # 2. Sobe o servidor TCP (necessário para responder PING/PONG)
    server.start_tcp_server(
        node_id=node_id,
        tcp_port=tcp_port,
        peer_manager=peer_manager,
        shared_folder=shared_folder,
        sync_manager=sync_manager,
    )

    # 3. Sobe a descoberta de peers via UDP (Pessoa 1)
    discovery.start_discovery(
        node_id=node_id,
        tcp_port=tcp_port,
        peer_manager=peer_manager,
    )
    sync_manager.start()

    print(f"[node] Nó {node_id} pronto (descoberta + heartbeat + sincronizacao).")

    # Mantém o processo principal vivo (todo o trabalho roda em threads)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[node] Encerrando nó {node_id}...")


if __name__ == "__main__":
    main()
