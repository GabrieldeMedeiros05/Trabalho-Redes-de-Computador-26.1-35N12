"""
discovery.py
Responsável: Gustavo (Descoberta e gerenciamento de peers)

Implementa a descoberta de nós na rede local via UDP broadcast.
Quando um nó sobe, ele anuncia sua presença (HELLO) e escuta por
anúncios de outros nós, além de responder com HELLO_ACK.

Depende de: protocol.py (mensagens), peers.py (armazenar quem foi encontrado)
"""

import socket
import threading

import protocol

DISCOVERY_PORT = 5000
BUFFER_SIZE = 4096


def send_hello_broadcast(node_id, tcp_port, discovery_port=DISCOVERY_PORT):
    """
    Envia um broadcast UDP anunciando que este nó está online.
    Chamado uma vez quando o nó sobe (em node.py).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    msg = protocol.build_hello(node_id, tcp_port)
    payload = protocol.encode_message(msg)
    sock.sendto(payload, ("<broadcast>", discovery_port))
    sock.close()
    print(f"[discovery] HELLO enviado (broadcast) na porta {discovery_port}")


def listen_for_discovery(node_id, tcp_port, peer_manager, discovery_port=DISCOVERY_PORT):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", discovery_port))

    print(f"[discovery] Escutando broadcasts na porta {discovery_port}...")

    while True:
        raw, addr = sock.recvfrom(BUFFER_SIZE)
        ip_origem = addr[0]
        try:
            msg = protocol.decode_message(raw)
        except Exception as e:
            print(f"[discovery] Mensagem inválida recebida: {e}")
            continue
        msg_type = msg.get("type")

        if msg_type == protocol.HELLO:
            sender_id = msg["node_id"]
            if sender_id == node_id:
                continue  # ignora o próprio broadcast

            peer_manager.add_or_update_peer(sender_id, ip_origem, msg["tcp_port"])
            print(f"[discovery] Novo peer descoberto: {sender_id} ({ip_origem}:{msg['tcp_port']})")

            # Responde via unicast para quem mandou o HELLO
            ack = protocol.build_hello_ack(node_id, tcp_port)
            ack_payload = protocol.encode_message(ack)
            sock.sendto(ack_payload, (ip_origem, addr[1]))

        elif msg_type == protocol.HELLO_ACK:
            sender_id = msg["node_id"]
            if sender_id == node_id:
                continue
            peer_manager.add_or_update_peer(sender_id, ip_origem, msg["tcp_port"])
            print(f"[discovery] HELLO_ACK recebido de {sender_id}")

        # PONG normalmente chega por TCP (ver server.py), não por aqui.
        # Mas se decidirem fazer heartbeat por UDP também, tratar aqui.


def start_discovery(node_id, tcp_port, peer_manager, discovery_port=DISCOVERY_PORT):
    """
    Função de conveniência chamada por node.py: sobe a thread de escuta
    e manda o primeiro HELLO.
    """
    t = threading.Thread(
        target=listen_for_discovery,
        args=(node_id, tcp_port, peer_manager, discovery_port),
        daemon=True,
    )
    t.start()
    send_hello_broadcast(node_id, tcp_port, discovery_port)
    return t
