

import socket
import threading

import protocol

BUFFER_SIZE = 4096


# ---------------------------------------------------------------------------
# SERVIDOR (lado que recebe conexões)
# ---------------------------------------------------------------------------

def start_tcp_server(
    node_id,
    tcp_port,
    peer_manager,
    shared_folder,
    sync_manager=None,
    on_file_received=None,
):
    """
    Sobe o servidor TCP em uma thread separada. Chamado por node.py.

    shared_folder e on_file_received não são usados nesta versão reduzida
    (servem para as partes de transferência de arquivo, que ainda não
    existem aqui) - mantidos na assinatura só para não quebrar a chamada
    feita em node.py.
    """
    t = threading.Thread(
        target=_server_loop,
        args=(node_id, tcp_port, peer_manager, sync_manager),
        daemon=True,
    )
    t.start()
    return t


def _server_loop(node_id, tcp_port, peer_manager, sync_manager):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("", tcp_port))
    server_sock.listen(10)
    print(f"[server] Servidor TCP escutando na porta {tcp_port}")

    while True:
        conn, addr = server_sock.accept()
        client_thread = threading.Thread(
            target=_handle_connection,
            args=(conn, addr, node_id, sync_manager),
            daemon=True,
        )
        client_thread.start()


def _handle_connection(conn, addr, node_id, sync_manager):
    """
    Trata mensagens recebidas em uma conexão TCP. Nesta versão reduzida,
    só sabemos responder a PING (necessário para o heartbeat de peers.py).
    """
    try:
        while True:
            msg = protocol.read_framed_message(conn)
            if msg is None:
                break  # conexão encerrada pelo outro lado

            msg_type = msg.get("type")

            if msg_type == protocol.PING:
                _handle_ping(conn, msg, node_id)
            elif sync_manager is not None:
                response = sync_manager.handle_message(msg)
                if response is not None:
                    conn.sendall(protocol.frame_message(response))
            else:
                print(f"[server] Tipo de mensagem não tratado nesta versão: {msg_type}")

    except (ConnectionResetError, BrokenPipeError):
        print(f"[server] Conexão com {addr} encerrada abruptamente")
    finally:
        conn.close()


def _handle_ping(conn, msg, node_id):
    """Responde a um PING com PONG (heartbeat)."""
    pong = protocol.build_pong(node_id)
    conn.sendall(protocol.frame_message(pong))


# ---------------------------------------------------------------------------
# CLIENTE (lado que inicia conexões para mandar dados a um peer)
# ---------------------------------------------------------------------------

def send_message_to_peer(peer_ip, peer_tcp_port, msg: dict, wait_response=False):
    """
    Abre uma conexão TCP com um peer, manda uma mensagem e
    opcionalmente espera por uma resposta.

    Usado por peers.py (start_heartbeat_loop) para mandar PING.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((peer_ip, peer_tcp_port))
        sock.sendall(protocol.frame_message(msg))

        if wait_response:
            response = protocol.read_framed_message(sock)
            return response
        return None
    finally:
        sock.close()
