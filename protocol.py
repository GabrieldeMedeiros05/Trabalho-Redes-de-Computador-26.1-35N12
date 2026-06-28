

import json

# ---------------------------------------------------------------------------
# Tipos de mensagem (constantes para evitar erro de digitação)
# ---------------------------------------------------------------------------
HELLO = "HELLO"                # Anúncio de novo nó (UDP broadcast)
HELLO_ACK = "HELLO_ACK"        # Resposta ao HELLO (UDP, unicast de volta)
PING = "PING"                  # Heartbeat para verificar se peer está vivo
PONG = "PONG"                  # Resposta ao PING
FILE_LIST_REQUEST = "FILE_LIST_REQUEST"
FILE_LIST_RESPONSE = "FILE_LIST_RESPONSE"
FILE_REQUEST = "FILE_REQUEST"
FILE_DATA = "FILE_DATA"
FILE_DELETE = "FILE_DELETE"
ERROR = "ERROR"


# ---------------------------------------------------------------------------
# Funções de construção de mensagens
# ---------------------------------------------------------------------------

def build_hello(node_id, tcp_port):
    """Mensagem enviada via UDP broadcast quando um nó entra na rede."""
    return {
        "type": HELLO,
        "node_id": node_id,
        "tcp_port": tcp_port,
    }


def build_hello_ack(node_id, tcp_port):
    """Resposta unicast ao HELLO, confirmando que este nó também existe."""
    return {
        "type": HELLO_ACK,
        "node_id": node_id,
        "tcp_port": tcp_port,
    }


def build_ping(node_id):
    return {"type": PING, "node_id": node_id}


def build_pong(node_id):
    return {"type": PONG, "node_id": node_id}


def build_file_list_request(node_id):
    return {"type": FILE_LIST_REQUEST, "node_id": node_id}


def build_file_list_response(node_id, files, deleted=None):
    return {
        "type": FILE_LIST_RESPONSE,
        "node_id": node_id,
        "files": files,
        "deleted": deleted or {},
    }


def build_file_request(node_id, path):
    return {"type": FILE_REQUEST, "node_id": node_id, "path": path}


def build_file_data(node_id, path, content_base64, metadata):
    return {
        "type": FILE_DATA,
        "node_id": node_id,
        "path": path,
        "content_base64": content_base64,
        "metadata": metadata,
    }


def build_file_delete(node_id, path, deleted_at=None):
    msg = {"type": FILE_DELETE, "node_id": node_id, "path": path}
    if deleted_at is not None:
        msg["deleted_at"] = deleted_at
    return msg


def build_error(node_id, message):
    return {"type": ERROR, "node_id": node_id, "message": message}


# ---------------------------------------------------------------------------
# Serialização / desserialização
# ---------------------------------------------------------------------------

def encode_message(msg: dict) -> bytes:
    """Converte dict -> bytes JSON, prontos para enviar pela rede."""
    return json.dumps(msg).encode("utf-8")


def decode_message(raw: bytes) -> dict:
    """Converte bytes JSON recebidos da rede -> dict Python."""
    return json.loads(raw.decode("utf-8"))


# ---------------------------------------------------------------------------
# Framing para TCP (TCP é um stream, não preserva limites de mensagem!)
# Usado pelo heartbeat (peers.py) e pelo servidor TCP (server.py) para
# saber onde uma mensagem termina e a próxima começa.
# Formato: 4 bytes de tamanho (big-endian) + payload JSON.
# ---------------------------------------------------------------------------

def frame_message(msg: dict) -> bytes:
    """Adiciona um cabeçalho de tamanho antes do JSON, para uso em sockets TCP."""
    payload = encode_message(msg)
    length = len(payload).to_bytes(4, byteorder="big")
    return length + payload


def read_framed_message(sock) -> dict:
    """Lê uma mensagem completa de um socket TCP usando o framing acima."""
    raw_len = _recv_exact(sock, 4)
    if raw_len is None:
        return None
    msg_len = int.from_bytes(raw_len, byteorder="big")
    raw_payload = _recv_exact(sock, msg_len)
    if raw_payload is None:
        return None
    return decode_message(raw_payload)


def _recv_exact(sock, n):
    """Helper: lê exatamente n bytes do socket (ou None se conexão cair)."""
    data = b""
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data
