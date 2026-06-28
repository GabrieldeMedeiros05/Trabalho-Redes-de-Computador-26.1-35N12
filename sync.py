"""
sync.py
Responsável: Caio (Monitoramento e sincronização de arquivos)

Monitora a pasta compartilhada do nó, detectando criação, alteração
e remoção de arquivos. Também implementa a sincronização entre peers,
incluindo envio e recebimento de arquivos, propagação de remoções e
reconciliação periódica para manter as pastas consistentes.
"""

import base64
import hashlib
import os
import threading
import time

import protocol
import server


SCAN_INTERVAL = 2


def scan_shared_folder(shared_folder):
    """Monta um retrato atual da pasta compartilhada."""
    files = {}
    for root, _, filenames in os.walk(shared_folder):
        for filename in filenames:
            full_path = os.path.join(root, filename)
            rel_path = os.path.relpath(full_path, shared_folder).replace("\\", "/")
            files[rel_path] = build_metadata(full_path)
    return files


def build_metadata(full_path):
    """Metadados usados para comparar versões de um arquivo."""
    stat = os.stat(full_path)
    return {
        "size": stat.st_size,
        "mtime": stat.st_mtime,
        "sha256": calculate_sha256(full_path),
    }


def calculate_sha256(full_path):
    digest = hashlib.sha256()
    with open(full_path, "rb") as file:
        while True:
            chunk = file.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def safe_join(shared_folder, rel_path):
    """Garante que o arquivo recebido não saia da pasta compartilhada."""
    base = os.path.abspath(shared_folder)
    target = os.path.abspath(os.path.join(base, rel_path))
    if target != base and not target.startswith(base + os.sep):
        raise ValueError("Caminho fora da pasta compartilhada")
    return target


class FileSynchronizer:
    def __init__(self, node_id, shared_folder, peer_manager):
        self.node_id = node_id
        self.shared_folder = shared_folder
        self.peer_manager = peer_manager
        self._lock = threading.Lock()
        # Snapshot local usado para detectar criação, alteração e remoção.
        self._snapshot = scan_shared_folder(shared_folder)
        # Guarda remoções recentes para evitar que arquivos apagados "voltem".
        self._deleted_at = {}

    def start(self):
        self.start_monitor_loop()
        self.start_periodic_sync_loop()

    def start_monitor_loop(self, interval=SCAN_INTERVAL):
        thread = threading.Thread(
            target=self._monitor_loop,
            args=(interval,),
            daemon=True,
        )
        thread.start()
        return thread

    def start_periodic_sync_loop(self, interval=5):
        thread = threading.Thread(
            target=self._periodic_sync_loop,
            args=(interval,),
            daemon=True,
        )
        thread.start()
        return thread

    def _monitor_loop(self, interval):
        print(f"[sync] Monitorando pasta: {self.shared_folder}")
        while True:
            time.sleep(interval)
            current = scan_shared_folder(self.shared_folder)

            with self._lock:
                previous = self._snapshot
                # Se os metadados mudaram, tratamos como criação/alteração.
                changed = [
                    path
                    for path, metadata in current.items()
                    if previous.get(path) != metadata
                ]
                # Se existia antes e não existe agora, foi removido localmente.
                deleted = [path for path in previous if path not in current]
                self._snapshot = current

            for path in changed:
                print(f"[sync] Arquivo novo/alterado: {path}")
                self.broadcast_file(path)

            for path in deleted:
                print(f"[sync] Arquivo removido: {path}")
                deleted_at = time.time()
                with self._lock:
                    self._deleted_at[path] = deleted_at
                self.broadcast_delete(path, deleted_at)

    def _periodic_sync_loop(self, interval):
        """Compara periodicamente o estado local com o dos peers."""
        while True:
            time.sleep(interval)
            for peer in self.peer_manager.get_all_peers():
                try:
                    self.sync_with_peer(peer)
                except OSError as error:
                    print(f"[sync] Falha ao sincronizar com {peer['node_id']}: {error}")

    def sync_with_peer(self, peer):
        """Faz reconciliação com um peer: baixa, envia e propaga remoções."""
        response = server.send_message_to_peer(
            peer["ip"],
            peer["tcp_port"],
            protocol.build_file_list_request(self.node_id),
            wait_response=True,
        )

        if not response or response.get("type") != protocol.FILE_LIST_RESPONSE:
            return

        remote_files = response.get("files", {})
        remote_deleted = response.get("deleted", {})
        local_files = scan_shared_folder(self.shared_folder)

        self.apply_remote_deletions(remote_deleted, local_files)
        local_files = scan_shared_folder(self.shared_folder)

        for path, remote_metadata in remote_files.items():
            local_metadata = local_files.get(path)
            remote_metadata = dict(remote_metadata)
            remote_metadata["path"] = path
            if self._should_request_file(local_metadata, remote_metadata):
                print(f"[sync] Baixando {path} de {peer['node_id']}")
                self.request_file(peer, path)

        for path, local_metadata in local_files.items():
            remote_metadata = remote_files.get(path)
            remote_deleted_at = remote_deleted.get(path)
            if self._should_send_file(local_metadata, remote_metadata, remote_deleted_at):
                print(f"[sync] Enviando {path} para {peer['node_id']}")
                self.send_file_to_peer(peer, path)

        self.send_local_deletions_to_peer(peer, remote_deleted)

    def apply_remote_deletions(self, remote_deleted, local_files):
        """Aplica remoções conhecidas por outro peer."""
        for path, deleted_at in remote_deleted.items():
            local_metadata = local_files.get(path)
            with self._lock:
                current_deleted_at = self._deleted_at.get(path)
                if current_deleted_at is None or deleted_at > current_deleted_at:
                    self._deleted_at[path] = deleted_at

            if local_metadata is None:
                continue
            if local_metadata.get("mtime", 0) <= deleted_at:
                self.apply_delete({"path": path, "deleted_at": deleted_at})

    def _should_request_file(self, local_metadata, remote_metadata):
        path = remote_metadata.get("path")
        if path is not None:
            deleted_at = self._deleted_at.get(path)
            if deleted_at is not None and deleted_at >= remote_metadata.get("mtime", 0):
                return False
        if local_metadata is None:
            return True
        if local_metadata.get("sha256") == remote_metadata.get("sha256"):
            return False
        return remote_metadata.get("mtime", 0) >= local_metadata.get("mtime", 0)

    def _should_send_file(self, local_metadata, remote_metadata, remote_deleted_at=None):
        if remote_deleted_at is not None and remote_deleted_at >= local_metadata.get("mtime", 0):
            return False
        if remote_metadata is None:
            return True
        if local_metadata.get("sha256") == remote_metadata.get("sha256"):
            return False
        return local_metadata.get("mtime", 0) > remote_metadata.get("mtime", 0)

    def send_local_deletions_to_peer(self, peer, remote_deleted):
        with self._lock:
            local_deleted = dict(self._deleted_at)

        for path, deleted_at in local_deleted.items():
            if remote_deleted.get(path, 0) >= deleted_at:
                continue
            try:
                msg = protocol.build_file_delete(self.node_id, path, deleted_at)
                server.send_message_to_peer(peer["ip"], peer["tcp_port"], msg)
            except OSError as error:
                print(f"[sync] Falha ao avisar remocao para {peer['node_id']}: {error}")

    def request_file(self, peer, path):
        response = server.send_message_to_peer(
            peer["ip"],
            peer["tcp_port"],
            protocol.build_file_request(self.node_id, path),
            wait_response=True,
        )
        if response and response.get("type") == protocol.FILE_DATA:
            self.apply_file_data(response)

    def send_file_to_peer(self, peer, path):
        msg = self.build_file_data_message(path)
        server.send_message_to_peer(peer["ip"], peer["tcp_port"], msg)

    def broadcast_file(self, path):
        msg = self.build_file_data_message(path)
        for peer in self.peer_manager.get_all_peers():
            try:
                server.send_message_to_peer(peer["ip"], peer["tcp_port"], msg)
            except OSError as error:
                print(f"[sync] Falha ao enviar {path} para {peer['node_id']}: {error}")

    def broadcast_delete(self, path, deleted_at=None):
        if deleted_at is None:
            deleted_at = time.time()
        msg = protocol.build_file_delete(self.node_id, path, deleted_at)
        for peer in self.peer_manager.get_all_peers():
            try:
                server.send_message_to_peer(peer["ip"], peer["tcp_port"], msg)
            except OSError as error:
                print(f"[sync] Falha ao avisar remocao para {peer['node_id']}: {error}")

    def build_file_data_message(self, path):
        """Lê um arquivo local e monta a mensagem FILE_DATA."""
        full_path = safe_join(self.shared_folder, path)
        metadata = build_metadata(full_path)
        with open(full_path, "rb") as file:
            encoded = base64.b64encode(file.read()).decode("ascii")
        return protocol.build_file_data(self.node_id, path, encoded, metadata)

    def handle_message(self, msg):
        msg_type = msg.get("type")

        if msg_type == protocol.FILE_LIST_REQUEST:
            files = scan_shared_folder(self.shared_folder)
            with self._lock:
                deleted = dict(self._deleted_at)
            return protocol.build_file_list_response(self.node_id, files, deleted)

        if msg_type == protocol.FILE_REQUEST:
            try:
                return self.build_file_data_message(msg["path"])
            except (FileNotFoundError, ValueError) as error:
                return protocol.build_error(self.node_id, str(error))

        if msg_type == protocol.FILE_DATA:
            self.apply_file_data(msg)
            return None

        if msg_type == protocol.FILE_DELETE:
            self.apply_delete(msg)
            return None

        return None

    def apply_file_data(self, msg):
        """Recebe FILE_DATA, valida o hash e grava o arquivo."""
        path = msg["path"]
        metadata = msg.get("metadata", {})
        content = base64.b64decode(msg["content_base64"])

        if hashlib.sha256(content).hexdigest() != metadata.get("sha256"):
            print(f"[sync] Hash invalido para {path}; arquivo ignorado")
            return

        deleted_at = self._deleted_at.get(path)
        if deleted_at is not None and deleted_at > metadata.get("mtime", 0):
            print(f"[sync] Arquivo ignorado por remocao mais recente: {path}")
            return

        full_path = safe_join(self.shared_folder, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as file:
            file.write(content)

        if "mtime" in metadata:
            os.utime(full_path, (metadata["mtime"], metadata["mtime"]))

        with self._lock:
            self._snapshot[path] = build_metadata(full_path)
            self._deleted_at.pop(path, None)

        print(f"[sync] Arquivo sincronizado: {path}")

    def apply_delete(self, msg):
        """Recebe FILE_DELETE e remove a cópia local quando necessário."""
        path = msg["path"]
        deleted_at = msg.get("deleted_at", time.time())
        try:
            full_path = safe_join(self.shared_folder, path)
        except ValueError:
            return

        if os.path.exists(full_path):
            local_metadata = build_metadata(full_path)
            if local_metadata.get("mtime", 0) > deleted_at:
                print(f"[sync] Remocao ignorada; arquivo local e mais novo: {path}")
                return
            os.remove(full_path)

        with self._lock:
            self._snapshot.pop(path, None)
            self._deleted_at[path] = deleted_at

        print(f"[sync] Arquivo removido por sincronizacao: {path}")
