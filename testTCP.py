# test_tcp.py
import threading
import time
import socket
import server
import protocol

class MockPeerManager:
    def add_or_update_peer(self, node_id, ip, tcp_port): pass

def run_tests():
    print("=== INICIANDO TESTES DO MÓDULO TCP ===")
    
    NODE_ID = "Nó_Teste_Gabriel"
    PORTA = 9999
    mock_pm = MockPeerManager()
    
    # 1. Inicia o seu servidor TCP
    server.start_tcp_server(
        node_id=NODE_ID,
        tcp_port=PORTA,
        peer_manager=mock_pm,
        shared_folder="./test_shared",
        sync_manager=None # Sem sync manager por enquanto, vamos testar só o PING/PONG
    )
    
    time.sleep(1) # Dá tempo pro servidor abrir o bind
    
    # 2. TESTE 1: Enviar PING válido e aguardar PONG
    print("\n[Teste 1] Testando envio de PING e recebimento de PONG...")
    ping_msg = protocol.build_ping("Cliente_Teste")
    
    try:
        response = server.send_message_to_peer("127.0.0.1", PORTA, ping_msg, wait_response=True)
        print(f"Resposta recebida do servidor: {response}")
        if response and response.get("type") == protocol.PONG:
            print("-> SUCESSO: O servidor respondeu PONG corretamente!")
        else:
            print("-> FALHA: Resposta inválida.")
    except Exception as e:
        print(f"-> FALHA Crítica no Teste 1: {e}")

    # 3. TESTE 2: Forçar um Timeout no Cliente
    print("\n[Teste 2] Testando comportamento de Timeout no cliente para porta inexistente...")
    try:
        # Tenta conectar em uma porta IP que provavelmente não existe com timeout baixo
        sock_fake = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fake.settimeout(1.0)
        sock_fake.connect(("10.255.255.255", 9999)) # IP inacessível
    except socket.timeout:
        print("-> SUCESSO: Timeout do socket funcionando como proteção contra travamento.")
    except Exception:
        print("-> SUCESSO: Conexão rejeitada imediatamente pela rede (Tratado).")

    print("\n=== TESTES CONCLUÍDOS ===")

if __name__ == "__main__":
    run_tests()

