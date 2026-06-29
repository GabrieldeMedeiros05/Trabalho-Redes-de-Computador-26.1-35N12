# Trabalho-Redes-de-Computador-26.1-35N12

Caio Rodrigues Lino Mesquita, 20/2014842
Gabriel de Medeiros Matos, 24/2001491
Gustavo Mourão Mena Barreto, 23/2026414
Helio de Oliveira Dias, 23/2009520


Sistema de sincronização distribuída de pastas, estilo OneDrive simplificado,
implementado em arquitetura peer-to-peer (P2P) para a disciplina CIC0124 -
Redes de Computadores (UnB).

Cada instância do programa é um nó independente, capaz de descobrir outros
nós na rede local, manter uma lista de peers ativos, monitorar sua própria
pasta compartilhada e sincronizar automaticamente a criação, alteração e
remoção de arquivos com os demais nós da rede.

## Requisitos

- Python 3.10 ou superior (sem dependências externas)

## Estrutura do projeto

| Arquivo | Responsabilidade |
|---|---|
| `node.py` | Ponto de entrada; inicializa todos os componentes do nó |
| `discovery.py` | Descoberta automática de peers via UDP broadcast |
| `peers.py` | Gerenciamento da lista de peers, heartbeat e detecção de falhas |
| `protocol.py` | Definição do protocolo de mensagens (JSON) e framing TCP |
| `server.py` | Servidor e cliente TCP (comunicação entre nós) |
| `sync.py` | Monitoramento da pasta compartilhada e sincronização de arquivos |
| `testTCP.py` | Testes isolados da camada de comunicação TCP |

## Como executar

Cada nó é executado como um processo independente. O formato do comando é:

```bash
python3 node.py <node_id> <tcp_port> [pasta_compartilhada]
```

- `node_id`: identificador textual do nó (ex: `node1`, `A`, etc.)
- `tcp_port`: porta TCP usada pelo nó para receber conexões de outros peers
- `pasta_compartilhada` (opcional): diretório monitorado e sincronizado pelo
  nó. Caso não seja informado, usa `./shared`. Se a pasta não existir, ela
  é criada automaticamente.

### Exemplo com 2 nós (mesma máquina)

Abra dois terminais na pasta do projeto.

**Terminal 1:**
```bash
python3 node.py node1 6001 ./shared1
```

**Terminal 2** (aguarde 1-2 segundos antes de rodar, para dar tempo da
descoberta funcionar corretamente):
```bash
python3 node.py node2 6002 ./shared2
```

Os dois nós devem se descobrir automaticamente via broadcast UDP. No
terminal de cada nó, deve aparecer uma mensagem confirmando a descoberta
do outro (`Novo peer descoberto` ou `HELLO_ACK recebido`).

### Exemplo com 3 ou mais nós

Basta repetir o processo, usando uma porta TCP e uma pasta diferentes
para cada novo nó:

```bash
python3 node.py node3 6003 ./shared3
```

Não há limite imposto pelo sistema para o número de nós simultâneos.

## Como testar a sincronização

Com dois ou mais nós em execução, abra um terminal adicional e crie,
edite ou remova arquivos dentro de uma das pastas compartilhadas:

```bash
echo "conteudo de teste" > shared1/teste.txt
```

Após alguns segundos, o arquivo deve aparecer automaticamente nas demais
pastas compartilhadas (`shared2`, `shared3`, etc.). O mesmo vale para
edições (a versão mais recente prevalece, com base no horário de
modificação do arquivo) e remoções.

Para confirmar:
```bash
cat shared2/teste.txt
```

## Como testar a tolerância a falhas

Com os nós já tendo se descoberto mutuamente, interrompa um deles com
`Ctrl+C`. Os nós restantes devem continuar funcionando normalmente entre
si — incluindo a sincronização de novos arquivos — sem depender do nó que
foi encerrado.

O mecanismo de heartbeat (PING/PONG, a cada 10 segundos) detecta a queda
do peer, e após um timeout de 30 segundos sem resposta, o peer é removido
da lista de nós ativos (mensagem `Peer removido (timeout)` no log).

> **Observação:** ao testar quedas propositais, aguarde a confirmação de
> descoberta mútua entre todos os nós envolvidos antes de interromper
> algum deles. Caso contrário, a falha pode não ser detectada corretamente
> entre o par específico que ainda não havia concluído a descoberta.

## Como testar a entrada tardia de um nó

Crie arquivos em uma pasta compartilhada **antes** de iniciar qualquer
nó, suba o primeiro nó apontando para essa pasta, e só depois suba um
segundo nó com uma pasta vazia. O nó que entrou depois deve recuperar
automaticamente todos os arquivos existentes, inclusive os que estiverem
dentro de subpastas.

## Testes da camada de comunicação (TCP)

O arquivo `testTCP.py` contém testes isolados do servidor TCP (envio de
PING/recebimento de PONG, comportamento de timeout em conexões para
portas inexistentes). Para executar:

```bash
python3 testTCP.py
```

O resultado esperado está documentado em `resultado.txt`.

## Limpando o ambiente entre testes

```bash
rm -rf shared1 shared2 shared3 __pycache__
```

## Encerrando um nó

Pressione `Ctrl+C` no terminal correspondente. O nó realiza o
encerramento de forma limpa, finalizando as threads em segundo plano.

