# WhatsApp Bridge

Projeto desenvolvido como desafio técnico com o objetivo de **ler mensagens diretamente do banco de dados interno do WhatsApp em um dispositivo Android**, processá-las através de um agente nativo e disponibilizá-las para um sistema externo.

A solução utiliza um **agente escrito em Go**, **Redis Streams** como camada de comunicação e uma **API em .NET** responsável pelo consumo, persistência e visualização dos eventos.

---

## Visão geral

O fluxo da aplicação é:

```text
WhatsApp
   │
   ▼
SQLite interno do Android
   │
   ▼
Agente nativo em Go
   │
   ▼
Redis Stream
   │
   ▼
API .NET
   │
   ├── Persistência SQLite
   │
   └── Dashboard Web
```

O agente é executado diretamente dentro do ambiente Android e monitora o banco de dados do WhatsApp.

Quando uma nova mensagem é identificada, ela é transformada em um evento e publicada em um **Redis Stream**.

A API externa escrita em **C# / .NET** consome esses eventos, persiste as mensagens e disponibiliza os dados através de endpoints HTTP e de uma dashboard simples.

---

# Arquitetura

A aplicação foi dividida em três partes principais:

```text
┌───────────────────────────────┐
│          Android              │
│                               │
│  WhatsApp                     │
│      │                        │
│      ▼                        │
│  SQLite                       │
│      │                        │
│      ▼                        │
│  Native Agent (Go)            │
└──────────────┬────────────────┘
               │
               │ Redis Stream
               ▼
┌───────────────────────────────┐
│            Redis              │
│                               │
│        agent:events           │
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│          API .NET             │
│                               │
│  RedisEventWorker             │
│        │                      │
│        ├── MessageStore       │
│        │                      │
│        └── Dashboard          │
└───────────────────────────────┘
```

---

# 1. Android / WhatsApp

Para acessar o banco interno do WhatsApp é necessário executar o ambiente Android com **acesso root**.

Durante o desenvolvimento foi utilizado um emulador Android através do **Android Studio**.

O WhatsApp mantém suas informações internamente utilizando **SQLite**.

Entre as tabelas utilizadas para leitura estão:

```text
message
chat
jid
```

### `message`

Contém os registros das mensagens.

### `chat`

Contém informações relacionadas às conversas.

### `jid`

Contém os identificadores utilizados pelo WhatsApp para representar usuários e conversas.

O agente consulta essas tabelas para montar as informações necessárias de cada mensagem.

---

# 2. Native Agent

O agente responsável por acessar diretamente o ambiente Android foi desenvolvido em **Go**.

Estrutura principal:

```text
native-agent/
│
├── bin/
│   └── native-agent
│
├── cmd/
│   └── agent/
│       └── main.go
│
├── internal/
│   ├── agent/
│   │   └── agent.go
│   │
│   ├── checkpoint/
│   │   └── store.go
│   │
│   ├── config/
│   │   └── config.go
│   │
│   ├── contacts/
│   │   └── writer.go
│   │
│   ├── logger/
│   │   └── logger.go
│   │
│   ├── native/
│   │   └── probe.go
│   │
│   ├── redisclient/
│   │   └── client.go
│   │
│   └── whatsapp/
│       └── store.go
│
├── go.mod
└── go.sum
```

### Responsabilidades

O agente é responsável por:

* acessar o banco SQLite do WhatsApp;
* identificar novas mensagens;
* consultar informações relacionadas ao chat e ao remetente;
* gerar eventos;
* publicar eventos no Redis;
* manter um checkpoint da última mensagem processada;
* enviar heartbeats;
* realizar operações nativas no Android, como adição de contatos.

---

# Checkpoint

O agente mantém um **checkpoint** com a última mensagem processada.

Exemplo:

```text
message_id = 150
```

Se o agente for encerrado e iniciado novamente, ele não precisa reler todas as mensagens do banco.

Ele continua a partir do último ponto conhecido:

```text
150
 ↓
151
152
153
```

Isso evita processamento duplicado e reduz consultas desnecessárias ao banco.

---

# Heartbeat

O agente também publica periodicamente um **heartbeat**.

O heartbeat funciona como um sinal indicando que o processo continua ativo.

Exemplo:

```text
agent.heartbeat
```

Isso permite que outros componentes saibam se o agente está funcionando corretamente.

---

# 3. Redis

O Redis funciona como uma camada intermediária entre o agente Android e a API externa.

Foi utilizado **Redis Streams**.

Stream principal:

```text
agent:events
```

Exemplo de evento:

```text
type             whatsapp.message
message_id       17
chat_jid         150358215118855@lid
text             Olá
from_me          0
message_type     0
created_at       2026-08-16T20:00:00Z
```

---

## Por que Redis Streams?

O agente e a API não precisam estar diretamente conectados.

O fluxo fica desacoplado:

```text
Agente
   │
   │ publica
   ▼
Redis
   │
   │ consome
   ▼
API
```

Se a API ficar temporariamente indisponível, o evento continua no Stream e poderá ser processado posteriormente.

---

# Consumer Group

A API utiliza um **Consumer Group** para consumir os eventos.

```text
Stream:   agent:events

Group:    dotnet-monitor

Consumer: api-1
```

O Consumer Group permite controlar quais eventos já foram entregues e processados.

---

# 4. API externa

A API foi desenvolvida utilizando:

```text
C#
.NET
ASP.NET Core
StackExchange.Redis
```

A aplicação possui um `HostedService` responsável pelo consumo dos eventos:

```text
RedisEventWorker
```

O Worker permanece executando enquanto a aplicação está ativa.

Seu fluxo simplificado é:

```text
Redis
  │
  ▼
RedisEventWorker
  │
  ▼
ProcessEntryAsync
  │
  ├── whatsapp.message
  │
  ▼
MessageStore
```

Quando um evento do tipo:

```text
whatsapp.message
```

é recebido, ele é convertido para um registro de mensagem.

Exemplo:

```text
MessageRecord
```

contendo informações como:

```text
RedisEventId
WhatsAppMessageId
ChatJid
SenderJid
Text
FromMe
MessageType
CreatedAt
PersistedAt
```

---

# Persistência

As mensagens recebidas pela API são persistidas localmente utilizando **SQLite**.

Isso permite que os dados continuem disponíveis mesmo quando o Redis estiver temporariamente offline.

Também existe deduplicação para evitar que o mesmo evento seja persistido múltiplas vezes.

---

# Dashboard

A aplicação possui uma dashboard simples para visualizar o funcionamento do sistema.

Ela permite visualizar:

* mensagens recebidas;
* eventos processados;
* status do Redis;
* latência da conexão;
* dados persistidos.

Principais endpoints utilizados:

```http
GET /api/dashboard
```

e:

```http
GET /api/messages?limit=100
```

A dashboard utiliza os dados persistidos para que mensagens já processadas continuem visíveis mesmo caso o Redis fique temporariamente indisponível.

---

# Adição de contatos

O projeto também possui suporte ao fluxo de adição de contatos no Android.

O objetivo é permitir que, ao identificar um número ainda não cadastrado, seja possível adicioná-lo aos contatos do dispositivo.

O fluxo é:

```text
Sistema externo
      │
      ▼
API
      │
      ▼
Agente
      │
      ▼
Android Contacts
```

---

# Tecnologias utilizadas

| Tecnologia          | Responsabilidade                                          |
| ------------------- | --------------------------------------------------------- |
| Android             | Ambiente onde o WhatsApp é executado                      |
| Android Studio      | Emulador utilizado durante desenvolvimento                |
| SQLite              | Banco utilizado pelo WhatsApp e persistência local da API |
| Go                  | Desenvolvimento do agente nativo                          |
| CGO                 | Integração do Go com ambiente nativo                      |
| Android NDK         | Compilação do agente para Android                         |
| Redis               | Comunicação entre agente e API                            |
| Redis Streams       | Transporte e armazenamento dos eventos                    |
| C#                  | Linguagem utilizada na API                                |
| .NET / ASP.NET Core | API HTTP e processamento dos eventos                      |
| StackExchange.Redis | Cliente Redis utilizado pela API                          |
| HTML / JavaScript   | Dashboard simples                                         |
| Docker              | Execução do Redis                                         |
| Docker Compose      | Gerenciamento dos serviços locais                         |

---

# Como executar

## Pré-requisitos

É necessário possuir:

```text
Git
Docker
Docker Compose
Go
.NET SDK
Android Studio
Android SDK
Android NDK
ADB
```

---

# 1. Clonar o projeto

```bash
git clone <url-do-repositorio>

cd teste-vaga-whatsapp
```

---

# 2. Iniciar o Redis

Na raiz do projeto:

```bash
docker compose up -d redis
```

Verifique se o container está funcionando:

```bash
docker compose ps
```

Também é possível testar diretamente:

```bash
docker compose exec redis redis-cli PING
```

Resposta esperada:

```text
PONG
```

---

# 3. Verificar o Redis Stream

Para visualizar os eventos:

```bash
docker compose exec redis redis-cli XRANGE agent:events - +
```

Para visualizar os eventos mais recentes:

```bash
docker compose exec redis redis-cli XREVRANGE agent:events + - COUNT 20
```

---

# 4. Executar a API .NET

Entre no diretório da API:

```bash
cd dotnet-api/WhatsAppBridge.Api
```

Execute:

```bash
dotnet run
```

Durante o desenvolvimento a API foi executada em:

```text
http://127.0.0.1:5080
```

Os logs devem indicar a inicialização do consumidor Redis:

```text
Redis event worker started.
Stream=agent:events
Group=dotnet-monitor
Consumer=api-1
```

---

# 5. Abrir a dashboard

Com a API executando, abra no navegador:

```text
http://127.0.0.1:5080
```

A dashboard começará a consultar os endpoints da API.

---

# 6. Preparar o Android

Inicialize o emulador pelo Android Studio.

Verifique a conexão:

```bash
adb devices
```

Em seguida:

```bash
adb root
```

Teste:

```bash
adb shell
```

---

# 7. Compilar o agente

O agente é compilado para o Android utilizando Go + CGO + Android NDK.

Exemplo:

```bash
GOOS=android \
GOARCH=amd64 \
CGO_ENABLED=1 \
CC=<caminho-do-ndk>/x86_64-linux-android24-clang \
go build -o bin/native-agent ./cmd/agent
```

O caminho do compilador depende da instalação local do Android NDK.

---

# 8. Enviar o agente para o Android

```bash
adb push bin/native-agent /data/local/tmp/native-agent
```

Dar permissão de execução:

```bash
adb shell chmod +x /data/local/tmp/native-agent
```

---

# 9. Executar o agente

No emulador Android, o endereço:

```text
10.0.2.2
```

é utilizado para acessar a máquina host.

Execute:

```bash
adb shell 'LOG_LEVEL=debug REDIS_ADDR=10.0.2.2:6379 /data/local/tmp/native-agent'
```

Os logs devem indicar algo semelhante a:

```text
native environment initialized
agent started
```

e posteriormente os heartbeats.

---

# Como testar

Uma forma simples de validar todo o fluxo é testar cada camada separadamente.

## Teste 1 — Redis

```bash
docker compose exec redis redis-cli PING
```

Resultado esperado:

```text
PONG
```

---

## Teste 2 — Criar evento manual

É possível publicar um evento diretamente no Redis:

```bash
docker compose exec redis redis-cli XADD agent:events '*' \
type whatsapp.message \
message_id test-1 \
chat_jid test@example \
sender_jid test@example \
text "Mensagem de teste" \
from_me 0 \
message_type 0 \
created_at "2026-08-16T20:00:00Z"
```

A API deverá consumir o evento.

Depois consulte:

```text
GET /api/messages?limit=100
```

A mensagem deverá aparecer na resposta e na dashboard.

---

## Teste 3 — Fluxo real do WhatsApp

Com:

```text
Redis executando
API executando
Emulador executando
Agente executando
```

envie uma mensagem para a conta do WhatsApp utilizada no emulador.

O fluxo esperado é:

```text
Mensagem chega no WhatsApp
        ↓
Registro aparece no SQLite
        ↓
Agente detecta nova mensagem
        ↓
Evento é publicado no Redis
        ↓
RedisEventWorker recebe
        ↓
Mensagem é persistida
        ↓
Dashboard exibe a mensagem
```

---

# Teste de tolerância a falhas

Também é possível validar o comportamento quando o Redis fica indisponível.

Pare o Redis:

```bash
docker compose stop redis
```

A API deverá informar a indisponibilidade sem perder as mensagens já persistidas.

Inicie novamente:

```bash
docker compose start redis
```

O Worker tentará se reconectar e continuará o processamento.

---

# Testes do agente Go

Dentro de:

```bash
cd native-agent
```

Execute:

```bash
go test ./...
```

Também é possível executar:

```bash
go vet ./...
```

Esses comandos ajudam a detectar erros de compilação, problemas básicos no código e possíveis inconsistências.

> Para builds Android com CGO, pode ser necessário configurar o compilador do Android NDK através da variável `CC`.

---

# Decisões de arquitetura

A aplicação foi dividida desta forma para manter responsabilidades separadas.

### Go

Responsável pelo acesso de baixo nível ao Android e ao banco interno do WhatsApp.

### Redis Streams

Responsável por desacoplar o agente da aplicação externa e transportar os eventos.

### .NET

Responsável pelas regras da aplicação externa, persistência, API HTTP e dashboard.

### SQLite

Utilizado para persistir os dados processados pela API.

Essa separação permite que cada componente evolua de forma independente.

---

# Tratamento de falhas

Alguns cenários considerados durante o desenvolvimento:

### Redis temporariamente offline

O Worker tenta restabelecer a conexão.

### API reiniciada

O Consumer Group permite continuar o processamento dos eventos.

### Agente reiniciado

O checkpoint permite continuar a leitura a partir da última mensagem processada.

### Dashboard sem Redis

As mensagens já persistidas continuam disponíveis através do SQLite.

---

# Considerações

O acesso ao banco interno do WhatsApp depende de permissões elevadas no Android e da estrutura interna utilizada pela versão instalada do aplicativo.

Por esse motivo, esta implementação foi criada para **ambiente de desenvolvimento e demonstração técnica utilizando um Android com acesso root**.

Mudanças na estrutura interna do banco do WhatsApp podem exigir ajustes nas consultas utilizadas pelo agente.

---

# Resumo

A arquitetura final pode ser resumida em:

```text
WhatsApp
    ↓
SQLite
    ↓
Go Native Agent
    ↓
Redis Stream
    ↓
.NET Worker
    ↓
SQLite
    ↓
API / Dashboard
```

O projeto demonstra conceitos como:

* integração com Android;
* acesso a SQLite;
* desenvolvimento nativo com Go;
* compilação cross-platform;
* comunicação orientada a eventos;
* Redis Streams;
* Consumer Groups;
* checkpoints;
* workers em background;
* tolerância a falhas;
* persistência;
* APIs REST;
* separação de responsabilidades.
