# IoT Sensor Monitoring with Kafka e MongoDB

Este projeto simula sensores IoT enviando dados para um tópico Kafka, com um consumer que processa e armazena os dados em um banco MongoDB. Tudo é orquestrado via Docker Compose.

## Serviços
- **sensor-producer**: Gera dados falsos e envia para o Kafka.
- **sensor-consumer**: Consome dados do Kafka e armazena no MongoDB.
- **kafka & zookeeper**: Mensageria.
- **mongodb**: MongoDB para armazenamento dos dados.

## Como rodar

1. Suba todos os serviços:

```sh
docker-compose up --build
```

2. O producer e o consumer serão executados automaticamente.

3. Os dados serão salvos na coleção `sensor_data` do banco `iotdb` no MongoDB.

## Como rodar os testes

1. Com os serviços rodando, acesse o container do producer ou consumer:

```sh
docker-compose exec sensor-producer bash
# ou
# docker-compose exec sensor-consumer bash
```

2. Execute os testes (ajuste o caminho conforme sua estrutura):

```sh
pytest
```

Se os testes estiverem em uma pasta específica, por exemplo `tests/`, use:

```sh
pytest tests/
```

## Requisitos
- Docker e Docker Compose instalados.

## Observações
- O tópico Kafka usado é `iot_sensors`.
- O banco de dados é acessível em `mongodb:27017` (interno) ou `localhost:27017` (externo).
- Banco: `iotdb` | Coleção: `sensor_data`
