# IoT Sensor Monitoring with Kafka e MySQL

Este projeto simula sensores IoT enviando dados para um tópico Kafka, com um consumer que processa e armazena os dados em um banco MySQL. Tudo é orquestrado via Docker Compose.

## Serviços
- **producer**: Gera dados falsos e envia para o Kafka.
- **consumer**: Consome dados do Kafka e armazena no MySQL.
- **kafka & zookeeper**: Mensageria.
- **db**: MySQL para armazenamento dos dados.

## Como rodar

1. Suba todos os serviços:

```sh
docker-compose up --build
```

2. O producer e o consumer serão executados automaticamente.

3. Os dados serão salvos na tabela `sensor_data` do banco `iotdb`.

## Requisitos
- Docker e Docker Compose instalados.

## Observações
- O tópico Kafka usado é `iot_sensors`.
- O banco de dados é acessível em `db:3306` (interno) ou `localhost:3306` (externo).
- Usuário: `iotuser` | Senha: `iotpass` | Banco: `iotdb`
