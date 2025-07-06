# Projeto Code Elevate - Assignment 1

Este projeto tem como objetivo realizar o processamento de dados de transportes utilizando PySpark, seguindo as camadas de bronze, silver e gold, conforme boas práticas de engenharia de dados.

## Como Executar com Docker

1. Certifique-se de ter o Docker e o Docker Compose instalados.
2. No diretório `assignment-1`, execute o comando abaixo para construir e iniciar os serviços (Jupyter Notebook e processamento PySpark):

```bash
docker-compose up --build
```

3. O Jupyter Notebook estará disponível em [http://localhost:8888](http://localhost:8888).
4. Para acompanhar os logs do processamento da aplicação, utilize:

```bash
docker logs assignment-1-script-1
```

5. Para parar e desmontar os containers e liberar recursos, utilize:

```bash
docker-compose down
```

6. Para remover as imagens criadas (opcional):

```bash
docker image prune -f
```

---

## Estrutura do Projeto

```
assignment-1/
├── code_elevate_app/
│   ├── __init__.py
│   ├── main.py
│   └── utils/
│       ├── aggregator.py
│       ├── logger.py
│       ├── processing.py
│       └── reader.py
├── table_destinations/
├── tests/
│   ├── test_aggregator.py
│   ├── test_processing.py
│   └── test_reader.py
├── info_transportes.csv
└── notebook-final-manipulacao.ipynb
```

## Descrição das Camadas

**Bronze:** Ingestão dos dados brutos do CSV e persistência em formato Parquet.
**Silver:** Transformações e limpezas nos dados, gerando uma tabela intermediária.
**Gold:** Agregações e geração de indicadores finais para análise.

## Como Executar Manualmente

1. Instale as dependências necessárias (PySpark).
2. Execute o comando abaixo, informando o caminho do arquivo CSV de entrada:

```bash
python -m code_elevate_app.main <caminho_para_csv>
```

Exemplo:
```bash
python -m code_elevate_app.main info_transportes.csv
```

## Requisitos
Python 3.8+
PySpark

## Como Executar os Testes

Você pode rodar os testes unitários de duas formas:

### 1. Usando Docker (recomendado)

1. Certifique-se de que a imagem já foi construída:

```bash
docker-compose build
```

2. Execute os testes dentro do container:

```bash
docker-compose run script python -m unittest discover -s tests

```

### 2. Localmente (fora do Docker)

1. Instale as dependências do projeto:

```bash
pip install -r requirements.txt
```

2. Execute os testes:

```bash
python -m unittest discover -s tests
```

Os testes estão localizados na pasta `tests/` e cobrem as principais funcionalidades das camadas bronze, silver e gold.

## Observações
As tabelas intermediárias e finais são salvas na pasta `table_destinations/`.
O log do processamento é exibido no console.

## Autor
Projeto desenvolvido para fins de avaliação técnica em Engenharia de Dados.
