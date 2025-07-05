# Projeto Code Elevate - Assignment 1

Este projeto tem como objetivo realizar o processamento de dados de transportes utilizando PySpark, seguindo as camadas de bronze, silver e gold, conforme boas práticas de engenharia de dados.

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

## Como Executar

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

## Testes
Os testes unitários estão localizados na pasta `tests/`.

Para executar todos os testes, utilize o comando abaixo no terminal, dentro da pasta `assignment-1`:

```bash
python -m unittest discover -s tests
```

Certifique-se de que as dependências estejam instaladas e que o PySpark esteja configurado corretamente no ambiente.

## Observações
As tabelas intermediárias e finais são salvas na pasta `table_destinations/`.
O log do processamento é exibido no console.

## Autor
Projeto desenvolvido para fins de avaliação técnica em Engenharia de Dados.
