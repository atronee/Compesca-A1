# Data Plumber 👷‍♂️

O Data Plumber é um projeto em C++ que fornece um conjunto de funções para lidar com _pipelines_ de processamento de dados. Com várias funções disponíveis, é permitido ao usuário a execução de várias operações úteis para _data wrangling_. Foi desenvolvido como projeto de A1 da matéria de Computação Escalável da FGV, ministrada no primeiro semestre de 2024 pelo Prof. Dr. Thiago Araújo.

## Funcionalidades

Nosso projeto se baseia numa modelagem de _pipeline_ de processamento de dados com 5 abstrações:
1. Dataframe: estrutura de dados tabular que armazena os dados a serem processados.
2. Handler: função que processa os dados de um Dataframe.
3. Repository: estrutura de dados que armazena os Dataframes.
4. Trigger: função que dispara a execução de um Handler.
5. Queue: estrutura de dados que armazena os dataframes de entrada e saída de um Handler.

Para melhor distribuir os recursos computacionais, o Data Plumber utiliza a biblioteca _thread_ do C++ para paralelizar a execução dos Handlers. Dessa forma, é possível processar os dados de forma mais eficiente e rápida. Mais especificamente, as queues servem como filas consumidor-produtor, remontando ao problema clássico de computação paralela.

## Pré-requisitos

Certifique-se de ter os seguintes requisitos instalados em sua máquina:

- Compilador C++
- Biblioteca padrão do C++
- Sistema operacional compatível (Windows, macOS, Linux)
- Biblioteca _thread_ do C++
- Biblioteca _sqlite3_ do C++
  - Para instalar a biblioteca _sqlite3_ no Linux, execute o comando ```sudo apt install libsqlite3-dev```

## Como usar

1. Clone o repositório do Data Plumber em sua máquina local: ```git clone https://github.com/atronee/Compesca-A1.git```
2. No diretório do projeto, compile e execute o código-fonte: ```make main```.

Importante notar que neste repositório também estão disponíveis _drivers_ que testam as funções implementadas por nós. 