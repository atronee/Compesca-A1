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
- Biblioteca grpc++ do C++
  - Para instalar a biblioteca grpc++ no Linux ou macOS, siga o tutorial em https://grpc.io/docs/languages/cpp/quickstart/
- Biblioteca _protobuf_ do C++
  - Instalar o grpc++ também instala o protobuf
- Bibliotecas grpcio e grpcio-tools do Python
  - Provemos um requirements.txt para instalar as bibliotecas necessárias. Para instalar, execute ```pip install -r requirements.txt```
  - Também disponibilizamos um pyproject.toml e poetry.lock para instalar as dependências com o Poetry. Para instalar, execute ```poetry install```

## Como usar

1. Clone o repositório do Data Plumber em sua máquina local: ```git clone https://github.com/atronee/Compesca-A1.git```
2. Instale as [dependências](#pré-requisitos) do projeto.
3. No diretório do projeto, compile o código fonte usando cmake:
    - ```mkdir cmake-build```
    - ```cd cmake-build```
    - ```cmake ..```
    - ```make```
4. create the file temposExec.txt in the build folder:
    - ```touch temposExec.txt```
5. Execute o programa gerado: ```./data_plumber```
6. Para executar o cliente gRPC, execute o comando 
        ```python -m grpc_tools.protoc -I./rpc --python_out=./rpc --pyi_out=./rpc --grpc_python_out=./rpc ./rpc/contract.proto```
    e depois execute ```python rpc/mock_client.py```.