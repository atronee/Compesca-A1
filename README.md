# Tree Explorer 🌳

O Tree Explorer é um projeto em C++ que fornece um conjunto de funções para lidar com árvores binárias de busca. Ele oferece um menu interativo para o usuário, permitindo a execução de várias operações úteis para BSTs. Foi desenvolvido como projeto final da matéria de Estrutura de Dados da FGV, ministrada no primeiro semestre de 2023 pelo Prof. Dr. Rafael de Pinho.

## Funcionalidades

1. O programa oferece um menu ASCII para o usuário com as opções de uso.
2. Todas as operações indicam o tempo de processamento utilizado após a finalização.
3. Construção de uma árvore binária de busca a partir de um arquivo texto.
4. Construção de uma árvore binária de busca a partir de dados digitados pelo usuário.
5. Informação da altura da árvore.
6. Informação do tamanho da árvore.
7. Inserção de um elemento fornecido pelo usuário.
8. Remoção de um elemento fornecido pelo usuário.
9. Busca do endereço de memória de um elemento fornecido pelo usuário.
10. Verificação se a árvore é completa.
11. Verificação se a árvore é perfeita.
12. Exibição da árvore utilizando BFS (Breadth-First Search) - utiliza uma implementação eficiente com fila.
13. Conversão da árvore em uma lista e ordenação com Bubble Sort.
14. Conversão da árvore em uma lista e ordenação com Selection Sort.
15. Conversão da árvore em uma lista e ordenação com Insertion Sort.
16. Conversão da árvore em uma lista e ordenação com Shell Sort.
17. Impressão da árvore de acordo com várias ordens de travessia (pré-ordem, in-ordem ou pós-ordem).

## Pré-requisitos

Certifique-se de ter os seguintes requisitos instalados em sua máquina:

- Compilador C++
- Biblioteca padrão do C++
- Sistema operacional compatível (Windows, macOS, Linux)

## Como usar

1. Clone o repositório do Tree Explorer em sua máquina local: ```git clone https://github.com/seu-usuario/tree-explorer.git```
2. No diretório do projeto, compile e execute o código-fonte: ```make main```, ou ```g++ -o main.exe main.cpp interface.cpp create_tree.cpp tree_infos.cpp BFS.cpp sort_list.cpp``` seguido de ```./main.exe```
3. Siga as instruções exibidas no menu para interagir com as funcionalidades do Tree Explorer.

Importante notar que neste repositório também estão disponíveis _drivers_ que testam as funções implementadas por nós. Para tanto, você pode executar comandos como ```make test_create_tree```, ```make test_tree_infos``` ou ```make test_sort_list```. Também pode ser executado na linha de comando ```g++ test_{NOME}.cpp {NOME}.cpp -o test_{NOME}.exe``` seguido de ```./test_{NOME}.exe```, substituindo ```{NOME}``` por ```create_tree```, ```tree_infos``` ou ```sort_list```.
