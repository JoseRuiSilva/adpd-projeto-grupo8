# Trabalho Prático Grupo8 ADPD 2025

Aqui encontra-se o nosso projeto para a cadeira de Ambientes Distribuídos. Criámos um sistema que liga uma máquina na Google Cloud, baixa dados de transações, faz os cálculos todos usando o DuckDB e guarda o resultado final num Bucket.
A ideia foi fazer tudo automático para que, após a configuração inicial, seja apenas necessário correr um comando e o sistema faz o trabalho todo e desliga-se no fim. Deves começar por te conectares a este repositório através da tua máquina.

## Requerimentos

Antes de começar, é necessário ter isto no PC:
- Vagrant;
- Git;
- Docker;

## 0. Baixar este projeto

A primeira coisa a fazer é trazer estes ficheiros para o teu computador.
- Abre o terminal ou linha de comandos numa pasta onde queiras guardar este repositório.
- Corre o comando:

      git clone https://github.com/JoseRuiSilva/adpd-projeto-grupo8.git

Agora podes entrar na pasta e iniciar.

## 1. Preparar a Google Cloud

Para que o script funcione, é necessário configurar as credenciais:
- Criar um Projeto: Vai à consola da GCP e usa um projeto que ja tenhas criado ou cria um novo. Anota o Project ID desse projeto.
- Verifica se as seguintes API's estão ligadas:
        - Compute Engine API;
        - Cloud Resource Manager API;
        - IAM (Identity and Access Management) API;
- Criar um Bucket: No menu "Cloud Storage", cria um bucket único (para facilitar, cria com o nome adpd-dados-2025-novo) onde os dados serão gravados.
- Criar Service Account: Cria uma nova Service Account e dá-lhe permissões de Compute Admin v1 e Editor.
- Baixar chave JSON: nessa Service Account, vai a "Keys">"Add Key">"Create new key" (JSON), renomeia a chave transferida para gcp-key.json e coloca nesta pasta do git hub na tua máquina.

## 2. Configuração de Segurança SSH

O Vagrant precisa de uma chave SSH para comunicar com a máquina na nuvem. Abre o terminal na pasta do projeto e corre:

    ssh-keygen -t rsa -f gcp_key -C vagrant
Isto vai criar dois ficheiros (gcp_key e gcp_key.pub) que serão detetados automaticamente.

## 3. Configurar o Docker

Esta é uma recomendação para prevenir erros com dependências do Windows e para que não seja necessário fazer várias instalações para configurar o vagrant.
- Inicia construindo a imagem: Abre o terminal na pasta e corre:

      docker build -t vagrant-runner .
- Entra no contentor: Corre isto no terminal
  - Em Windows:

            docker run --rm -it -v ${PWD}:/app vagrant-runner bash
  - Em Mac/Linux:

          docker run -it --rm -v "$(pwd)":/app vagrant-runner bash
 
## 4. Configurar as Variáveis

Se o nome dado ao bucket foi adpd-dados-2025-novo, podes saltar este passo. Se não, terás que correr no terminal do docker sempre que reentrares:

    export GOOGLE_BUCKET_NAME="o-meu-nome-unico"

## 5. Executar a pipeline:

Podes agora iniciar a vm através do comando: 
      
      vagrant up --provider=google
Sempre que acabar de correr apenas necessitas de correr este comando.

- O que vai acontecer:
    - O Vagrant cria uma VM económica e2-micro na Google Cloud.
    - Instala Python, DuckDB e as dependências necessárias.
    - Faz o download dos dados brutos.
    - O DuckDB limpa e processa os dados.
    - Os resultados são enviados para a pasta gold no Bucket.
    - A VM apaga-se sozinha para não gerar custos extra.
