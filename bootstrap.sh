#!/bin/bash
bucket=$1
pasta=$(dirname "$0")
# Atualizar e instalar dependências
sudo apt-get update -y
sudo apt-get install -y python3-pip python3-venv
python3 -m venv "$pasta/venv"
source "$pasta/venv/bin/activate"
# Iniciar o processo de ingestão e análise
pip install -r "$pasta/requirements.txt"
python3 "$pasta/src/1_ingest.py" "$bucket"
echo "Ingestão concluída. Iniciando análise..."
python3 "$pasta/src/2_analysis.py" "$bucket"
echo "Concluido."