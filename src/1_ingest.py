import duckdb
import os
import sys
from google.cloud import storage

bucket_name = "adpd-dados-grupo8-final"
if len(sys.argv) > 1:
    bucket_name = sys.argv[1]

base_url = "https://huggingface.co/datasets/einrafh/hnm-fashion-recommendations-data/resolve/main/data/raw"
datasets = {
    "articles": base_url + "/articles.csv",
    "customers": base_url + "/customers.csv",
    "transactions": base_url + "/transactions_train.csv"
}

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs;")

print("Iniciando ingestao para o bucket:")

for name, url in datasets.items():
    parquet_file = name + ".parquet"
    blob_path = "raw/" + parquet_file
    
    blob = bucket.blob(blob_path)
    if blob.exists():
        print("Ficheiro ", name, " já existe. A saltar...")
        continue

    print("A processar ficheiro ", name)
    
    try:
        sql = "COPY (SELECT * FROM read_csv('" + url + "', AUTO_DETECT=TRUE)) TO '" + parquet_file + "' (FORMAT 'PARQUET')"
        con.sql(sql)
        
        blob.upload_from_filename(parquet_file)
        print("Upload concluido")
        
    except Exception as e:
        print("Erro ao processar ", name, ": ", str(e))
    
    if os.path.exists(parquet_file):
        os.remove(parquet_file)

print("Processo de ingestao terminado.")