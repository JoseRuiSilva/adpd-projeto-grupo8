import duckdb
import sys
import os
from google.cloud import storage

print("Iniciando o processo de análise de dados...")
bucket_name = "adpd-dados-2025-novo"
if len(sys.argv) > 1:
    bucket_name = sys.argv[1]

print("Usando o bucket:", bucket_name)
local_trans = "/tmp/transactions.parquet"
local_cust = "/tmp/customers.parquet"
local_art = "/tmp/articles.parquet"
output_csv = "resultado_analise.csv"

print("A configurar o cliente GCP...")
caminho_chave = os.path.expanduser("~/gcp-key.json")
client = storage.Client.from_service_account_json(caminho_chave)
bucket = client.bucket(bucket_name)

print("Baixando ficheiros do bucket:", bucket_name)
bucket.blob("raw/transactions.parquet").download_to_filename(local_trans)
bucket.blob("raw/customers.parquet").download_to_filename(local_cust)
bucket.blob("raw/articles.parquet").download_to_filename(local_art)

con = duckdb.connect()
con.sql("SET memory_limit='600MB';") 
con.sql("SET temp_directory='/tmp/duckdb_temp.tmp';")

print("A criar tabelas temporarias com limpeza de dados...")

con.sql(f"""
    CREATE TABLE art AS
    SELECT 
        CAST(article_id AS VARCHAR) as article_id, 
        TRIM(LOWER(prod_name)) as prod_name,
        TRIM(LOWER(product_group_name)) as product_group_name,
        TRIM(LOWER(section_name)) as section_name,
        TRIM(LOWER(colour_group_name)) as colour_group_name,
        COALESCE(TRIM(LOWER(detail_desc)), 'unknown') as detail_desc
    FROM '{local_art}'
    WHERE article_id != '-1' 
""")

con.sql(f"""
    CREATE TABLE cust AS
    SELECT 
        CAST(customer_id AS VARCHAR) as customer_id,
        CASE WHEN age BETWEEN 0 AND 120 THEN age ELSE NULL END as age,
        COALESCE(CAST(FN AS INTEGER), 0) as FN,
        COALESCE(CAST(Active AS INTEGER), 0) as Active,
        COALESCE(TRIM(LOWER(club_member_status)), 'unknown') as club_member_status,
        COALESCE(TRIM(LOWER(fashion_news_frequency)), 'unknown') as fashion_news_frequency
    FROM '{local_cust}'
""")

con.sql(f"""
    CREATE TABLE trans AS
    SELECT 
        CAST(t_dat AS DATE) as t_dat,
        CAST(customer_id AS VARCHAR) as customer_id,
        CAST(article_id AS VARCHAR) as article_id,
        price,
        CAST(sales_channel_id AS VARCHAR) as sales_channel_id
    FROM '{local_trans}'
    WHERE price >= 0 
    AND customer_id IS NOT NULL 
    AND article_id IS NOT NULL
""")

print("A executar a queris de analise...")
query1 = f"""
COPY (
    SELECT 
        t.customer_id, 
        COUNT(*) as total_compras, 
        SUM(t.price) as total_gasto, 
        c.age 
    FROM trans t 
    LEFT JOIN cust c ON t.customer_id = c.customer_id 
    GROUP BY ALL
) TO '{output_csv}' (HEADER)
"""

con.sql(query1)

print("A enviar o resultado para:", bucket_name)
bucket.blob("gold/" + output_csv).upload_from_filename(output_csv)

os.remove(local_trans)
os.remove(local_cust)
os.remove(local_art)
os.remove(output_csv)

print("Processo concluido com sucesso")