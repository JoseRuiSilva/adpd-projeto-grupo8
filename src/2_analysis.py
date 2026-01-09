import duckdb
import sys
import os
import time
from google.cloud import storage

bucket_name = "adpd-dados-grupo8-final"
if len(sys.argv) > 1:
    bucket_name = sys.argv[1]

local_trans = "/tmp/transactions.parquet"
local_cust = "/tmp/customers.parquet"
local_art = "/tmp/articles.parquet"

caminho_chave = os.path.expanduser("~/gcp-key.json")
client = storage.Client.from_service_account_json(caminho_chave)
bucket = client.bucket(bucket_name)

bucket.blob("raw/transactions.parquet").download_to_filename(local_trans)
bucket.blob("raw/customers.parquet").download_to_filename(local_cust)
bucket.blob("raw/articles.parquet").download_to_filename(local_art)

con = duckdb.connect()
con.sql("SET memory_limit='3GB';") 
con.sql("SET temp_directory='/tmp/duckdb_temp.tmp';")

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


# 1. CARATERIZAÇÃO DE CLIENTES
start_time = time.time()
q_clientes = f"""
COPY (
    WITH base AS (
        SELECT
            t.customer_id,
            t.t_dat AS dt,
            t.article_id,
            t.price,
            t.sales_channel_id,
            a.section_name
        FROM trans t
        LEFT JOIN art a ON t.article_id = a.article_id
    ),
    agg AS (
        SELECT
            customer_id,
            COUNT(*) AS total_items,
            SUM(price) AS total_value,
            MAX(dt) AS last_purchase_date
        FROM base
        GROUP BY customer_id
    ),
    fav_channel AS (
        SELECT customer_id, sales_channel_id
        FROM (
            SELECT customer_id, sales_channel_id,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) as rn
            FROM base
            GROUP BY customer_id, sales_channel_id
        ) x WHERE rn = 1
    ),
    fav_section AS (
        SELECT customer_id, section_name
        FROM (
            SELECT customer_id, section_name,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) as rn
            FROM base
            WHERE section_name IS NOT NULL
            GROUP BY customer_id, section_name
        ) x WHERE rn = 1
    )
    SELECT
        a.customer_id,
        c.club_member_status,
        a.total_items,
        ROUND(a.total_value, 2) as total_value,
        a.last_purchase_date,
        DATE_DIFF('day', a.last_purchase_date, CURRENT_DATE) AS days_since_last_purchase,
        ch.sales_channel_id AS favourite_channel,
        s.section_name AS favourite_section
    FROM agg a
    LEFT JOIN fav_channel ch USING (customer_id)
    LEFT JOIN fav_section s USING (customer_id)
    LEFT JOIN cust c ON a.customer_id = c.customer_id
) TO 'resultado_clientes.csv' (HEADER, DELIMITER ',')
"""
con.sql(q_clientes)
print(f"Concluído query 1 em {time.time() - start_time:.2f}s")


# 2. CARATERIZAÇÃO DE PRODUTOS (Sazonalidade)
start_time = time.time()
q_produtos = f"""
COPY (
    SELECT 
        a.product_group_name,
        COUNT(*) as total_vendas,
        ROUND(SUM(t.price), 2) as valor_total,
        ROUND(SUM(CASE WHEN MONTH(t.t_dat) IN (12, 1, 2) THEN t.price ELSE 0 END), 2) as valor_inverno,
        ROUND(SUM(CASE WHEN MONTH(t.t_dat) IN (3, 4, 5) THEN t.price ELSE 0 END), 2) as valor_primavera,
        ROUND(SUM(CASE WHEN MONTH(t.t_dat) IN (6, 7, 8) THEN t.price ELSE 0 END), 2) as valor_verao,
        ROUND(SUM(CASE WHEN MONTH(t.t_dat) IN (9, 10, 11) THEN t.price ELSE 0 END), 2) as valor_outono
    FROM trans t
    JOIN art a ON t.article_id = a.article_id
    GROUP BY a.product_group_name
    ORDER BY valor_total DESC
) TO 'resultado_produtos.csv' (HEADER, DELIMITER ',')
"""
con.sql(q_produtos)
print(f"Concluído query 2 em {time.time() - start_time:.2f}s")


# 3. TOP 10 CLIENTES
start_time = time.time()
q_top10 = f"""
COPY (
    WITH Top10Customers AS (
        SELECT customer_id, SUM(price) as total_spent
        FROM trans
        GROUP BY customer_id
        ORDER BY total_spent DESC
        LIMIT 10
    )
    SELECT 
        tc.customer_id,
        ROUND(tc.total_spent, 2) as total_spent,
        a.prod_name,
        a.product_group_name,
        a.colour_group_name,
        COUNT(*) as quantity_bought
    FROM Top10Customers tc
    JOIN trans t ON tc.customer_id = t.customer_id
    JOIN art a ON t.article_id = a.article_id
    GROUP BY ALL
    ORDER BY total_spent DESC, quantity_bought DESC
) TO 'resultado_top10.csv' (HEADER, DELIMITER ',')
"""
con.sql(q_top10)
print(f"Concluída query 3 em {time.time() - start_time:.2f}s")


# 4. DESEMPENHO AO LONGO DO TEMPO (Agrupado por Mes e Cor)
start_time = time.time()
q_tempo = f"""
COPY (
    WITH base AS (
        SELECT
            CAST(t.t_dat AS DATE) AS dt,
            DATE_TRUNC('month', CAST(t.t_dat AS DATE)) AS month,
            t.article_id,
            t.price,
            a.colour_group_name
        FROM trans t
        LEFT JOIN art a
            ON t.article_id = a.article_id
    ),
    agg AS (
        SELECT
            month,
            colour_group_name,
            COUNT(*) AS total_items,
            SUM(price) AS total_value
        FROM base
        WHERE colour_group_name IS NOT NULL
        GROUP BY month, colour_group_name
    )
    SELECT *
    FROM agg
    ORDER BY month, total_value DESC
) TO 'resultado_tempo.csv' (HEADER, DELIMITER ',')
"""
con.sql(q_tempo)
print(f"Concluída query 4 em {time.time() - start_time:.2f}s")


bucket.blob("gold/resultado_clientes.csv").upload_from_filename("resultado_clientes.csv")
bucket.blob("gold/resultado_produtos.csv").upload_from_filename("resultado_produtos.csv")
bucket.blob("gold/resultado_top10.csv").upload_from_filename("resultado_top10.csv")
bucket.blob("gold/resultado_tempo.csv").upload_from_filename("resultado_tempo.csv")

bucket.blob("gold/SUCESSO.txt").upload_from_string("Processamento concluído com sucesso.")

files_to_remove = [
    local_trans, local_cust, local_art, 
    "resultado_clientes.csv", "resultado_produtos.csv", 
    "resultado_top10.csv", "resultado_tempo.csv"
]
for f in files_to_remove:
    if os.path.exists(f): 
        os.remove(f)