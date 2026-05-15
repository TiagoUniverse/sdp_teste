"""
Teste de Integração - Pipeline End-to-End
Este teste valida o fluxo completo de dados através das camadas bronze, silver e gold
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from datetime import datetime
from utils.utils import transform_bronze, transform_silver


def test_integration_customer_pipeline_end_to_end(spark):
    """
    Teste de integração completo que simula:
    1. Ingestão de dados brutos (raw)
    2. Transformação para Bronze (com metadados)
    3. Transformação para Silver (limpeza e enriquecimento)
    4. Validação final do resultado
    """
    
    # ===== ETAPA 1: Criar dados RAW simulados =====
    raw_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True)
    ])
    
    raw_data = [
        ("C001", "João Silva", "joao.silva@email.com", "1990-05-15", "2024-01-01", "2024-01-01"),
        ("C002", "Maria Santos", "maria.santos@email.com", "1985-08-20", "2024-01-02", "2024-01-02"),
        ("C003", "Pedro Costa", "pedro.costa@email.com", "1992-11-30", "2024-01-03", "2024-01-03"),
        ("C004", "Ana Paula", "ana.paula@email.com", "1988-03-10", "2024-01-04", "2024-01-04"),
    ]
    
    df_raw = spark.createDataFrame(raw_data, raw_schema)
    
    print("📥 Dados RAW carregados:")
    df_raw.show(truncate=False)
    
    # ===== ETAPA 2: Transformação BRONZE =====
    df_bronze = transform_bronze(df_raw)
    
    print("\n🥉 Dados BRONZE (com metadados):")
    df_bronze.show(truncate=False)
    
    # Validações da camada Bronze
    assert "created_bronze" in df_bronze.columns, "Coluna created_bronze deve existir"
    assert "created_ts_bronze" in df_bronze.columns, "Coluna created_ts_bronze deve existir"
    assert df_bronze.count() == 4, "Deve haver 4 registros na bronze"
    
    # ===== ETAPA 3: Transformação SILVER =====
    df_silver = transform_silver(df_bronze)
    
    print("\n🥈 Dados SILVER (processados):")
    df_silver.show(truncate=False)
    df_silver.printSchema()
    
    # Validações da camada Silver
    assert "created_silver" in df_silver.columns, "Coluna created_silver deve existir"
    assert "created_ts_silver" in df_silver.columns, "Coluna created_ts_silver deve existir"
    assert df_silver.count() == 4, "Deve haver 4 registros na silver"
    
    # ===== ETAPA 4: Validações de Negócio =====
    
    # Verificar que todos os emails estão presentes
    emails = [row.email for row in df_silver.select("email").collect()]
    assert len(emails) == 4, "Todos os emails devem estar presentes"
    assert all("@" in email for email in emails), "Todos os emails devem ser válidos"
    
    # Verificar que nenhum dado essencial foi perdido
    null_counts = df_silver.select([
        (df_silver[col].isNull().cast("int").alias(col)) 
        for col in ["customer_id", "name", "email"]
    ])
    
    for col in ["customer_id", "name", "email"]:
        null_count = null_counts.select(col).groupBy().sum().collect()[0][0]
        assert null_count == 0, f"Coluna {col} não deve ter valores nulos"
    
    print("\n✅ TESTE DE INTEGRAÇÃO PASSOU!")
    print(f"   → {df_raw.count()} registros processados")
    print(f"   → Pipeline RAW → BRONZE → SILVER validado")
    print(f"   → Todas as regras de negócio verificadas")


def test_integration_with_data_quality_checks(spark):
    """
    Teste de integração com validações de qualidade de dados
    Simula cenários com dados problemáticos
    """
    
    # Dados com problemas comuns
    problematic_data = [
        ("C001", "João Silva", "joao@email.com", "1990-05-15", "2024-01-01", "2024-01-01"),
        ("C002", None, "maria@email.com", "1985-08-20", "2024-01-02", "2024-01-02"),  # Nome nulo
        ("C003", "Pedro Costa", "email_invalido", "1992-11-30", "2024-01-03", "2024-01-03"),  # Email inválido
        ("C004", "Ana Paula", "ana@email.com", None, "2024-01-04", "2024-01-04"),  # Data de nascimento nula
    ]
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True)
    ])
    
    df_raw = spark.createDataFrame(problematic_data, schema)
    
    # Processar através do pipeline
    df_bronze = transform_bronze(df_raw)
    df_silver = transform_silver(df_bronze)
    
    print("\n🔍 Análise de Qualidade de Dados:")
    print(f"   Total de registros: {df_silver.count()}")
    
    # Contar registros com problemas
    null_names = df_silver.filter(df_silver.name.isNull()).count()
    null_birthdates = df_silver.filter(df_silver.birth_date.isNull()).count()
    
    print(f"   Nomes nulos: {null_names}")
    print(f"   Datas de nascimento nulas: {null_birthdates}")
    
    # Verificar que o pipeline não perde registros (mesmo com dados problemáticos)
    assert df_silver.count() == 4, "Pipeline deve manter todos os registros para análise"
    
    print("\n✅ TESTE DE QUALIDADE DE DADOS PASSOU!")


def test_integration_pipeline_performance(spark):
    """
    Teste de integração com volume maior de dados
    Valida performance e escalabilidade
    """
    
    # Gerar dados em maior volume
    large_data = [
        (f"C{i:04d}", f"Cliente {i}", f"cliente{i}@email.com", 
         f"199{i%10}-0{(i%12)+1}-{(i%28)+1:02d}", 
         "2024-01-01", "2024-01-01")
        for i in range(1000)  # 1000 registros
    ]
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True)
    ])
    
    df_raw = spark.createDataFrame(large_data, schema)
    
    print(f"\n⚡ Teste de Performance com {df_raw.count()} registros")
    
    # Processar através do pipeline
    df_bronze = transform_bronze(df_raw)
    df_silver = transform_silver(df_bronze)

    expected_columns = [
    "customer_id", "name", "email", "birth_date",
    "created_at", "updated_at",
    "created_silver", "created_ts_silver"
    ]
    
    # Validações
    assert df_silver.count() == 1000, "Todos os registros devem ser processados"
    assert df_silver.columns == expected_columns, "Schema deve ter as colunas corretas"

    print(f"✅ Pipeline processou {df_silver.count()} registros com sucesso!")
