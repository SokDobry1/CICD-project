#!/usr/bin/env python3
"""
Обработчик вакансий с использованием Spark
"""

import os
import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from dotenv import load_dotenv

def create_spark_session():
    """Создает сессию Spark"""
    return SparkSession.builder \
        .appName("HH Vacancies Processor") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .getOrCreate()

def extract_skills(description):
    """Извлекает ключевые навыки из описания"""
    if not description:
        return []
    
    skills = []
    description_lower = description.lower()
    
    skill_patterns = {
        'docker': 'Docker',
        'kubernetes': 'Kubernetes',
        'aws': 'AWS',
        'python': 'Python',
        'ansible': 'Ansible',
        'terraform': 'Terraform',
        'jenkins': 'Jenkins',
        'gitlab': 'GitLab',
        'bash': 'Bash',
        'linux': 'Linux'
    }
    
    for pattern, skill_name in skill_patterns.items():
        if pattern in description_lower:
            skills.append(skill_name)
    
    return skills

def normalize_salary(salary_data):
    """Нормализует зарплату к RUB"""
    if not salary_data:
        return None
    
    try:
        currency = salary_data.get('currency', '').upper()
        salary_from = salary_data.get('from')
        salary_to = salary_data.get('to')
        
        # Курсы валют (упрощенные)
        exchange_rates = {
            'RUR': 1.0,
            'RUB': 1.0,
            'USD': 95.0,
            'EUR': 102.0,
            'KZT': 0.2
        }
        
        rate = exchange_rates.get(currency, 1.0)
        
        # Вычисляем среднюю зарплату
        if salary_from and salary_to:
            avg_salary = (salary_from + salary_to) / 2
        elif salary_from:
            avg_salary = salary_from
        elif salary_to:
            avg_salary = salary_to
        else:
            return None
        
        return int(avg_salary * rate)
    except:
        return None

def main(input_file):
    """Основная функция обработки"""
    if not os.path.exists(input_file):
        print(f"Файл не найден: {input_file}")
        return
    
    print(f"Обработка файла: {input_file}")
    
    # Загружаем переменные окружения
    load_dotenv('../.env')
    
    # Создаем сессию Spark
    spark = create_spark_session()
    
    # Читаем JSON файл
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Создаем RDD и преобразуем в DataFrame
    rdd = spark.sparkContext.parallelize(data)
    
    # Определяем схему
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("area", StructType([
            StructField("name", StringType(), True)
        ]), True),
        StructField("salary", StructType([
            StructField("from", IntegerType(), True),
            StructField("to", IntegerType(), True),
            StructField("currency", StringType(), True)
        ]), True),
        StructField("snippet", StructType([
            StructField("requirement", StringType(), True),
            StructField("responsibility", StringType(), True)
        ]), True),
        StructField("published_at", StringType(), True)
    ])
    
    df = spark.createDataFrame(rdd, schema=schema)
    
    # Регистрируем UDF
    extract_skills_udf = F.udf(extract_skills, F.ArrayType(StringType()))
    normalize_salary_udf = F.udf(normalize_salary, IntegerType())
    
    # Преобразуем данные
    processed_df = df.select(
        F.col("id").alias("vacancy_id"),
        F.col("name").alias("title"),
        F.col("area.name").alias("city"),
        normalize_salary_udf(F.col("salary")).alias("salary_rub"),
        F.concat(
            F.coalesce(F.col("snippet.requirement"), F.lit("")),
            F.lit(" "),
            F.coalesce(F.col("snippet.responsibility"), F.lit(""))
        ).alias("description"),
        extract_skills_udf(
            F.concat(
                F.coalesce(F.col("snippet.requirement"), F.lit("")),
                F.lit(" "),
                F.coalesce(F.col("snippet.responsibility"), F.lit(""))
            )
        ).alias("skills"),
        F.to_date(F.col("published_at")).alias("published_date"),
        F.current_timestamp().alias("processed_at")
    ).filter(F.col("vacancy_id").isNotNull())
    
    # Выводим статистику
    print("Статистика обработки:")
    print(f"Всего вакансий: {processed_df.count()}")
    print(f"Вакансий с зарплатой: {processed_df.filter(F.col('salary_rub').isNotNull()).count()}")
    
    processed_df.show(10, truncate=False)
    
    # Записываем в PostgreSQL
    db_url = f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'hh_vacancies')}"
    db_properties = {
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver"
    }
    
    print(f"Запись данных в PostgreSQL: {db_url}")
    
    # Записываем данные
    processed_df.write \
        .mode("append") \
        .jdbc(db_url, "vacancies", properties=db_properties)
    
    print("Данные успешно записаны в таблицу 'vacancies'")
    
    # Останавливаем Spark
    spark.stop()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Использование: python spark_processor.py <input_file.json>")
        sys.exit(1)
    
    main(sys.argv[1])
