#!/usr/bin/env python3
"""
Чтение записей из таблицы vacancies
"""

import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

def read_vacancies(limit=10):
    """Читает вакансии из таблицы"""
    load_dotenv('../../.env')
    
    spark = SparkSession.builder \
        .appName("Read Vacancies") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .getOrCreate()
    
    db_url = f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'hh_vacancies')}"
    db_properties = {
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver"
    }
    
    df = spark.read \
        .jdbc(db_url, "vacancies", properties=db_properties) \
        .limit(limit)
    
    print(f"Всего записей: {df.count()}")
    df.show(truncate=False)
    
    spark.stop()

if __name__ == '__main__':
    read_vacancies(10)
