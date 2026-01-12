#!/usr/bin/env python3
"""
Удаление записей из таблицы vacancies
"""

import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql import functions as F

def delete_test_vacancies():
    """Удаляет тестовые вакансии"""
    load_dotenv('../../.env')
    
    spark = SparkSession.builder \
        .appName("Delete Vacancies") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .getOrCreate()
    
    db_url = f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'hh_vacancies')}"
    db_properties = {
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver"
    }
    
    # Читаем данные, фильтруем тестовые
    df = spark.read \
        .jdbc(db_url, "vacancies", properties=db_properties)
    
    filtered_df = df.filter(~F.col("vacancy_id").contains("test_"))
    
    # Перезаписываем таблицу без тестовых данных
    filtered_df.write \
        .mode("overwrite") \
        .jdbc(db_url, "vacancies_clean", properties=db_properties)
    
    print("Тестовые вакансии удалены (создана таблица vacancies_clean)")
    print("В реальном приложении используйте DELETE запросы")
    
    spark.stop()

if __name__ == '__main__':
    delete_test_vacancies()
