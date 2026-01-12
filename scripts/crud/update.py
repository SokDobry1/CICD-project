#!/usr/bin/env python3
"""
Обновление записей в таблице vacancies
"""

import os
from pyspark.sql import SparkSession, functions as F
from dotenv import load_dotenv
from pyspark.sql import functions as F

def update_vacancies():
    """Обновляет зарплату для тестовых вакансий"""
    load_dotenv('../../.env')
    
    spark = SparkSession.builder \
        .appName("Update Vacancies") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .getOrCreate()
    
    db_url = f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'hh_vacancies')}"
    db_properties = {
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver"
    }
    
    # Читаем данные
    df = spark.read \
        .jdbc(db_url, "vacancies", properties=db_properties)
    
    # Обновляем зарплату для тестовых вакансий
    updated_df = df.withColumn(
        "salary_rub",
        F.when(F.col("vacancy_id") == "test_123456", 200000)
         .otherwise(F.col("salary_rub"))
    )
    
    # Перезаписываем таблицу
    updated_df.write \
        .mode("overwrite") \
        .jdbc(db_url, "vacancies_temp", properties=db_properties)
    
    # В реальном приложении нужно было бы переименовать таблицу
    print("Для обновления данных в реальном приложении нужны дополнительные операции")
    print("В демонстрационных целях создана таблица vacancies_temp")
    
    spark.stop()

if __name__ == '__main__':
    update_vacancies()
