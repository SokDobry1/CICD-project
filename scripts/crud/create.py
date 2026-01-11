#!/usr/bin/env python3
"""
Создание записи в таблице vacancies
"""

import os
import sys
from pyspark.sql import SparkSession
from dotenv import load_dotenv

def create_vacancy():
    """Создает новую вакансию"""
    load_dotenv('../../.env')
    
    spark = SparkSession.builder \
        .appName("Create Vacancy") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .getOrCreate()
    
    # Пример данных для новой вакансии
    data = [{
        "vacancy_id": "test_123456",
        "title": "Test DevOps Engineer",
        "city": "Ростов-на-Дону",
        "salary_rub": 150000,
        "description": "Тестовая вакансия",
        "skills": ["Docker", "Kubernetes"],
        "published_date": "2024-01-01",
        "processed_at": "2024-01-01 12:00:00"
    }]
    
    df = spark.createDataFrame(data)
    
    db_url = f"jdbc:postgresql://{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'hh_vacancies')}"
    db_properties = {
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver"
    }
    
    df.write \
        .mode("append") \
        .jdbc(db_url, "vacancies", properties=db_properties)
    
    print("Тестовая вакансия создана")
    spark.stop()

if __name__ == '__main__':
    create_vacancy()
