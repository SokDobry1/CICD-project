#!/usr/bin/env python3
"""
Обработчик вакансий с использованием Spark
"""

import os
import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType
from dotenv import load_dotenv

def create_spark_session():
    """Создает сессию Spark"""
    # Получаем абсолютный путь к JAR файлу
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    jar_path = os.path.join(project_root, 'lib', 'postgresql-42.7.4.jar')

    return SparkSession.builder \
        .appName("HH Vacancies Processor") \
        .config("spark.driver.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .config("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8") \
        .config("spark.jars", jar_path) \
        .config("spark.sql.broadcastTimeout", "600") \
        .getOrCreate()

def extract_skills(key_skills):
    """Извлекает названия навыков из массива"""
    if not key_skills:
        return []

    skills = []
    for skill in key_skills:
        if hasattr(skill, 'name'):
            skills.append(skill.name)
        elif isinstance(skill, dict) and 'name' in skill:
            skills.append(skill['name'])
        elif isinstance(skill, str):
            skills.append(skill)

    return skills

def normalize_salary(salary_data):
    """Нормализует зарплату к RUB"""
    if not salary_data:
        return None

    try:
        # Для Row объектов Spark используем атрибуты или индексы
        if hasattr(salary_data, 'currency'):
            currency = salary_data.currency.upper() if salary_data.currency else ''
        else:
            currency = str(salary_data.get('currency', '')).upper()

        if hasattr(salary_data, 'from'):
            salary_from = salary_data['from']  # 'from' является ключевым словом, используем индекс
        else:
            salary_from = salary_data.get('from')

        if hasattr(salary_data, 'to'):
            salary_to = salary_data.to
        else:
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
    print(f"Запуск spark_processor.py с файлом: {input_file}")

    if not os.path.exists(input_file):
        print(f"Файл не найден: {input_file}")
        return

    print(f"Файл найден, начинаем обработку: {input_file}")

    # Загружаем переменные окружения в начале
    env_path = '../.env'
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"Загружены переменные окружения из {env_path}")
    else:
        print(f"Файл .env не найден по пути {env_path}")

    # Проверяем и устанавливаем переменные окружения
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '9301')
    db_name = os.getenv('DB_NAME', 'hh_vacancies')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')

    print(f"БД параметры: host={db_host}, port={db_port}, db={db_name}, user={db_user}")

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
        StructField("description", StringType(), True),
        StructField("key_skills", ArrayType(StructType([
            StructField("name", StringType(), True)
        ])), True),
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
        F.regexp_replace(F.col("description"), "<[^>]+>", "").alias("description"),  # Убираем HTML теги
        extract_skills_udf(F.col("key_skills")).alias("skills"),
        F.to_date(F.col("published_at")).alias("published_date"),
        F.current_timestamp().alias("processed_at")
    ).filter(F.col("vacancy_id").isNotNull())

    # Выводим статистику
    print("Статистика обработки:")
    print(f"Всего вакансий: {processed_df.count()}")
    print(f"Вакансий с зарплатой: {processed_df.filter(F.col('salary_rub').isNotNull()).count()}")

    processed_df.show(10, truncate=False)

    # Записываем в PostgreSQL
    db_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    db_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    print(f"Подключение к БД: {db_url} как {db_user}")
    
    print(f"Запись данных в PostgreSQL: {db_url}")

    # Создаем временную таблицу для новых данных
    temp_table = "vacancies_temp"
    processed_df.write \
        .mode("overwrite") \
        .jdbc(db_url, temp_table, properties=db_properties)

    # Выполняем UPSERT через прямое подключение к PostgreSQL
    import psycopg2

    print(f"psycopg2 подключение: host={db_host}, port={db_port}, db={db_name}, user={db_user}")

    conn = psycopg2.connect(
        host=db_host,
        port=int(db_port),
        database=db_name,
        user=db_user,
        password=db_password
    )

    try:
        with conn.cursor() as cursor:
            # Вставляем данные с обработкой конфликтов
            cursor.execute("""
                INSERT INTO vacancies (vacancy_id, title, city, salary_rub, description, skills, published_date, processed_at)
                SELECT vacancy_id, title, city, salary_rub, description, skills, published_date, processed_at
                FROM vacancies_temp
                ON CONFLICT (vacancy_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    city = EXCLUDED.city,
                    salary_rub = EXCLUDED.salary_rub,
                    description = EXCLUDED.description,
                    skills = EXCLUDED.skills,
                    published_date = EXCLUDED.published_date,
                    processed_at = EXCLUDED.processed_at
            """)

            # Удаляем временную таблицу
            cursor.execute("DROP TABLE IF EXISTS vacancies_temp")

            conn.commit()
            print(f"Данные успешно обновлены в таблице 'vacancies'. Затронуто строк: {cursor.rowcount}")

    except Exception as e:
        conn.rollback()
        print(f"Ошибка при записи в базу данных: {e}")
        raise
    finally:
        conn.close()
    
    # Останавливаем Spark
    spark.stop()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Использование: python spark_processor.py <input_file.json>")
        sys.exit(1)
    
    main(sys.argv[1])
