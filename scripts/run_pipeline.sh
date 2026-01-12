#!/bin/bash

# Основной скрипт запуска ETL-пайплайна

set -e

echo "Запуск ETL-пайплайна..."


# Создаем директорию для сырых данных
mkdir -p ../raw_data

# Шаг 1: Сбор данных с hh.ru
echo "Сбор данных с hh.ru..."
source ../venv/bin/activate && python3 hh_collector.py && deactivate

# Находим последний собранный файл
LATEST_FILE=$(ls -t ../raw_data/vacancies_*.json 2>/dev/null | head -1)

if [ -z "$LATEST_FILE" ]; then
    echo "Ошибка: Не найдены данные для обработки"
    exit 1
fi

echo "Обработка файла: $LATEST_FILE"

# Шаг 2: Обработка данных с помощью Spark
echo "Обработка данных с помощью Spark..."
source ../venv/bin/activate && python3 spark_processor.py "$LATEST_FILE" && deactivate

echo "ETL-пайплайн успешно завершен!"
