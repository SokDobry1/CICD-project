#!/bin/bash

# Основной скрипт запуска ETL-пайплайна

set -e

echo "Запуск ETL-пайплайна..."

# Загружаем переменные окружения
if [ -f ../.env ]; then
    export $(cat ../.env | grep -v '^#' | xargs)
else
    echo "Предупреждение: .env файл не найден, используем значения по умолчанию"
fi

# Создаем директорию для сырых данных
mkdir -p ../raw_data

# Шаг 1: Сбор данных с hh.ru
echo "Сбор данных с hh.ru..."
python hh_collector.py

# Находим последний собранный файл
LATEST_FILE=$(ls -t ../raw_data/vacancies_*.json 2>/dev/null | head -1)

if [ -z "$LATEST_FILE" ]; then
    echo "Ошибка: Не найдены данные для обработки"
    exit 1
fi

echo "Обработка файла: $LATEST_FILE"

# Шаг 2: Обработка данных с помощью Spark
echo "Обработка данных с помощью Spark..."
python spark_processor.py "$LATEST_FILE"

echo "ETL-пайплайн успешно завершен!"
