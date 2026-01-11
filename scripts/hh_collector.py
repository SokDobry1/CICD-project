#!/usr/bin/env python3
"""
Сборщик вакансий с hh.ru
"""

import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv('../.env')

def fetch_hh_vacancies():
    """Получает вакансии с API hh.ru"""
    
    # Параметры запроса
    params = {
        'text': 'DevOps',
        'area': os.getenv('HH_AREA', '113'),  # Ростов-на-Дону
        'per_page': 100,  # Максимальное количество на страницу
        'period': 3,  # За последние 3 дня
        'only_with_salary': False
    }
    
    # Заголовки
    headers = {
        'User-Agent': 'devops-analysis/1.0 (email@example.com)',
        'HH-User-Agent': 'devops-analysis/1.0 (email@example.com)'
    }
    
    vacancies = []
    
    try:
        print(f"Запрос вакансий для региона: {params['area']}")
        
        # Делаем запрос к API
        response = requests.get(
            'https://api.hh.ru/vacancies',
            params=params,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        vacancies = data.get('items', [])
        
        print(f"Получено вакансий: {len(vacancies)}")
        
        # Получаем детальную информацию по каждой вакансии
        detailed_vacancies = []
        for i, vacancy in enumerate(vacancies, 1):
            try:
                detail_response = requests.get(
                    f"https://api.hh.ru/vacancies/{vacancy['id']}",
                    headers=headers,
                    timeout=10
                )
                detail_response.raise_for_status()
                detailed_vacancies.append(detail_response.json())
                print(f"Обработано вакансий: {i}/{len(vacancies)}", end='\r')
            except requests.RequestException as e:
                print(f"Ошибка при получении вакансии {vacancy['id']}: {e}")
                continue
        
        print()  # Новая строка после прогресса
        
        return detailed_vacancies
        
    except requests.RequestException as e:
        print(f"Ошибка при запросе к API: {e}")
        return []

def save_vacancies(vacancies):
    """Сохраняет вакансии в JSON файл"""
    if not vacancies:
        print("Нет данных для сохранения")
        return None
    
    # Создаем имя файла с датой
    current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'../raw_data/vacancies_{current_date}.json'
    
    # Сохраняем данные
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(vacancies, f, ensure_ascii=False, indent=2)
    
    print(f"Данные сохранены в: {filename}")
    return filename

def main():
    """Основная функция"""
    print("Начало сбора данных с hh.ru...")
    
    # Получаем вакансии
    vacancies = fetch_hh_vacancies()
    
    # Сохраняем вакансии
    if vacancies:
        filename = save_vacancies(vacancies)
        if filename:
            print(f"Сбор данных завершен. Файл: {filename}")
        else:
            print("Не удалось сохранить данные")
    else:
        print("Не получено ни одной вакансии")

if __name__ == '__main__':
    main()
