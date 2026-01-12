#!/usr/bin/env python3
"""
Сборщик вакансий с hh.ru

Использование:
    python hh_collector.py [max_vacancies] [per_page] [days_back] [page_delay] [detail_delay]

Аргументы:
    max_vacancies: Максимальное количество вакансий для сбора (по умолчанию: 1000)
    per_page: Количество вакансий на страницу (10-100, по умолчанию: 100)
    days_back: Искать вакансии за последние N дней (1-365, по умолчанию: 30)
    page_delay: Задержка между запросами страниц в секундах (0.5-10.0, по умолчанию: 1.0)
    detail_delay: Задержка между детальными запросами в секундах (0.1-5.0, по умолчанию: 0.2)

Переменные окружения:
    HH_MAX_VACANCIES: Максимальное количество вакансий
    HH_PER_PAGE: Вакансий на страницу
    HH_DAYS_BACK: Дней для поиска вакансий
    HH_PAGE_DELAY: Задержка между страницами
    HH_DETAIL_DELAY: Задержка между детальными запросами

Примеры:
    python hh_collector.py 500 50 7 2.0 0.5  # 500 вакансий, медленная скорость
    python hh_collector.py 2000 100 30 0.5 0.1  # 2000 вакансий, быстрая скорость
    python hh_collector.py 100 20 7  # 100 вакансий, 20 на страницу, за 7 дней
    python hh_collector.py           # По умолчанию: 1000 вакансий, 100 на страницу, за 30 дней
"""

import os
import json
import requests
import time
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv('../.env')

def fetch_hh_vacancies(max_vacancies=1000, per_page=100, days_back=30, page_delay=1.0, detail_delay=0.2):
    """
    Получает вакансии с API hh.ru с поддержкой пагинации

    Args:
        max_vacancies: Максимальное количество вакансий для сбора
        per_page: Количество вакансий на страницу (максимум 100)
        days_back: Искать вакансии за последние N дней (по умолчанию 30)
        page_delay: Задержка между запросами страниц в секундах (по умолчанию 1.0)
        detail_delay: Задержка между детальными запросами в секундах (по умолчанию 0.2)
    """

    # Вычисляем дату для фильтрации
    date_from = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

    # Параметры запроса
    base_params = {
        'text': 'DevOps',
        'per_page': min(per_page, 100),  # Максимум 100 по API
        'page': 0,
        'date_from': date_from  # Фильтр по дате публикации
    }

    # Заголовки (улучшенные для избежания блокировки)
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    }

    all_vacancies = []
    page = 0
    max_pages = min(20, (max_vacancies // per_page) + 1)  # Ограничение на количество страниц

    try:
        print(f"Максимум вакансий: {max_vacancies}, на странице: {per_page}")

        while page < max_pages:
            params = base_params.copy()
            params['page'] = page

            # Делаем запрос к API
            try:
                response = requests.get(
                    'https://api.hh.ru/vacancies',
                    params=params,
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()

                data = response.json()
                page_vacancies = data.get('items', [])

                if not page_vacancies:
                    print(f"На странице {page} вакансий не найдено, прекращаем сбор")
                    break

                all_vacancies.extend(page_vacancies)
                found_on_page = len(page_vacancies)

                print(f"Страница {page + 1}: получено {found_on_page} вакансий (всего: {len(all_vacancies)})")

                # Проверяем, не превысили ли лимит
                if len(all_vacancies) >= max_vacancies:
                    all_vacancies = all_vacancies[:max_vacancies]
                    print(f"Достигнут лимит в {max_vacancies} вакансий")
                    break

                # Задержка между запросами страниц
                if page < max_pages - 1:
                    time.sleep(page_delay)

                page += 1

            except requests.RequestException as e:
                print(f"Ошибка при запросе страницы {page}: {e}")
                # Повторяем попытку через 5 секунд
                time.sleep(5)
                continue

        print(f"Всего собрано вакансий: {len(all_vacancies)}")

        # Получаем детальную информацию по каждой вакансии
        detailed_vacancies = []
        error_count = 0
        consecutive_errors = 0

        for i, vacancy in enumerate(all_vacancies, 1):
            try:
                detail_response = requests.get(
                    f"https://api.hh.ru/vacancies/{vacancy['id']}",
                    headers=headers,
                    timeout=15  # Увеличиваем таймаут
                )
                detail_response.raise_for_status()
                detailed_vacancies.append(detail_response.json())

                # Сбрасываем счетчик последовательных ошибок при успешном запросе
                consecutive_errors = 0

                if i % 10 == 0:  # Показываем прогресс каждые 10 вакансий
                    print(f"Обработано вакансий: {i}/{len(all_vacancies)}", end='\r')

            except requests.HTTPError as e:
                error_count += 1
                consecutive_errors += 1

                if e.response.status_code == 403:
                    # Вакансия недоступна (удалена, требует авторизации и т.д.)
                    print(f"Вакансия {vacancy['id']} недоступна (403 Forbidden)")
                elif e.response.status_code == 404:
                    # Вакансия не найдена
                    print(f"Вакансия {vacancy['id']} не найдена (404 Not Found)")
                elif e.response.status_code == 429:
                    # Превышен лимит запросов
                    print(f"Превышен лимит запросов (429 Too Many Requests). Пауза 60 секунд...")
                    time.sleep(60)
                    consecutive_errors = 0  # Сбрасываем счетчик после паузы
                    continue
                else:
                    print(f"HTTP ошибка при получении вакансии {vacancy['id']}: {e}")

            except requests.RequestException as e:
                error_count += 1
                consecutive_errors += 1
                print(f"Ошибка сети при получении вакансии {vacancy['id']}: {e}")

            # Проверяем, не слишком ли много последовательных ошибок
            if consecutive_errors >= 10:
                print(f"\nПрекращаем обработку из-за {consecutive_errors} последовательных ошибок")
                break

            # Задержка между детальными запросами
            time.sleep(detail_delay)

        print(f"\nОбработка завершена. Получено детальной информации: {len(detailed_vacancies)}")
        if error_count > 0:
            print(f"Всего ошибок при получении деталей: {error_count}")
            success_rate = (len(detailed_vacancies) / len(all_vacancies)) * 100
            print(f"{success_rate:.1f}%")

        print(f"\nОбработка завершена. Получено детальной информации: {len(detailed_vacancies)}")

        return detailed_vacancies

    except Exception as e:
        print(f"Критическая ошибка при сборе данных: {e}")
        return []

def save_vacancies(vacancies):
    """Сохраняет вакансии в JSON файл"""
    if not vacancies:
        print("Нет данных для сохранения")
        return None

    # Создаем имя файла с датой
    current_date = datetime.now().strftime('%Y%m%d_%H%M%S')
    # Определяем путь относительно расположения скрипта
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    raw_data_dir = os.path.join(project_root, 'raw_data')
    os.makedirs(raw_data_dir, exist_ok=True)
    filename = os.path.join(raw_data_dir, f'vacancies_{current_date}.json')
    
    # Сохраняем данные
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(vacancies, f, ensure_ascii=False, indent=2)
    
    print(f"Данные сохранены в: {filename}")
    return filename

def main():
    """Основная функция"""
    # Парсим аргументы командной строки
    max_vacancies = 1000  # По умолчанию 1000 вакансий
    per_page = 100  # По умолчанию 100 на страницу
    days_back = 30  # По умолчанию за последние 30 дней
    page_delay = 1.0  # По умолчанию 1 секунда между страницами
    detail_delay = 0.2  # По умолчанию 0.2 секунды между детальными запросами

    if len(sys.argv) > 1:
        try:
            max_vacancies = int(sys.argv[1])
        except ValueError:
            print("Первый аргумент должен быть числом (максимальное количество вакансий)")
            return

    if len(sys.argv) > 2:
        try:
            per_page = int(sys.argv[2])
        except ValueError:
            print("Второй аргумент должен быть числом (вакансий на страницу)")
            return

    if len(sys.argv) > 3:
        try:
            days_back = int(sys.argv[3])
        except ValueError:
            print("Третий аргумент должен быть числом (дней для поиска)")
            return

    if len(sys.argv) > 4:
        try:
            page_delay = float(sys.argv[4])
        except ValueError:
            print("Четвертый аргумент должен быть числом (задержка между страницами)")
            return

    if len(sys.argv) > 5:
        try:
            detail_delay = float(sys.argv[5])
        except ValueError:
            print("Пятый аргумент должен быть числом (задержка между детальными запросами)")
            return

    # Проверяем переменные окружения
    max_vacancies = int(os.getenv('HH_MAX_VACANCIES', max_vacancies))
    per_page = int(os.getenv('HH_PER_PAGE', per_page))
    days_back = int(os.getenv('HH_DAYS_BACK', days_back))
    page_delay = float(os.getenv('HH_PAGE_DELAY', page_delay))
    detail_delay = float(os.getenv('HH_DETAIL_DELAY', detail_delay))

    # Ограничиваем значения разумными пределами
    max_vacancies = min(max_vacancies, 10000)  # Максимум 10000 вакансий
    per_page = min(max(per_page, 10), 100)  # Минимум 10, максимум 100
    days_back = min(max(days_back, 1), 365)  # Минимум 1 день, максимум 365 дней
    page_delay = min(max(page_delay, 0.5), 10.0)  # Минимум 0.5 сек, максимум 10 сек
    detail_delay = min(max(detail_delay, 0.1), 5.0)  # Минимум 0.1 сек, максимум 5 сек

    print("Начало сбора данных с hh.ru...")
    print(f"Параметры: max_vacancies={max_vacancies}, per_page={per_page}, days_back={days_back}")
    print(f"Задержки: page_delay={page_delay}s, detail_delay={detail_delay}s")

    # Получаем вакансии
    vacancies = fetch_hh_vacancies(
        max_vacancies=max_vacancies,
        per_page=per_page,
        days_back=days_back,
        page_delay=page_delay,
        detail_delay=detail_delay
    )

    # Сохраняем вакансии
    if vacancies:
        filename = save_vacancies(vacancies)
        if filename:
            print(f"Сбор данных завершен. Файл: {filename}")
            print(f"Всего сохранено вакансий: {len(vacancies)}")
        else:
            print("Не удалось сохранить данные")
    else:
        print("Не получено ни одной вакансии")

if __name__ == '__main__':
    main()
