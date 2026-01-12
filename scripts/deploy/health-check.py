#!/usr/bin/env python3
"""
Скрипт проверки работоспособности после деплоя
"""

import sys
import time
import requests
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def check_postgres():
    """Проверяет доступность PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="hh_vacancies",
            user="postgres",
            password="postgres",
            connect_timeout=5
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            
        conn.close()
        
        if result and result[0] == 1:
            print("✅ PostgreSQL: доступен")
            return True
        else:
            print("❌ PostgreSQL: не доступен")
            return False
            
    except Exception as e:
        print(f"❌ PostgreSQL ошибка: {e}")
        return False

def check_grafana():
    """Проверяет доступность Grafana"""
    try:
        response = requests.get(
            "http://localhost:3000/api/health",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("database") == "ok":
                print("✅ Grafana: доступна")
                return True
            else:
                print(f"❌ Grafana: проблемы с базой данных - {data}")
                return False
        else:
            print(f"❌ Grafana: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Grafana ошибка: {e}")
        return False

def check_services():
    """Проверяет запущенные Docker-сервисы"""
    import subprocess
    
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}} {{.Status}}"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        services = result.stdout.strip().split('\n')
        
        required_services = [
            "hh_postgres_prod",
            "hh_grafana_prod",
            "hh_nginx_prod"
        ]
        
        all_ok = True
        running_services = []
        
        for service in services:
            if service:
                name, status = service.split(' ', 1)
                running_services.append(name)
                
                if "Up" in status:
                    print(f"✅ {name}: запущен")
                else:
                    print(f"❌ {name}: {status}")
                    all_ok = False
        
        # Проверяем отсутствующие сервисы
        for required in required_services:
            if required not in running_services:
                print(f"❌ {required}: не запущен")
                all_ok = False
                
        return all_ok
        
    except Exception as e:
        print(f"❌ Ошибка проверки Docker: {e}")
        return False

def check_endpoints():
    """Проверяет доступность конечных точек"""
    endpoints = [
        ("Grafana Dashboard", "http://localhost:3000"),
        ("PostgreSQL", "localhost:5432"),
    ]
    
    all_ok = True
    
    for name, url in endpoints:
        try:
            if "localhost" in url:
                # Для PostgreSQL используем psycopg2
                if "5432" in url:
                    continue  # Уже проверено
                else:
                    response = requests.get(url, timeout=10)
                    if response.status_code < 500:
                        print(f"✅ {name}: доступен")
                    else:
                        print(f"❌ {name}: HTTP {response.status_code}")
                        all_ok = False
                        
        except Exception as e:
            print(f"❌ {name} ошибка: {e}")
            all_ok = False
            
    return all_ok

def main():
    """Основная функция проверки"""
    print("🔍 Запуск проверки работоспособности...")
    print("=" * 50)
    
    checks = [
        ("Docker сервисы", check_services),
        ("PostgreSQL", check_postgres),
        ("Grafana", check_grafana),
        ("Конечные точки", check_endpoints)
    ]
    
    results = []
    
    for check_name, check_func in checks:
        print(f"\nПроверка: {check_name}")
        print("-" * 30)
        
        try:
            result = check_func()
            results.append(result)
            
            if not result:
                print(f"⚠️  {check_name}: ТРЕБУЕТСЯ ВНИМАНИЕ")
        except Exception as e:
            print(f"❌ Ошибка при проверке {check_name}: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("РЕЗУЛЬТАТ ПРОВЕРКИ:")
    
    success_count = sum(results)
    total_checks = len(results)
    
    if success_count == total_checks:
        print("✅ ВСЕ СИСТЕМЫ РАБОТАЮТ НОРМАЛЬНО")
        return 0
    else:
        print(f"⚠️  {success_count}/{total_checks} проверок пройдены успешно")
        
        # Дополнительная диагностика
        print("\nДиагностика:")
        print("1. Проверьте логи Docker: docker-compose -f docker-compose.prod.yml logs")
        print("2. Проверьте статус сервисов: docker-compose -f docker-compose.prod.yml ps")
        print("3. Перезапустите сервисы: docker-compose -f docker-compose.prod.yml restart")
        
        return 1

if __name__ == "__main__":
    # Даем время сервисам запуститься
    time.sleep(10)
    
    # Запускаем проверку до 3 раз с задержкой
    for attempt in range(3):
        print(f"\nПопытка проверки #{attempt + 1}")
        
        exit_code = main()
        
        if exit_code == 0:
            print("\n✅ Система готова к работе!")
            sys.exit(0)
        else:
            if attempt < 2:
                print(f"\n⏳ Повторная проверка через 30 секунд...")
                time.sleep(30)
            else:
                print("\n❌ Проверка не пройдена после 3 попыток")
                sys.exit(1)
