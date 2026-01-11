#!/usr/bin/env python3
"""
Тесты для процессора вакансий
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../scripts'))

from spark_processor import normalize_salary, extract_skills

def test_normalize_salary():
    """Тест нормализации зарплаты"""
    # Тест с рублями
    salary_rub = {'from': 100000, 'to': 150000, 'currency': 'RUR'}
    assert normalize_salary(salary_rub) == 125000
    
    # Тест с долларами
    salary_usd = {'from': 1000, 'currency': 'USD'}
    normalized = normalize_salary(salary_usd)
    assert normalized is not None
    assert normalized > 90000  # Примерный курс
    
    # Тест без данных
    assert normalize_salary(None) is None
    assert normalize_salary({}) is None
    
    print("✓ test_normalize_salary пройден")

def test_extract_skills():
    """Тест извлечения навыков"""
    # Тест с несколькими навыками
    description = "Требуется знание Docker и Kubernetes. Python будет плюсом."
    skills = extract_skills(description)
    assert 'Docker' in skills
    assert 'Kubernetes' in skills
    assert 'Python' in skills
    assert len(skills) == 3
    
    # Тест без навыков
    assert extract_skills("Обычное описание") == []
    assert extract_skills("") == []
    assert extract_skills(None) == []
    
    print("✓ test_extract_skills пройден")

def test_all():
    """Запуск всех тестов"""
    test_normalize_salary()
    test_extract_skills()
    print("\n✅ Все тесты пройдены успешно!")

if __name__ == '__main__':
    test_all()
