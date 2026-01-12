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
    """Тест извлечения навыков из данных HH API"""
    # Тест с массивом словарей (имитация данных от HH)
    mock_skills_data = [
        {'name': 'Docker'},
        {'name': 'Kubernetes'},
        {'name': 'Python'}
    ]
    skills = extract_skills(mock_skills_data)
    assert 'Docker' in skills
    assert 'Kubernetes' in skills
    assert 'Python' in skills
    assert len(skills) == 3
    
    # Тест с пустым массивом
    assert extract_skills([]) == []
    assert extract_skills(None) == []
    
    # Тест с массивом строк (если навык пришел как строка)
    assert extract_skills(['Linux', 'Bash']) == ['Linux', 'Bash']
    
    print("✓ test_extract_skills пройден")

def test_all():
    """Запуск всех тестов"""
    test_normalize_salary()
    test_extract_skills()
    print("\n✅ Все тесты пройдены успешно!")

if __name__ == '__main__':
    test_all()
