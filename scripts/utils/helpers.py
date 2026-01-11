#!/usr/bin/env python3
"""
Вспомогательные функции
"""

import re
from datetime import datetime

def parse_date(date_string):
    """Парсит дату из строки"""
    try:
        return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
    except:
        return None

def extract_number(text):
    """Извлекает число из текста"""
    if not text:
        return None
    
    numbers = re.findall(r'\d+', text.replace(',', '').replace(' ', ''))
    return int(numbers[0]) if numbers else None

def validate_vacancy(vacancy):
    """Проверяет вакансию на корректность"""
    required_fields = ['id', 'name', 'area']
    
    for field in required_fields:
        if field not in vacancy:
            return False
    
    return True

def format_skills(skills_list):
    """Форматирует список навыков"""
    if not skills_list:
        return []
    
    # Убираем дубликаты и сортируем
    return sorted(list(set(skill.strip() for skill in skills_list if skill.strip())))
