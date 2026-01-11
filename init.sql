-- Таблица для хранения обработанных вакансий
CREATE TABLE IF NOT EXISTS vacancies (
    vacancy_id VARCHAR(50) PRIMARY KEY,
    title TEXT NOT NULL,
    city VARCHAR(100),
    salary_rub INTEGER,
    description TEXT,
    skills TEXT[],
    published_date DATE,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для ускорения запросов
CREATE INDEX IF NOT EXISTS idx_vacancies_date ON vacancies(published_date);
CREATE INDEX IF NOT EXISTS idx_vacancies_city ON vacancies(city);
CREATE INDEX IF NOT EXISTS idx_vacancies_salary ON vacancies(salary_rub);

-- Представление для аналитики
CREATE OR REPLACE VIEW vacancies_stats AS
SELECT 
    published_date,
    COUNT(*) as total_vacancies,
    AVG(salary_rub)::INTEGER as avg_salary,
    COUNT(DISTINCT city) as cities_count
FROM vacancies
GROUP BY published_date
ORDER BY published_date DESC;
