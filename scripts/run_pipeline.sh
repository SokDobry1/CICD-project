#!/usr/bin/env bash
# ============================================================
# ETL-пайплайн: сбор и обработка вакансий с hh.ru
# ============================================================

set -Eeuo pipefail

# -----------------------------
# Константы и пути
# -----------------------------
VENV_PATH="../venv/bin/activate"
RAW_DATA_DIR="../raw_data"

COLLECTOR_SCRIPT="hh_collector.py"
SPARK_SCRIPT="spark_processor.py"

# Параметры сбора (соответствуют hh_collector.py)
MAX_VACANCIES=20
PER_PAGE=20
DAYS_BACK=7
PAGE_DELAY=2.0
DETAIL_DELAY=0.5

# -----------------------------
# Цвета и логирование
# -----------------------------
RED="\033[0;31m"
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
BLUE="\033[0;34m"
NC="\033[0m"

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# -----------------------------
# Обработка ошибок
# -----------------------------
trap 'log_error "Скрипт завершился с ошибкой (строка $LINENO)"; exit 1' ERR

# -----------------------------
# Функции
# -----------------------------
activate_venv() {
    if [[ ! -f "$VENV_PATH" ]]; then
        log_error "Виртуальное окружение не найдено: $VENV_PATH"
        exit 1
    fi
    # shellcheck disable=SC1090
    source "$VENV_PATH"
}

run_collector() {
    log_info "Сбор данных с hh.ru"
    activate_venv
    python3 "$COLLECTOR_SCRIPT" \
        "$MAX_VACANCIES" \
        "$PER_PAGE" \
        "$DAYS_BACK" \
        "$PAGE_DELAY" \
        "$DETAIL_DELAY"
    deactivate
}

find_latest_file() {
    ls -t "$RAW_DATA_DIR"/vacancies_*.json 2>/dev/null | head -n 1 || true
}

run_spark() {
    local input_file="$1"
    log_info "Обработка данных с помощью Spark"
    activate_venv
    python3 "$SPARK_SCRIPT" "$input_file"
    deactivate
}

# -----------------------------
# Основной поток выполнения
# -----------------------------
log_info "Запуск ETL-пайплайна"

log_info "Подготовка директорий"
mkdir -p "$RAW_DATA_DIR"

run_collector

LATEST_FILE=$(find_latest_file)

if [[ -z "$LATEST_FILE" ]]; then
    log_error "Не найдены данные для обработки в $RAW_DATA_DIR"
    exit 1
fi

log_info "Найден файл данных: $LATEST_FILE"

run_spark "$LATEST_FILE"

log_success "ETL-пайплайн успешно завершён"
