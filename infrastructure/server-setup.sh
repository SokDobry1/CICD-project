#!/bin/bash
# =================================================================
# DevOops Vacancy Analytics - Server Setup Script
# =================================================================

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Проверка прав root
if [[ $EUID -ne 0 ]]; then
   log_error "Этот скрипт должен быть запущен с правами root"
   exit 1
fi

# Переменные
PROJECT_NAME="devops-vacancy-analysis"
PROJECT_PATH="/opt/${PROJECT_NAME}"
GITHUB_REPO="https://github.com/ваш-username/ваш-репозиторий.git"

log_info "Начало настройки сервера для проекта: ${PROJECT_NAME}"

# =================================================================
# 1. Обновление системы
# =================================================================
log_info "1. Обновление системы..."
apt-get update
apt-get upgrade -y

# =================================================================
# 2. Установка Docker
# =================================================================
log_info "2. Установка Docker..."
apt-get install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io

# =================================================================
# 3. Установка Docker Compose
# =================================================================
log_info "3. Установка Docker Compose..."
curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# =================================================================
# 4. Настройка Docker
# =================================================================
log_info "4. Настройка Docker..."
usermod -aG docker $USER
systemctl enable docker
systemctl start docker

# =================================================================
# 5. Установка Git
# =================================================================
log_info "5. Установка Git..."
apt-get install -y git

# =================================================================
# 6. Создание структуры проекта
# =================================================================
log_info "6. Создание структуры проекта..."
mkdir -p ${PROJECT_PATH}
cd ${PROJECT_PATH}

# =================================================================
# 7. Клонирование репозитория (если указан)
# =================================================================
if [ ! -z "${GITHUB_REPO}" ] && [ "${GITHUB_REPO}" != "https://github.com/ваш-username/ваш-репозиторий.git" ]; then
    log_info "7. Клонирование репозитория..."
    git clone ${GITHUB_REPO} .
else
    log_info "7. Создание базовой структуры..."
    mkdir -p {scripts,config,raw_data,logs,nginx}
    touch docker-compose.prod.yml .env
fi

# =================================================================
# 8. Настройка firewall
# =================================================================
log_info "8. Настройка firewall..."
if command -v ufw &> /dev/null; then
    ufw allow 22/tcp  # SSH
    ufw allow 80/tcp  # HTTP
    ufw allow 443/tcp # HTTPS
    ufw allow 3000/tcp # Grafana
    ufw --force enable
fi

# =================================================================
# 9. Настройка swap
# =================================================================
log_info "9. Настройка swap..."
if [ ! -f /swapfile ]; then
    fallocate -l 2G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' | tee -a /etc/fstab
    sysctl vm.swappiness=10
    sysctl vm.vfs_cache_pressure=50
    echo 'vm.swappiness=10' | tee -a /etc/sysctl.conf
    echo 'vm.vfs_cache_pressure=50' | tee -a /etc/sysctl.conf
fi

# =================================================================
# 10. Настройка SSH
# =================================================================
log_info "10. Настройка SSH..."
sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin no/' /etc/ssh/sshd_config
sed -i 's/PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config
systemctl restart sshd

# =================================================================
# 11. Создание пользователя для деплоя
# =================================================================
log_info "11. Создание пользователя для деплоя..."
useradd -m -s /bin/bash deploy || true
usermod -aG docker deploy
mkdir -p /home/deploy/.ssh
chown -R deploy:deploy /home/deploy/.ssh
chmod 700 /home/deploy/.ssh

# =================================================================
# 12. Настройка прав
# =================================================================
log_info "12. Настройка прав..."
chown -R deploy:deploy ${PROJECT_PATH}
chmod -R 755 ${PROJECT_PATH}

# =================================================================
# 13. Создание systemd службы
# =================================================================
log_info "13. Создание systemd службы..."
cat > /etc/systemd/system/${PROJECT_NAME}.service << 'SERVICE_EOF'
[Unit]
Description=DevOps Vacancy Analytics
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/devops-vacancy-analysis
ExecStart=/usr/local/bin/docker-compose -f docker-compose.prod.yml up -d
ExecStop=/usr/local/bin/docker-compose -f docker-compose.prod.yml down
User=deploy
Group=docker

[Install]
WantedBy=multi-user.target
SERVICE_EOF

systemctl daemon-reload
systemctl enable ${PROJECT_NAME}.service

# =================================================================
# 14. Настройка cron для бэкапов
# =================================================================
log_info "14. Настройка cron для бэкапов..."
cat > /opt/backup.sh << 'BACKUP_EOF'
#!/bin/bash
BACKUP_DIR="/opt/backups"
DATE=$(date +%Y%m%d_%H%M%S)
PROJECT_PATH="/opt/devops-vacancy-analysis"

mkdir -p ${BACKUP_DIR}

# Бэкап базы данных
docker exec -t ${PROJECT_PATH}_postgres_1 pg_dump -U postgres hh_vacancies > ${BACKUP_DIR}/db_backup_${DATE}.sql

# Бэкап данных Grafana
tar -czf ${BACKUP_DIR}/grafana_backup_${DATE}.tar.gz -C ${PROJECT_PATH} grafana_data_prod

# Удаляем старые бэкапы (старше 7 дней)
find ${BACKUP_DIR} -name "*.sql" -mtime +7 -delete
find ${BACKUP_DIR} -name "*.tar.gz" -mtime +7 -delete
BACKUP_EOF

chmod +x /opt/backup.sh

# Добавляем в cron ежедневный бэкап в 3:00
(crontab -l 2>/dev/null; echo "0 3 * * * /opt/backup.sh") | crontab -

# =================================================================
# 15. Завершение
# =================================================================
log_success "Настройка сервера завершена!"
echo ""
echo "=========================================================="
echo "СЕРВЕР ГОТОВ К ДЕПЛОЮ"
echo "=========================================================="
echo "1. Настройте GitHub Secrets в вашем репозитории:"
echo "   - SERVER_HOST: $(curl -s ifconfig.me)"
echo "   - SERVER_USER: deploy"
echo "   - SSH_PRIVATE_KEY: приватный ключ для доступа"
echo "   - SERVER_PROJECT_PATH: ${PROJECT_PATH}"
echo "   - DOCKER_USERNAME: ваш Docker Hub логин"
echo "   - DOCKER_PASSWORD: ваш Docker Hub пароль"
echo ""
echo "2. Добавьте публичный SSH ключ в /home/deploy/.ssh/authorized_keys"
echo ""
echo "3. Склонируйте репозиторий в ${PROJECT_PATH}:"
echo "   git clone ${GITHUB_REPO} ${PROJECT_PATH}"
echo ""
echo "4. Запустите проект:"
echo "   cd ${PROJECT_PATH}"
echo "   docker-compose -f docker-compose.prod.yml up -d"
echo "=========================================================="
