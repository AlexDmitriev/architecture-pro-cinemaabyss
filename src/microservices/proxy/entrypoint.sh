#!/bin/bash

# Извлекаем хост:порт из URL переменных
export MONOLITH_HOST=$(echo ${MONOLITH_URL} | sed 's|http://||' | sed 's|https://||')
export MOVIES_SERVICE_HOST=$(echo ${MOVIES_SERVICE_URL} | sed 's|http://||' | sed 's|https://||')
export EVENTS_SERVICE_HOST=$(echo ${EVENTS_SERVICE_URL} | sed 's|http://||' | sed 's|https://||')

# Если gradual migration выключен, устанавливаем процент в 100 (все запросы на movies-service)
if [ "${GRADUAL_MIGRATION}" != "true" ]; then
    export MOVIES_MIGRATION_PERCENT="100"
fi

# Обрабатываем разные случаи MOVIES_MIGRATION_PERCENT
# 0% - всегда монолит (split_clients не используется, так как nginx не принимает 0%)
# 100% - всегда микросервис (split_clients не используется, так как nginx не принимает 100%)
# 1-99% - используем split_clients для распределения трафика
if [ "${MOVIES_MIGRATION_PERCENT}" = "0" ]; then
    export SPLIT_CLIENTS_CONFIG="# Split clients отключен, так как MOVIES_MIGRATION_PERCENT=0 (всегда монолит)"
    export MOVIES_MIGRATION_INIT="set \$movies_migration ${MONOLITH_URL};"
elif [ "${MOVIES_MIGRATION_PERCENT}" = "100" ]; then
    export SPLIT_CLIENTS_CONFIG="# Split clients отключен, так как MOVIES_MIGRATION_PERCENT=100 (всегда микросервис)"
    export MOVIES_MIGRATION_INIT="set \$movies_migration ${MOVIES_SERVICE_URL};"
else
    # Для процентов от 1 до 99 используем split_clients
    # Важно: не экранируем переменные окружения (MOVIES_MIGRATION_PERCENT, MOVIES_SERVICE_URL, MONOLITH_URL),
    # но экранируем переменные nginx (\$movies_migration, \${remote_addr} и т.д.)
    export SPLIT_CLIENTS_CONFIG="split_clients \"\${remote_addr}\${http_user_agent}\${request_uri}\${request_method}\${args}\${time_iso8601}\" \$movies_migration {
        ${MOVIES_MIGRATION_PERCENT}% ${MOVIES_SERVICE_URL};
        * ${MONOLITH_URL};
    }"
    export MOVIES_MIGRATION_INIT="# Переменная \$movies_migration устанавливается через split_clients"
fi

# Подставляем переменные окружения в конфиг nginx
envsubst '${PORT} ${MONOLITH_URL} ${MONOLITH_HOST} ${MOVIES_SERVICE_URL} ${MOVIES_SERVICE_HOST} ${EVENTS_SERVICE_URL} ${EVENTS_SERVICE_HOST} ${GRADUAL_MIGRATION} ${MOVIES_MIGRATION_PERCENT} ${SPLIT_CLIENTS_CONFIG} ${MOVIES_MIGRATION_INIT}' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf

# Запускаем nginx
exec "$@"