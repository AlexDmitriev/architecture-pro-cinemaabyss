#!/bin/bash

# Извлекаем хост:порт из URL переменных
export MONOLITH_HOST=$(echo ${MONOLITH_URL} | sed 's|http://||' | sed 's|https://||')
export MOVIES_SERVICE_HOST=$(echo ${MOVIES_SERVICE_URL} | sed 's|http://||' | sed 's|https://||')
export EVENTS_SERVICE_HOST=$(echo ${EVENTS_SERVICE_URL} | sed 's|http://||' | sed 's|https://||')

# Подставляем переменные окружения в конфиг nginx
envsubst '${PORT} ${MONOLITH_URL} ${MONOLITH_HOST} ${MOVIES_SERVICE_URL} ${MOVIES_SERVICE_HOST} ${EVENTS_SERVICE_URL} ${EVENTS_SERVICE_HOST} ${GRADUAL_MIGRATION} ${MOVIES_MIGRATION_PERCENT}' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf

# Запускаем nginx
exec "$@"