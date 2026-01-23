# Используем официальный образ nginx с alpine для минимального размера
FROM nginx:alpine

# Устанавливаем envsubst для подстановки переменных окружения в конфиг
RUN apk add --no-cache bash gettext

# Удаляем дефолтный конфиг nginx
RUN rm /etc/nginx/nginx.conf /etc/nginx/conf.d/default.conf

# Копируем нашу конфигурацию nginx
COPY nginx.conf.template /etc/nginx/nginx.conf.template
COPY entrypoint.sh /entrypoint.sh

# Делаем entrypoint исполняемым
RUN chmod +x /entrypoint.sh

# Экспонируем порт
EXPOSE 8000

# Используем entrypoint для подстановки переменных окружения
ENTRYPOINT ["/entrypoint.sh"]
CMD ["nginx", "-g", "daemon off;"]