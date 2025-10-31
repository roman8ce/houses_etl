# houses_etl

Проект по сбору и обработке данных рынка недвижимости с использованием современного стека технологий.

## Обзор архитектуры

<img width="6774" height="1784" alt="houses_etl" src="https://github.com/user-attachments/assets/ffa60191-c6cc-4dc4-9d45-b94acf756067" />

- Сбор данных с сайтов недвижимости через API (Yandex Disk)
- Обработка данных с помощью Apache Spark
- Хранение данных в S3-хранилище MinIO
- Витрина данных в Clickhouse
- Оркестрация процессов через Apache Airflow

## Используемые технологии

- **Python**: Основной язык программирования
- **Apache Airflow**: Оркестрация рабочих процессов
- **Apache Spark**: Обработка больших данных
- **ClickHouse**: Хранение и аналитика данных
- **Docker**: Контейнеризация
- **MinIO**: Обработка больших данных

## Структура проекта

```
houses_etl/
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
├── spark/
│   └── Dockerfile
├── clickhouse/
│   └── init/
│       └── init.sql
├── docker-compose.yml
└── README.md
```

## Установка и запуск

1. Клонируйте репозиторий:
```bash
git clone <repository-url>
cd houses_etl
```

2. Создайте файл `.env` с необходимыми переменными окружения

3. Запустите сервисы:
```bash
docker-compose up -d
```

4. Доступ к сервисам:

| Сервис     | URL                   |
|------------|-----------------------|
| Airflow    | http://localhost:8080 |
| ClickHouse | http://localhost:8123 |
| MinIO      | http://localhost:9003 |
| Spark UI   | http://localhost:9090 |

## Порты сервисов

| Сервис       | Порт |
|--------------|------|
| Airflow      | 8080 |
| ClickHouse   | 8123 |
| MinIO        | 9003 |
| Spark Master | 7077 |
| Spark UI     | 9090 |
