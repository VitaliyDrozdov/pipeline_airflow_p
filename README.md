
<h1 align="center"> Airflow pipeline с параллельными задачами </h1>


<hr>

</p>
<h2 align="center">


### Используемый стек:<a name="stack"></a>

- Python
- Pandas
- Airflow

Инструменты разработки:
- Poetry
- Pre-commit


## 📍 Описание

<p>
Тестовый pipeline для запуска нескольких задач параллельно.
</p>

### Структура репозитория <a name="architecture"></a>

| Директория    | Описание                                                |
|---------------|---------------------------------------------------------|
| `dags`       | Файлы c DAG airflow                                      |
| `dags/data` | Папка с исходными файлами                                 |
| `dags/funcs`     | Вспомогательные функции                               |
| `./requirements_dev.txt`     | Зависимости для разработки               |
| `./requirements.txt`     | Зависимости для docker контейнера            |



<h2 align="center">

## 🚀 Запуск

</h2>

<p>


- В папке с проектом запустить Docker контейнеры:

```text
docker compose up
```

</p>


Airflow будет доступен по адресу:
```text
https://localhost:8080/
```

По-умолчанию данные для входа:

login: airflow

password: airflow


## Автор :

[VitaliyDrozdov](https://github.com/VitaliyDrozdov)
