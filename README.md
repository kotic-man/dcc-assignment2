# Сервис реплицируемых счётчиков (dcc-assignment2)

Это моё решение задания: сервис счётчиков на Python и gRPC. В нём есть безопасные повторы запросов (idempotency key), логические часы Лампорта, три реплики с записью по большинству подтверждений, автоматические тесты, тесты сбоев и замер задержек.

## 1. Установка (Windows PowerShell)

Сначала я создаю виртуальное окружение, включаю его и ставлю нужные пакеты:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install grpcio grpcio-tools pytest
```

На Linux или macOS окружение включается командой `source .venv/bin/activate`.

Файлы `counter_pb2.py` и `counter_pb2_grpc.py` сгенерированы из `counter.proto` и уже лежат в репозитории. Если нужно создать их заново:

```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
```

## 2. Запуск одной реплики

Запускаю сервер (реплику) в первом терминале:

```powershell
python server.py --port 50051 --name replica-A --log logs\replica-A.log
```

Необязательные флаги: `--delay-ms N` (искусственная задержка ответа), `--fault none|crash-after-recv|slow-first` (имитация сбоев для тестов), `--quiet` (не печатать события в консоль).

Клиент запускается во втором терминале:

```powershell
python client.py incr likes:post-42 --by 5
python client.py get likes:post-42
python client.py incr likes:post-42 --by 5 --key abc    # выполнить дважды: во второй раз будет duplicate: yes
python client.py --name client-1 --log logs\client-1.log incr likes:post-42 --by 1
```

Первая команда увеличивает счётчик, вторая читает его значение. Третья показывает защиту от дублей: при повторе с тем же ключом значение не меняется. Четвёртая записывает события клиента в лог с временем Лампорта.

## 3. Запуск трёх реплик (режим кворума)

Запускаю три реплики одной командой в первом терминале:

```powershell
python run_replicas.py 50051 50052 50053
```

Клиент во втором терминале:

```powershell
python client.py --targets localhost:50051,localhost:50052,localhost:50053 incr likes:post-42 --by 1
python client.py --targets localhost:50051,localhost:50052,localhost:50053 get likes:post-42
```

Запись считается успешной, если её подтвердили минимум 2 из 3 реплик (в выводе `acks: 2/3`). Если подтверждений меньше, клиент пишет `FAILED: quorum not reached`. Чтобы проверить падение реплики, я запускаю только два порта, например `python run_replicas.py 50051 50052`. Чтение идёт только с одной реплики.

## 4. Тесты

Все тесты запускаются одной командой. Серверы они поднимают и останавливают сами, вручную ничего делать не нужно. Ожидаемый результат: `14 passed`.

```powershell
python -m pytest -v
```

Тесты по отдельным файлам:

```powershell
python -m pytest tests\test_counter.py -v     # задания A2, A3, C2
python -m pytest tests\test_clocks.py -v      # задание B1
python -m pytest tests\test_failures.py -v    # задание C3 (запускает процессы реплик, пишет логи в logs\failures\)
```

Один конкретный тест: `python -m pytest -v -k test_majority_commit_two_acks`

## 5. Сценарий с часами Лампорта (задание B2)

В первом терминале запускаю реплику:

```powershell
python server.py --port 50051 --name replica-A --log logs\replica-A.log
```

Во втором терминале запускаю сценарий (старые логи перед этим удаляю командой `Remove-Item logs\*.log`):

```powershell
python scenario_b2.py
Get-Content logs\client-1.log, logs\client-2.log, logs\client-3.log, logs\replica-A.log | Out-File -Encoding utf8 logs\b2_trace.txt
```

Собранный общий лог `logs\b2_trace.txt` я использую в отчёте.

## 6. Замер производительности (задание C4)

```powershell
python tests\perf_benchmark.py
```

Скрипт выводит таблицу с медианой и p95 задержки (по 2000 запросов на каждую конфигурацию: одна реплика и кворум, 1 и 16 клиентов).

## 7. Структура проекта

| Путь                               | Что это                                                                          |
| ---------------------------------- | -------------------------------------------------------------------------------- |
| `counter.proto`, `counter_pb2*.py` | описание сервиса и сгенерированный код                                           |
| `server.py`                        | реплика (флаги: `--port`, `--fault`, `--delay-ms`, `--name`, `--log`, `--quiet`) |
| `client.py`                        | клиенты (одна реплика и кворум), команды `incr` и `get`, повторы запросов        |
| `clocks.py`                        | часы Лампорта и журнал событий                                                   |
| `run_replicas.py`                  | запуск нескольких реплик одной командой                                          |
| `scenario_b2.py`                   | сценарий с тремя клиентами для задания B2                                        |
| `tests/`                           | `test_counter.py`, `test_clocks.py`, `test_failures.py`, `perf_benchmark.py`     |
| `logs/`                            | сохранённые логи (трасса B2 и логи тестов сбоев)                                 |
| `notes/`                           | данные об окружении и сырые результаты для отчёта                                |
| `report.pdf`                       | отчёт о тестировании                                                             |
