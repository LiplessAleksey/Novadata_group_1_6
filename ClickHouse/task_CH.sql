SELECT * FROM system.settings WHERE name like '%ttl%';

/*
 Создаём таблицу сырых событий пользователей.
 Сохраняются user_id, тип события, потраченные баллы и время события.
 Храним данные 30 дней, по TTL. Партицируем по месяцам (YYYYMM).
 */
CREATE TABLE user_events (
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, user_id)
TTL event_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

/*
 Проверка, какие партиции и TTL-интервалы сейчас активны у таблицы user_events
 */
SELECT
    table,
    partition,
    name AS part_name,
    active,
    delete_ttl_info_min,
    delete_ttl_info_max,
    engine
FROM system.parts
WHERE table = 'user_events';

/*
 Создаём агрегированную таблицу logs_agg для хранения:
  - уникальных пользователей (AggregateFunction(uniq))
  - общей суммы баллов (AggregateFunction(sum))
  - количества действий (AggregateFunction(count))
 Храним агрегаты 180 дней.
 */
CREATE TABLE logs_agg (
    event_type String,
    event_date Date,
    uniq_users AggregateFunction(uniq, UInt32),
    total_points_spent AggregateFunction(sum, UInt32),
    event_count AggregateFunction(count, UInt32)
)
ENGINE = AggregatingMergeTree()
ORDER BY (event_type, event_date)
TTL event_date + INTERVAL 180 DAY;

/*
 Создаём материализованное представление, которое:
  - Автоматически обновляет агрегированную таблицу logs_agg при вставке в user_events
  - Использует state-функции: uniqState, sumState, countState
 */
CREATE MATERIALIZED VIEW mv_logs_agg TO logs_agg
AS SELECT
    event_type,
    toDate(event_time) AS event_date,
    uniqState(user_id) AS uniq_users,
    sumState(points_spent) AS total_points_spent,
    countState(*) AS event_count
FROM user_events
GROUP BY event_type, toDate(event_time);

/*
 Просмотр агрегированных данных (результаты из logs_agg)
 Используем merge-функции, чтобы получить итоговые значения из state.
 */
SELECT
    event_type,
    event_date,
    uniqMerge(uniq_users) AS uniq_users,
    sumMerge(total_points_spent) AS total_points_spent,
    countMerge(event_count) AS event_count
FROM logs_agg
GROUP BY event_type, event_date
ORDER BY event_date, event_type;

/*
 Проверка на мусорные значения event_time (например, случайные нули)
 Нужна, чтобы выявить строки, где event_time = '1970-01-01'
 */
SELECT *
FROM user_events
WHERE event_time < toDateTime('2000-01-01');

/*
 Расчёт Retention: сколько пользователей вернулись в течение 7 дней после первого события
 Формат: total_users_day_0 | returned_in_7_days | retention_7d_percent
 */
SELECT
    d0.event_date AS day0,
    countDistinct(d0.user_id) AS total_users_day_0,
    countDistinct(d7.user_id) AS returned_in_7_days,
    round(countDistinct(d7.user_id) / countDistinct(d0.user_id) * 100, 2) AS retention_7d_percent
FROM
    (
        SELECT toDate(event_time) AS event_date, user_id
        FROM user_events
    ) AS d0
LEFT JOIN
    (
        SELECT toDate(event_time) AS event_date, user_id
        FROM user_events
    ) AS d7
ON d0.user_id = d7.user_id
   AND d7.event_date BETWEEN d0.event_date + 1 AND d0.event_date + 7
GROUP BY day0
ORDER BY day0;


/*
 Запрос для вставки тестовых данных
 */
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),

(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),

(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),

(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),

(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),

(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

/*
  Проверка: что сейчас есть в таблице сырых логов
 */
select * from user_events ue;

/*
 Проверка: что хранится в агрегированной таблице (пока в виде state)
 */
select * from logs_agg la;

/*
 Быстрая аналитика по дням: итоговая метрика по каждой дате и типу события
 Используются 3 merge-функции (выполняем условие задания!)
 */
SELECT
    event_date,
    event_type,
    uniqMerge(uniq_users) AS unique_users,
    sumMerge(total_points_spent) AS total_spent,
    countMerge(event_count) AS total_actions
FROM logs_agg
GROUP BY event_date, event_type
ORDER BY event_date, event_type;