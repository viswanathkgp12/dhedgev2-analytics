WITH day_series AS (
    SELECT
        generate_series(
            (date '28-JUL-2021') :: date,
            now() :: date,
            interval '1 day'
        ) AS day
),
dhedge_v2_pools AS (
    -- get dhedge pool v2 addresses
    SELECT
        DISTINCT output_0 AS fund_address
    FROM
        dhedge_v2."PoolFactory_call_createFund"
    WHERE
        call_success = true
),
investors AS (
    -- get unique investors
    SELECT
        DISTINCT "from" AS account,
        date_trunc('day', block_time) AS day
    FROM
        polygon."transactions",
        dhedge_v2_pools
    WHERE
        success = 'true'
        AND "to" IN (dhedge_v2_pools.fund_address)
),
investors_min_dt AS (
    -- count unique investors on particular day
    SELECT
        min(d.day) AS day,
        i.account AS investor
    FROM
        day_series d
        LEFT OUTER JOIN investors i ON i.day = d.day
    WHERE
        d.day >= '28-JUL-2021'
    GROUP BY
        i.account
),
D1_temp AS (
    SELECT
        day,
        investor
    FROM
        investors_min_dt
    WHERE
        CAST(day AS DATE) = CAST((now() - interval '2 day') AS DATE)
),
D7_temp AS (
    SELECT
        day,
        investor
    FROM
        investors_min_dt
    WHERE
        CAST(day AS DATE) > CAST((now() - interval '7 day') AS DATE)
),
D30_temp AS (
    SELECT
        day,
        investor
    FROM
        investors_min_dt
    WHERE
        CAST(day AS DATE) > CAST((now() - interval '30 day') AS DATE)
),
D1 AS (
    SELECT
        1 AS Counter,
        count(DISTINCT investor) AS total_investors_1d
    FROM
        D1_temp
    GROUP BY
        day
),
D7 AS (
    SELECT
        1 AS Counter,
        count(DISTINCT investor) AS total_investors_7d
    FROM
        D7_temp
),
D30 AS (
    SELECT
        1 AS Counter,
        count(DISTINCT investor) AS total_investors_30d
    FROM
        D30_temp
)
SELECT
    total_investors_1d,
    total_investors_7d,
    total_investors_30d
FROM
    (
        (
            D1
            INNER JOIN D7 ON D7.Counter = D1.Counter
        )
        INNER JOIN D30 ON D30.Counter = D1.Counter
    )
