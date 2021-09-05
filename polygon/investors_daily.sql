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
)
SELECT
    day,
    -- get daily unique investors
    COUNT(DISTINCT investor) AS count
FROM
    investors_min_dt
GROUP BY
    day
ORDER BY
    day desc
