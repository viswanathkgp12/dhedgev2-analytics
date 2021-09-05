WITH day_series AS (
    SELECT
        generate_series(
            (date '28-JUL-2021') :: date,
            now() :: date,
            interval '1 day'
        ) AS date
),
dhedge_v2_pools AS (
    -- get dhedge pool v2 addresses
    SELECT
        DISTINCT output_0 AS fund_address,
        e."_fundName" AS fund_name,
        e."_supportedAssets" as supported_assets
    FROM
        dhedge_v2."PoolFactory_call_createFund" e
    WHERE
        call_success = true
),
pool_deposits AS (
    SELECT
        fund_name,
        contract_address,
        fund_value,
        date,
        block_number,
        index
    FROM
        (
            SELECT
                *,
                Row_Number() over (
                    Partition by contract_address,
                    date
                    ORDER By
                        block_number DESC,
                        index DESC
                ) RN
            FROM
                (
                    SELECT
                        DISTINCT dhedge_v2_pools.fund_name as fund_name,
                        contract_address,
                        bytea2numeric(substring(data, 225, 32)) / 1e18 as fund_value,
                        DATE_TRUNC('day', block_time) as date,
                        block_number,
                        index
                    FROM
                        polygon.logs,
                        dhedge_v2_pools
                    WHERE
                        contract_address = dhedge_v2_pools.fund_address
                        AND topic1 = '\x97e6c213c123075e233a6f2323f33d8319141b993ab05e9e2f7eb2eda08cb944' -- Deposit event topic
                ) e
        ) p
    WHERE
        p.RN <= 1
),
pool_withdrawals AS (
    SELECT
        fund_name,
        contract_address,
        fund_value,
        date,
        block_number,
        index
    FROM
        (
            SELECT
                *,
                Row_Number() over (
                    Partition by contract_address,
                    date
                    ORDER By
                        block_number DESC,
                        index DESC
                ) RN
            FROM
                (
                    SELECT
                        DISTINCT dhedge_v2_pools.fund_name as fund_name,
                        contract_address,
                        bytea2numeric(substring(data, 161, 32)) / 1e18 as fund_value,
                        DATE_TRUNC('day', block_time) as date,
                        block_number,
                        index
                    FROM
                        polygon.logs,
                        dhedge_v2_pools
                    WHERE
                        contract_address = dhedge_v2_pools.fund_address
                        AND topic1 = '\xfad3d7f9ed107ffa7fc8ce8baa521effc3650ec48a4d1dd36bdb9c4b91db1295' -- Withdrawal event topic
                ) e
        ) p
    WHERE
        p.RN <= 1
),
pool_holdings AS (
    SELECT
        fund_name,
        contract_address,
        fund_value,
        date
    FROM
        (
            SELECT
                *,
                Row_Number() over (
                    Partition by contract_address,
                    date
                    ORDER By
                        block_number DESC,
                        index DESC
                ) RN
            FROM
                (
                    SELECT
                        *
                    FROM
                        pool_deposits
                    UNION ALL
                    SELECT
                        *
                    FROM
                        pool_withdrawals
                ) e
        ) p
    WHERE
        p.RN <= 1
    ORDER BY
        contract_address,
        block_number ASC,
        index ASC
),
pool_holdings_with_next AS (
    SELECT
        *,
        LEAD(date) OVER (PARTITION BY contract_address) AS next_available_date
    FROM
        pool_holdings
),
pool_holdings_daily AS (
    SELECT
        fund_name,
        fund_value,
        day_series.date AS date
    FROM
        pool_holdings_with_next
        JOIN day_series ON day_series.date BETWEEN pool_holdings_with_next.date
        AND (
            CASE WHEN pool_holdings_with_next.next_available_date - INTERVAL '1 day' IS NULL THEN now() :: date ELSE pool_holdings_with_next.next_available_date - INTERVAL '1 day' END
        )
    ORDER BY
        contract_address,
        day_series.date ASC
)
SELECT
    sum(fund_value) AS tvl,
    date
FROM
    pool_holdings_daily
GROUP BY
    date
ORDER BY
    date
