WITH dhedge_v2_pools AS (
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
        to AS fund_address
    FROM
        polygon."transactions",
        dhedge_v2_pools
    WHERE
        success = 'true'
        AND "to" IN (dhedge_v2_pools.fund_address)
)
SELECT
    COUNT(account) as investors_per_fund,
    fund_address
FROM
    investors
ORDER BY
    investors_per_fund
