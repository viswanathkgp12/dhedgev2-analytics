WITH dhedge_v2_pools AS (
    -- get dhedge pool v2 addresses
    SELECT
        DISTINCT output_0 AS fund_address,
        e."_supportedAssets" as supported_assets,
        e."_fundName" as fund_name
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
        block_number,
        index
    FROM
        (
            SELECT
                DISTINCT dhedge_v2_pools.fund_name as fund_name,
                contract_address,
                bytea2numeric(substring(data, 225, 32)) / 1e18 as fund_value,
                block_number,
                index,
                Row_Number() over (
                    Partition by contract_address
                    ORDER By
                        block_number DESC,
                        index DESC
                ) RN
            FROM
                polygon.logs,
                dhedge_v2_pools
            WHERE
                contract_address = dhedge_v2_pools.fund_address
                AND topic1 = '\x97e6c213c123075e233a6f2323f33d8319141b993ab05e9e2f7eb2eda08cb944' -- Deposit event topic
        ) e
    WHERE
        e.RN <= 1
),
pool_withdrawals AS (
    SELECT
        fund_name,
        contract_address,
        fund_value,
        block_number,
        index
    FROM
        (
            SELECT
                DISTINCT dhedge_v2_pools.fund_name as fund_name,
                contract_address,
                bytea2numeric(substring(data, 161, 32)) / 1e18 as fund_value,
                block_number,
                index,
                Row_Number() over (
                    Partition by contract_address
                    ORDER By
                        block_number DESC,
                        index DESC
                ) RN
            FROM
                polygon.logs,
                dhedge_v2_pools
            WHERE
                contract_address = dhedge_v2_pools.fund_address
                AND topic1 = '\xfad3d7f9ed107ffa7fc8ce8baa521effc3650ec48a4d1dd36bdb9c4b91db1295' -- Withdrawal event topic
        ) e
    WHERE
        e.RN <= 1
),
pool_holdings AS (
    -- get pool_holdings
    SELECT
        fund_name,
        contract_address,
        fund_value,
        block_number
    FROM
        (
            SELECT
                fund_name,
                contract_address,
                fund_value,
                block_number,
                Row_Number() over (
                    Partition by contract_address
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
            ORDER BY
                contract_address,
                block_number DESC,
                index DESC
        ) e
    WHERE
        e.RN <= 1
)
SELECT
    sum(fund_value) as "Total Value Locked(Cumulative)"
FROM
    pool_holdings
