WITH dhedge_v2_pools AS (
    -- get dhedge pool v2 addresses
    SELECT
        DISTINCT COALESCE('0x' || ENCODE(output_0, 'hex')) as fund_address,
        e."_fundName" as fund_name,
        e."_fundSymbol" as fund_symbol,
        e."_manager" as manager,
        e."_managerName" as manager_name,
        e."_supportedAssets" as supported_assets
    FROM
        dhedge_v2."PoolFactory_call_createFund" e
    WHERE
        call_success = true
),
pool_wise_assets AS (
    SELECT
        1 AS Counter,
        jsonb_agg(p.asset) as assets,
        p.pool_address
    FROM
        (
            SELECT
                asset,
                dhedge_v2_pools.fund_address as pool_address
            FROM
                dhedge_v2_pools,
                jsonb_to_recordset(dhedge_v2_pools.supported_assets) as x(asset text)
        ) p
    GROUP BY
        p.pool_address
    ORDER BY
        p.pool_address ASC
)
SELECT
    jsonb_agg(el) AS assets
FROM
    pool_wise_assets p,
    jsonb_array_elements(p.assets) AS el
GROUP BY
    Counter
