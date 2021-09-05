-- get dhedge pool v2 unique manager addresses count
SELECT
    COUNT(DISTINCT(manager)) AS "Unique managers(cumulative)"
FROM
    (
        SELECT
            DISTINCT output_0 AS fund_address,
            e."_manager" AS manager
        FROM
            dhedge_v2."PoolFactory_call_createFund" e
        WHERE
            call_success = true
    ) t
