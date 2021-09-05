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
