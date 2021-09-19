CREATE OR REPLACE FUNCTION dune_user_generated.sushi_lp_price (pair_address bytea)
    RETURNS DECIMAL
AS $$
DECLARE
    lp_token_price DECIMAL;
BEGIN
    WITH asset_prices_found AS (
        SELECT
            DISTINCT(p.symbol) AS symbol,
            minute as block_time,
            price as exchange_rate_usd
        FROM
            prices."usd" p,
            dune_user_generated.view_dhedgev2_assets d
        WHERE
            minute = date_trunc('minute', NOW()) - INTERVAL '15 minutes'
            AND p.symbol IN (
                SELECT
                    d.symbol
                FROM
                    dune_user_generated.view_dhedgev2_assets d
            )
    ),
    prices AS (
        SELECT
            *
        FROM
            asset_prices_found
        UNION ALL
        SELECT
            symbol,
            now() AS block_time,
            CASE WHEN symbol = 'USDT' THEN 1 WHEN symbol = 'DAI' THEN 1 END as exchange_rate_usd
        FROM
            dune_user_generated.view_dhedgev2_assets d
        WHERE
            symbol NOT IN (
                SELECT
                    symbol
                FROM
                    asset_prices_found
            )
            AND symbol IN (
                SELECT
                    symbol
                FROM
                    dune_user_generated.view_dhedgev2_assets v
            )
    ),
    sushi_pair AS (
        -- Get UNISWAP pair information
        SELECT
            pair,
            token0,
            token1
        FROM
            sushi."UniswapV2Factory_evt_PairCreated"
        WHERE
            pair = pair_address --enter uniswap LP token address here
    ),
    token0 AS (
        SELECT
            decimals,
            symbol
        FROM
            dune_user_generated.view_dhedgev2_assets
        WHERE
            token_address = (
                SELECT
                    token0
                FROM
                    sushi_pair
            )
    ),
    token1 AS (
        SELECT
            decimals,
            symbol
        FROM
            dune_user_generated.view_dhedgev2_assets d
        WHERE
            token_address = (
                SELECT
                    token1
                FROM
                    sushi_pair
            )
    ),
    token0_price AS (
        SELECT
            1 AS Counter,
            exchange_rate_usd
        FROM
            prices
        WHERE
            symbol = (
                SELECT
                    symbol
                FROM
                    token0
            )
    ),
    token1_price AS (
        SELECT
            1 AS Counter,
            exchange_rate_usd
        FROM
            prices
        WHERE
            symbol = (
                SELECT
                    symbol
                FROM
                    token1
            )
    ),
    supply AS (
        -- get total supply
        select
            1 AS Counter,
            output_0 / 1e18 AS supply
        FROM
            sushi."UniswapV2Pair_call_totalSupply"
        WHERE
            contract_address = pair_address
        ORDER BY
            call_block_number DESC
        LIMIT
            1
    ), reserves AS (
        --get reserves per token
        SELECT
            1 AS Counter,
            reserve0 / 10 ^ (
                SELECT
                    decimals
                FROM
                    token0
            ) AS reserve0,
            reserve1 / 10 ^ (
                SELECT
                    decimals
                FROM
                    token1
            ) AS reserve1
        FROM
            sushi."UniswapV2Pair_evt_Sync"
        WHERE
            contract_address = pair_address AND
            evt_block_number > (
                SELECT block_number - 10000
                FROM
                    polygon.logs
                ORDER BY
                    block_number DESC
                LIMIT 1
            )
        ORDER BY
            evt_block_number DESC
        LIMIT
            1
    )
    SELECT
        2 * sqrt(reserve0 * reserve1) * sqrt(tp_0.exchange_rate_usd * tp_1.exchange_rate_usd) / supply INTO lp_token_price
    FROM
        reserves r
        JOIN supply s ON s.Counter = r.Counter
        JOIN token0_price tp_0 ON tp_0.Counter = r.Counter
        JOIN token1_price tp_1 ON tp_1.Counter = r.Counter;
    return lp_token_price;
END
$$ LANGUAGE 'plpgsql';

SELECT * FROM dune_user_generated.sushi_lp_price('\x116ff0d1caa91a6b94276b3471f33dbeb52073e7')