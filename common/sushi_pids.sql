CREATE OR REPLACE FUNCTION dune_user_generated.sushi_minichef_pid_to_address (pid bytea)
    RETURNS BYTEA
AS $$
DECLARE
    lp_address bytea;
BEGIN
    WITH sushi_minichef_pids AS (
        SELECT
            topic2 AS pool_id,
            SUBSTRING(topic3, 13, 20) AS lp_token_address
        FROM
            polygon.logs
        WHERE
            contract_address = '\x0769fd68dFb93167989C6f7254cd0D766Fb2841F'
            AND topic1 = '\x81ee0f8c5c46e2cb41984886f77a84181724abb86c32a5f6de539b07509d45e5'
    )

    SELECT lp_token_address INTO lp_address
    FROM sushi_minichef_pids
    WHERE pool_id=pid;
    return lp_address;
END
$$ LANGUAGE 'plpgsql';