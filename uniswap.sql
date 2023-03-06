-- uniswap."UNI_evt_DelegateChanged"
-- uniswap."UNI_evt_DelegateVotesChanged"
-- uniswap."UNI_evt_Transfer"

-- uni holders count
WITH transfer AS (
    SELECT
        evt_tx_hash AS tx_hash,
        "from" AS address,
        -amount AS amount
    FROM uniswap."UNI_evt_Transfer" AS tr
    WHERE contract_address = '\x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'
    
UNION ALL

    SELECT
        evt_tx_hash AS tx_hash,
        "to" AS address,
        amount AS amount
    FROM uniswap."UNI_evt_Transfer" AS tr
    WHERE contract_address = '\x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'
),

transferAmount AS (
    SELECT
        address,
        sum(amount)/1e18 as poolholdings
    FROM transfer
    GROUP BY address
)

SELECT sum(poolholdings)
FROM transferAmount
where address != '\x0000000000000000000000000000000000000000'
;





-- uniswap holder distribution (top 1k uni holders)

WITH transfer AS (
    SELECT
        evt_tx_hash AS tx_hash,
        "from" AS address,
        -value AS amount,
        contract_address
    FROM erc20."ERC20_evt_Transfer" AS tr
    WHERE contract_address = '\x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'
    
UNION ALL

    SELECT
        evt_tx_hash AS tx_hash,
        "to" AS address,
        value AS amount,
        contract_address
    FROM erc20."ERC20_evt_Transfer" AS tr
    WHERE contract_address = '\x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'
),

transferAmount AS (
    SELECT
        address,
        sum(amount)/1e18 as poolholdings
    FROM transfer
    GROUP BY address
)

SELECT address, poolholdings
FROM transferAmount
ORDER BY poolholdings DESC
LIMIT 1000
;

-- Uniswap v2: UNI Delegated Over Time
select
date_trunc('week', evt_block_time) as week,
sum("newBalance" - "previousBalance")/1e18 as net_delegated,
sum(sum("newBalance" - "previousBalance")/ 1e18) over (ORDER BY date_trunc('week', evt_block_time)) as total_delegated
from uniswap."UNI_evt_DelegateVotesChanged"
GROUP BY 1 
order by 1 desc 
;


-- Proposals
with votes as
(
SELECT  
    voter, 
    CASE WHEN support = TRUE THEN votes/ 10^18 END as votes_for,
    CASE WHEN support = FALSE THEN votes/ 10^18 END as votes_against, 
    CASE WHEN support = TRUE THEN 1 END as voter_for,
    CASE WHEN support = FALSE THEN 1 END as voter_against,
    "proposalId", 
    contract_address
FROM uniswap_v2."GovernorAlpha_evt_VoteCast" 
UNION ALL
SELECT  
    voter, 
    CASE WHEN support = 1 THEN votes/ 10^18 END as votes_for,
    CASE WHEN support = 0 THEN votes/ 10^18 END as votes_against, 
    CASE WHEN support = 1 THEN 1 END as voter_for,
    CASE WHEN support = 0 THEN 1 END as voter_against,
    "proposalId", contract_address
FROM uniswap_v3."GovernorBravoDelegate_evt_VoteCast" 
)

,proposals as 
(
    SELECT 
        CASE "startBlock" 
            WHEN 11042288 THEN '0.1'
            WHEN 11120865 THEN '0.2'
            WHEN 11473815 THEN '0.3'
            WHEN 12563485 THEN '0.4'
            WHEN 12620175 THEN '0.5'
            WHEN 12686657 THEN '1.1'
            WHEN 13020266 THEN '1.2'
        END as "real ID",
        id as internal_id,
        contract_address,
        proposer,
        description
    FROM uniswap_v2."GovernorAlpha_evt_ProposalCreated"
    UNION ALL
    SELECT 
        CASE 
            WHEN id < 10 THEN  '2.0' || id
            ELSE '2.' || id
        END as "real ID",
        id as internal_id,
        contract_address,
        proposer,
        description
    FROM uniswap_v3."GovernorBravoDelegate_evt_ProposalCreated"
    WHERE id > 8
)

SELECT 
    '<a href="https://app.uniswap.org/#/vote/' || LEFT("real ID", 1) || '/' || TRIM( LEADING '0' FROM REPLACE(RIGHT("real ID", 2),'.','')) ||'?chain=mainnet">' || "real ID" || '</a>' as "ID & Link",
    clearname as "Proposer",
    SUM(votes_for) + SUM(votes_against) as "Total Votes",
    SUM(votes_for) as "Votes FOR",
    SUM(votes_against) as "Votes AGAINST",
    SUM(votes_against)::float / (SUM(votes_for) + SUM(votes_against)) as "Percentage Votes Against",
    SUM(voter_for) + SUM(voter_against) as "Total # Voters",
    SUM(voter_for) as "# Voters FOR",
    SUM(voter_against) as "# Voters AGAINST",
    SUM(voter_against)::float / (SUM(voter_for) + SUM(voter_against)) as "Percentage Voters Against",
    LEFT(description, 100) || '....' as Title
FROM proposals p
LEFT JOIN votes v
    ON p.internal_id = v."proposalId" AND p.contract_address = v.contract_address
LEFT JOIN dune_user_generated.Uni_DelegateesMapping del
    ON p.proposer::text = del.delegatee
GROUP BY "real ID", description, clearname

;

-- Uniswap monthly active users
SELECT
    ssq.time, 
    new_users as "New",
    (unique_users - new_users) as "Old"
FROM (
    SELECT
        sq.time, 
        COUNT(*) AS new_users
    FROM (
        SELECT 
            tx_from as unique_users,
            MIN(date_trunc('month', block_time)) AS time
        FROM dex.trades
        WHERE project = 'Uniswap'
        GROUP BY 1
        ORDER BY 1
    ) sq
    GROUP BY 1
) ssq
LEFT JOIN (
        SELECT 
            date_trunc('month', block_time) AS time,
            COUNT(DISTINCT tx_from) AS unique_users
        FROM dex.trades
        WHERE project = 'Uniswap' 
        GROUP BY 1
        ORDER BY 1
) t2 ON t2.time = ssq.time
ORDER BY 1 DESC

;

-- uni treasury over time
/*
    --- Uniswap Treasury ---

    Wallet / Address
    'Uniswap Treasury'  0x1a9C8182C09F50C8318d769245beA52c32BE35BC

*/
-- WITH wallets AS (
--     SELECT
--         'UNISWAP' AS org, '\x1a9C8182C09F50C8318d769245beA52c32BE35BC'::bytea AS address, 'Uniswap Treasury' AS wallet
-- )

-- , creation_days AS (
--     SELECT
--         date_trunc('day', block_time) AS day
--     FROM ethereum.traces
--     WHERE address IN (SELECT address FROM wallets)
--     AND TYPE = 'create'
-- )

-- , days AS (
--     SELECT 
--         generate_series(MIN(day), date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first contract
--     FROM creation_days
-- )

-- , prices as (
-- --ERC20 Tokens
--     SELECT
--         "minute" AS day,
--         contract_address,
--         symbol,
--         price,
--         decimals
--     FROM prices.usd
--     WHERE "minute" IN (SELECT day FROM days)
-- )

-- , transfers AS (
-- --ERC20 Tokens
--     SELECT
--         date_trunc('day', evt_block_time) AS day,
--         "from" AS address,
--         contract_address,
--         sum(-value) AS amount
--     FROM erc20."ERC20_evt_Transfer"
--     WHERE "from" IN (SELECT address FROM wallets)
--     AND evt_block_time >= (SELECT min(day) FROM creation_days)
--     GROUP BY 1,2,3
    
-- UNION ALL

--     SELECT
--         date_trunc('day', evt_block_time) AS day,
--         "to" AS address,
--         contract_address,
--         sum(value) AS amount
--     FROM erc20."ERC20_evt_Transfer"
--     WHERE "to" IN (SELECT address FROM wallets)
--     AND evt_block_time >= (SELECT min(day) FROM creation_days)
--     GROUP BY 1,2,3

-- )

-- , transfers_day AS (
--     SELECT
--         t.day,
--         t.address,
--         t.contract_address,
--         sum(t.amount/10^coalesce(p.decimals,18)) AS change
--     FROM transfers t
--     LEFT OUTER JOIN prices p ON t.contract_address = p.contract_address AND t.day = p.day
--     GROUP BY 1,2,3
-- )

-- , balances_w_gap_days AS (
--     SELECT
--         day,
--         address,
--         contract_address,
--         sum(change) OVER (PARTITION BY address, contract_address ORDER BY day) AS "balance",
--         lead(day, 1, now()) OVER (PARTITION BY address, contract_address ORDER BY day) AS next_day
--     FROM transfers_day
-- )

-- , balances_all_days AS (
--     SELECT
--         d.day,
-- --        b.address,
--         b.contract_address,
--         sum(b.balance) AS "balance"
--     FROM balances_w_gap_days b
--     INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
--     GROUP BY 1,2 --,3
--     ORDER BY 1,2 --,3
-- )

-- SELECT
--     b.day,
-- --    b.address,
-- --    w.wallet,
-- --    w.org,
--     b.contract_address,
--     p.symbol AS token,
--     b.balance,
--     p.price,
--     b.balance * coalesce(p.price,0) AS usd_value
-- FROM balances_all_days b
-- LEFT OUTER JOIN prices p ON b.contract_address = p.contract_address AND b.day = p.day
-- -- LEFT OUTER JOIN wallets w ON b.address = w.address
-- WHERE p.symbol IS NOT NULL 
-- AND b.day > '2023-03-01'
-- ORDER BY usd_value DESC
-- LIMIT 100
-- ;


with days AS (
    SELECT 
        generate_series((date_trunc('day', NOW()) - interval '365' day), date_trunc('day', NOW()), '1 day') AS day -- Generate all days since 365 days before
)

, transfers AS (
--ERC20 Tokens
    SELECT
        date_trunc('day', evt_block_time) AS day,
        "from" AS address,
        sum(-value) AS amount
    FROM erc20."ERC20_evt_Transfer"
    WHERE contract_address = '\x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'
    AND evt_block_time >= (SELECT min(day) FROM days)
    GROUP BY 1,2
    
UNION ALL

    SELECT
        date_trunc('day', evt_block_time) AS day,
        "to" AS address,
        sum(value) AS amount
    FROM erc20."ERC20_evt_Transfer"
    WHERE contract_address = '\x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'
    AND evt_block_time >= (SELECT min(day) FROM days)
    GROUP BY 1,2

)

, transfers_day AS (
    SELECT
        t.day,
        t.address,
        sum(t.amount/10^18) AS change
    FROM transfers t
    GROUP BY 1,2
)

, balances_w_gap_days AS (
    SELECT
        day,
        address,
        sum(change) OVER (PARTITION BY address ORDER BY day) AS "balance",
        lead(day, 1, now()) OVER (PARTITION BY address ORDER BY day) AS next_day
    FROM transfers_day
)

, balances_all_days AS (
    SELECT
        d.day,
--        b.address,

        sum(b.balance) AS "balance"
    FROM balances_w_gap_days b
    INNER JOIN days d ON b.day <= d.day AND d.day < b.next_day
    GROUP BY 1,2 --,3
    ORDER BY 1,2 --,3
)

SELECT
    b.day,
    b.balance
    -- b.balance * coalesce(p.price,0) AS usd_value
FROM balances_all_days b
-- LEFT OUTER JOIN wallets w ON b.address = w.address
WHERE b.day > '2023-03-01'
ORDER BY b.day desc
LIMIT 100
;


