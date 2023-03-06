WITH temp_table AS (
SELECT 
    evt_block_time,
    tr."from" AS address,
    -tr.value AS amount,
    contract_address
FROM erc20."ERC20_evt_Transfer" tr
WHERE contract_address = '\x1f9840a85d5af5bf1d1762f925bdaddc4201f984'

UNION ALL

SELECT
    evt_block_time,
    tr."to" AS address,
    tr.value AS amount,
    contract_address
FROM erc20."ERC20_evt_Transfer" tr
WHERE contract_address = '\x1f9840a85d5af5bf1d1762f925bdaddc4201f984'
), 

temp_table2 AS (
SELECT
    address,
    SUM(amount/10^18) AS balance
FROM temp_table tr
GROUP BY 1
ORDER BY 2 DESC
),

data as (
SELECT
    balance
FROM temp_table2
WHERE balance > 0
ORDER BY balance DESC
),

percentilesALT as (select 1/(1+exp(-1.70096*(4*(2*generate_series(.01,1,.01)-1))))::FLOAT as ntile union select 1) ,

binning as ( --find percentiles
    select percentile_disc(ntile) within group (order by balance) as ntile, ntile as percentile
    from data, percentilesalt
    group by ntile
),

ranges as (
    select ntile,
        numrange( (lag(ntile)over (order by ntile))::numeric, ntile::numeric, '(]') as ranges --turn percentiles into ranges
        ,percentile
    from binning
),

median AS (
SELECT
    percentile_cont(.5) WITHIN GROUP (ORDER BY balance) AS MedianCont
FROM data
),

subset_bal AS (
SELECT 
    balance,
    NTILE(1000) OVER(ORDER BY balance ASC) as "NTILE"
FROM data
),

subset_bal2 AS (
select 
    "NTILE",
    Sum(balance) as "Total Balance"
from subset_bal
GROUP BY "NTILE"
),

subset_bal3 AS (
SELECT 
    "NTILE",
    CASE 
        WHEN "NTILE" >= 990 THEN "Total Balance" 
    END as "Total Balance Top 1%",
    CASE 
        WHEN "NTILE" < 990 THEN "Total Balance" 
    END as "Total Balance Bottom 99%%"
FROM subset_bal2
),

average AS (
SELECT
    AVG(balance) AS average_balance
FROM data
),

one_percent AS (
SELECT 
    SUM("Total Balance Top 1%") as "Top 1%",
    SUM("Total Balance Bottom 99%%") as "Bottom 99%"
FROM subset_bal3
),


final_table AS (
select '('||coalesce(round(lower(ranges),3),0.1)::text|| ' - ' ||round(upper(Ranges),3)::text|| ') UNI' as wallet_Size,
    ntile as walletsizeupperlimit,
    percentile as percentile,
    count(*) as "# of wallets"
from data
join ranges on ranges @>balance::numeric
group by 1,2,3
order by ntile
)

SELECT
    wallet_Size,
    "# of wallets",
    MedianCont,
    average_balance,
    "Top 1%",
    "Bottom 99%"
FROM final_table, median, average, one_percent
;

WITH transfer AS (
    SELECT
        evt_tx_hash AS tx_hash,
        "from" AS address,
        -amount AS amount,
        contract_address
    FROM uniswap."UNI_evt_Transfer" AS tr
    WHERE contract_address = '\x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'
    
UNION ALL

    SELECT
        evt_tx_hash AS tx_hash,
        "to" AS address,
        amount AS amount,
        contract_address
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

SELECT count(distinct address)
FROM transferAmount
where poolholdings > 0
;




("\xd3c12c8e40aaf71250c37a9c85e4c9585dc5b922cdc1ab65c7435fce6f23f85e",
"\x387eb2bf68e85a810e7acc4c89eb45e1fa58dae01d6409a837bdd4f076ddeac0",
"\xff02707f5c5166ee8ed8e21e2d15e246e06253081cfa55c8a54a7f1b516b0baa",
"\x6fbbf3bc5a1693f08ba88a6dc0ebc184feea5e6c16a8d36bfde56b9ed4d9c70a",
"\xf399f3a6fd6b28c5be22fa6cb71cce3e2e923285c3ca6a4b499e2096b724d4ac",
"\x27ee465b827263de2e00dba869891fdfe733750353e447fa5486d232d6a09fe0",
"\xcbf4fcc2fcc99f96f7fe487371ddac217e29cbc5b9ed6dd44d48d2e9148239a2",
"\x2a625e3632b2da15bd5513b3a955a8fb3586fd005f9c43a449f9d4477ba03135",
"\x37e10a59cca7ca0aa3f1ec7c2dbec87419d6b85cfd7f4609120292a8abce928e",
"\x0e819d8a6c4ade5460dab3189a08a2d756ff0e242adc4ef9c9663ed8c5b4075c")

-- uniswap."UNI_evt_DelegateChanged"
-- uniswap."UNI_evt_DelegateVotesChanged"
-- uniswap."UNI_evt_Transfer"

select a.amount, a.from, a.to, 
    b.delegate, b."newBalance" - b."previousBalance" as delegated_amt
    -- ,c.delegator, c."fromDelegate", c."toDelegate"
from uniswap."UNI_evt_Transfer" a 
inner join uniswap."UNI_evt_DelegateVotesChanged" b on a.evt_tx_hash = b.evt_tx_hash
-- inner join uniswap."UNI_evt_DelegateChanged" c on a.evt_tx_hash = c.evt_tx_hash
where a.evt_tx_hash in (
'\xd3c12c8e40aaf71250c37a9c85e4c9585dc5b922cdc1ab65c7435fce6f23f85e',
'\x387eb2bf68e85a810e7acc4c89eb45e1fa58dae01d6409a837bdd4f076ddeac0',
'\xff02707f5c5166ee8ed8e21e2d15e246e06253081cfa55c8a54a7f1b516b0baa',
'\x6fbbf3bc5a1693f08ba88a6dc0ebc184feea5e6c16a8d36bfde56b9ed4d9c70a',
'\xf399f3a6fd6b28c5be22fa6cb71cce3e2e923285c3ca6a4b499e2096b724d4ac',
'\x27ee465b827263de2e00dba869891fdfe733750353e447fa5486d232d6a09fe0',
'\xcbf4fcc2fcc99f96f7fe487371ddac217e29cbc5b9ed6dd44d48d2e9148239a2',
'\x2a625e3632b2da15bd5513b3a955a8fb3586fd005f9c43a449f9d4477ba03135',
'\x37e10a59cca7ca0aa3f1ec7c2dbec87419d6b85cfd7f4609120292a8abce928e',
'\x0e819d8a6c4ade5460dab3189a08a2d756ff0e242adc4ef9c9663ed8c5b4075c');


tresury addresses:
('0x1a9c8182c09f50c8318d769245bea52c32be35bc',
'0x4750c43867ef5f89869132eccf19b9b6c4286e1a',
'0xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
'0x4b4e140d1f131fdad6fb59c13af796fd194e4135',
'0x3d30b1ab88d487b0f3061f40de76845bec3f1e94')

'\x1a9C8182C09F50C8318d769245beA52c32BE35BC'::bytea

('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
'\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
'\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
'\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
'\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')




day

contract_address

token

balance

price

usd_value
2023-03-02 00:00
\x0000000000000000000000000000000000000000
ETH
0.001660629381536117
1662.67
2.7610786537986556
2023-03-02 00:00
\x1f9840a85d5af5bf1d1762f925bdaddc4201f984
UNI
324096177.0735667
6.85
2220058812.953932
2023-03-01 00:00
\x0000000000000000000000000000000000000000
ETH
0.001660629381536117
1609.72
2.6731483280463184
2023-03-01 00:00
\x1f9840a85d5af5bf1d1762f925bdaddc4201f984
UNI
324096177.0735667
6.5
2106625150.9781833
2023-02-28 00:00
\x0000000000000000000000000000000000000000
ETH
0.001660629381536117
1635.15
2.7153781332187816



WITH l_month AS (
    SELECT 
        generate_series((date_trunc('month', NOW()) - interval '12' month), date_trunc('month', NOW()), '1 month') AS month_name, -- Generate all days since 365 days before
        ROW_NUMBER() over() as row_num
)
, transfer1 as (
    SELECT "from" AS address, -amount AS amount, evt_block_time
    FROM uniswap."UNI_evt_Transfer" 
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1)
    UNION ALL
    SELECT "to" AS address, amount AS amount, evt_block_time
    FROM uniswap."UNI_evt_Transfer"
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1)
)
, transfer2 as (
    SELECT "from" AS address, -amount AS amount, evt_block_time
    FROM uniswap."UNI_evt_Transfer" 
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2)
    UNION ALL
    SELECT "to" AS address, amount AS amount, evt_block_time
    FROM uniswap."UNI_evt_Transfer"
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2)
)
, transferAmount1 AS (
    SELECT 
        address,
        sum(amount)/1e18 as uni_amount,
        case when address in ('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
                              '\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
                              '\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
                              '\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
                              '\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
             then 1 else 0 end as isTreasury
    FROM transfer1
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1)
    GROUP BY 1
    HAVING sum(amount)/1e18 > 0
)
, transferAmount2 AS (
    SELECT 
        address,
        sum(amount)/1e18 as uni_amount,
        case when address in ('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
                              '\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
                              '\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
                              '\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
                              '\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
             then 1 else 0 end as isTreasury
    FROM transfer2
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2)
    GROUP BY 1
    HAVING sum(amount)/1e18 > 0
)
,result as (
    select  (SELECT month_name FROM l_month WHERE row_num = 1) as "Month",
    count(distinct address) as "Uni Holders",
    sum(uni_amount) as "Total Uni Quantity",
    sum(case when isTreasury = 1 then uni_amount end) as "Total Treasury Quantity"
    FROM transferAmount1
    -- where uni_amount > 0
    group by 1
UNION ALL 
    select  (SELECT month_name FROM l_month WHERE row_num = 2) as "Month",
    count(distinct address) as "Uni Holders",
    sum(uni_amount) as "Total Uni Quantity",
    sum(case when isTreasury = 1 then uni_amount end) as "Total Treasury Quantity"
FROM transferAmount2
-- where uni_amount > 0
group by 1
)
select *
from result
;




WITH l_month AS (
    SELECT 
        generate_series((date_trunc('month', NOW()) - interval '12' month), date_trunc('month', NOW()), '1 month') AS month_name,
        ROW_NUMBER() OVER (ORDER BY generate_series((date_trunc('month', NOW()) - interval '12' month), date_trunc('month', NOW()), '1 month')) AS row_num
)
, transfers AS (
    SELECT 
        "from" AS address, 
        "to" AS to_address,
        amount,
        (CASE 
            WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1) THEN 1 
            WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2) THEN 2 
            ELSE 3 
        END) AS month_num
    FROM uniswap."UNI_evt_Transfer"
    WHERE evt_block_time < (SELECT month_name FROM l_month ORDER BY row_num DESC LIMIT 1)
        AND "from" IN ('\x1a9c8182c09f50c8318d769245bea52c32be35bc', 
        '\x4750c43867ef5f89869132eccf19b9b6c4286e1a', 
        '\xe3953d9d317b834592ab58ab2c7a6ad22b54075d', 
        '\x4b4e140d1f131fdad6fb59c13af796fd194e4135', 
        '\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
)
, transferAmounts AS (
    SELECT 
        l_month.month_name AS month, 
        transfers.address,
        SUM(CASE WHEN transfers.month_num = 1 THEN -transfers.amount ELSE 0 END)/1e18 AS uni_amount_1,
        SUM(CASE WHEN transfers.month_num = 2 THEN -transfers.amount ELSE 0 END)/1e18 AS uni_amount_2,
        SUM(CASE WHEN transfers.month_num = 3 THEN -transfers.amount ELSE 0 END)/1e18 AS uni_amount_3,
        CASE 
            WHEN transfers.address IN ('\x1a9c8182c09f50c8318d769245bea52c32be35bc', '\x4750c43867ef5f89869132eccf
';


WITH l_month AS (
    SELECT 
        generate_series((date_trunc('month', NOW()) - interval '12' month), (date_trunc('month', NOW())+ interval '1' month), '1 month') AS month_name, -- Generate all days since 365 days before
        ROW_NUMBER() over(order by generate_series((date_trunc('month', NOW()) - interval '12' month), (date_trunc('month', NOW())+ interval '1' month), '1 month'))  as row_num
)
, transfer as (
    SELECT "from" AS address, -amount AS amount, 
    (CASE 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1) THEN 1 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2) THEN 2 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 3) THEN 3 
            ELSE 4
        END) AS month_num
    FROM uniswap."UNI_evt_Transfer" 
    UNION ALL
    SELECT "to" AS address, amount AS amount, 
    (CASE 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1) THEN 1 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2) THEN 2 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 3) THEN 3 
            ELSE 4
        END) AS month_num
    FROM uniswap."UNI_evt_Transfer"
)
, transferAmount AS (
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 1) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        case when address in ('a','b')
             then 1 else 0 end as isTreasury -- treasury addresses
    FROM transfer 
    left join l_month on transfer.month_num = l_month.row_num
    WHERE month_num <= 1
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 2) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        case when address in ('a','b')
             then 1 else 0 end as isTreasury -- treasury addresses
    FROM transfer
    WHERE month_num <= 2
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 3) as month,
        address,
        sum(amount)/1e18 as uni_amount,
       case when address in ('a','b')
             then 1 else 0 end as isTreasury -- treasury addresses
    FROM transfer
    WHERE month_num <= 3
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
)
select month as "Month",
count(distinct address) as "Uni Holders",
sum(uni_amount) as "Total Uni Quantity",
sum(case when isTreasury = 1 then uni_amount end) as "Total Treasury Quantity"
FROM transferAmount
group by 1
;




SELECT 
    (SELECT month_name FROM l_month WHERE row_num = 1) as month,
    address,
    SUM(CASE WHEN month_num <= 1 THEN amount/1e18 ELSE 0 END) as month_1_amount,
    SUM(CASE WHEN month_num <= 2 THEN amount/1e18 ELSE 0 END) as month_2_amount,
    SUM(CASE WHEN month_num <= 3 THEN amount/1e18 ELSE 0 END) as month_3_amount,
    CASE WHEN address in ('a','b') THEN 1 ELSE 0 END as isTreasury -- treasury addresses
FROM transfer
LEFT JOIN l_month ON transfer.month_num = l_month.row_num
WHERE month_num <= 3
GROUP BY 1, 2
HAVING SUM(amount)/1e18 > 0

;




WITH l_month AS (
    SELECT 
        generate_series((date_trunc('month', NOW()) - interval '12' month), (date_trunc('month', NOW())+ interval '1' month), '1 month') AS month_name, -- Generate all days since 365 days before
        ROW_NUMBER() over(order by generate_series((date_trunc('month', NOW()) - interval '12' month), (date_trunc('month', NOW())+ interval '1' month), '1 month'))  as row_num
)
, transfer as (
    SELECT "from" AS address, -amount AS amount, 
    case when "from" in ('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
                              '\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
                              '\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
                              '\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
                              '\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
             then 1 else 0 end as isTreasury, -- treasury addresses
    (CASE 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1) THEN 1 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2) THEN 2 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 3) THEN 3 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 4) THEN 4 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 5) THEN 5 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 6) THEN 6 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 7) THEN 7 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 8) THEN 8 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 9) THEN 9 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 10) THEN 10 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 11) THEN 11 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 12) THEN 12
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 13) THEN 13 
            ELSE 14
        END) AS month_num
    FROM uniswap."UNI_evt_Transfer" 
    UNION ALL
    SELECT "to" AS address, amount AS amount, 
    case when "to" in ('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
                              '\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
                              '\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
                              '\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
                              '\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
             then 1 else 0 end as isTreasury, -- treasury addresses
    (CASE 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1) THEN 1 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2) THEN 2 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 3) THEN 3 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 4) THEN 4 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 5) THEN 5 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 6) THEN 6 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 7) THEN 7 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 8) THEN 8 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 9) THEN 9 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 10) THEN 10 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 11) THEN 11 
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 12) THEN 12
        WHEN evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 13) THEN 13 
            ELSE 14
        END) AS month_num
    FROM uniswap."UNI_evt_Transfer"
)
, transferAmount AS (
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 1) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer 
    left join l_month on transfer.month_num = l_month.row_num
    WHERE month_num <= 1
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 2) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer
    WHERE month_num <= 2
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 3) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer
    WHERE month_num <= 3
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 4) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer 
    left join l_month on transfer.month_num = l_month.row_num
    WHERE month_num <= 4
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 5) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer
    WHERE month_num <= 5
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 6) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer
    WHERE month_num <= 6
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 7) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer 
    left join l_month on transfer.month_num = l_month.row_num
    WHERE month_num <= 7
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 8) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer
    WHERE month_num <= 8
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 9) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer
    WHERE month_num <= 9
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 10) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer 
    left join l_month on transfer.month_num = l_month.row_num
    WHERE month_num <= 10
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 11) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer
    WHERE month_num <= 11
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 12) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer
    WHERE month_num <= 12
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 13) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer
    WHERE month_num <= 11
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 14) as month,
        address,isTreasury,
        sum(amount)/1e18 as uni_amount
    FROM transfer
    WHERE month_num <= 12
    group by 1,2,3
    HAVING sum(amount)/1e18 > 0
    )
)
select month as "Month",
count(distinct address) as "Uni Holders",
sum(uni_amount) as "Total Uni Quantity",
sum(case when isTreasury = 1 then uni_amount end) as "Total Treasury Quantity"
FROM transferAmount
group by 1
;


delegate|previousBalance|newBalance|evt_block_time|num
a|24|39|2020-08-17 00:49|1
b|64|89|2020-09-17 00:49|2
c|24|59|2020-09-12 00:49|3
a|14|99|2020-09-16 00:49|4
c|64|93|2020-08-11 00:49|5
b|24|59|2020-03-12 00:49|6
a|14|99|2020-07-16 00:49|7
c|64|93|2020-06-11 00:49|8


with daily_delegate as (
SELECT distinct
  date_trunc('day', evt_block_time) AS day,
  delegate, 
  first_value(evt_block_time) OVER (
    PARTITION BY delegate, date_trunc('day', evt_block_time)
    ORDER BY evt_block_time DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS max_evt_block_time,
  first_value("newBalance"/1e18) OVER (
    PARTITION BY delegate, date_trunc('day', evt_block_time)
    ORDER BY evt_block_time DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS newBalance,
  first_value(("newBalance"-"previous")/1e18) OVER (
    PARTITION BY delegate, date_trunc('day', evt_block_time)
    ORDER BY evt_block_time DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS Balance
FROM 
  uniswap."UNI_evt_DelegateVotesChanged"
GROUP BY 
  date_trunc('day', evt_block_time), delegate, "newBalance","previousBalance",evt_block_time
ORDER BY 
  delegate, day
)
select day, 
count(distinct delegate) as total_delegatees,
sum(newBalance/1e18) as total_uni_delegated
from daily_delegate
group by day
order by day;

-- change point

