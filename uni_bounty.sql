-- - [x]Month
-- - [x]Holders
-- - [x]Uni quantity held by holders
-- - [x]Uni quantity held by treasury address
-- - []Delegators
-- - []Delegated uni quantity
-- - []Delegatees
-- - []Delegatees' uni pool: newBalance votes
-- - []Delegatees ever voted in that period of time: proposal month, duration
-- - []Delegatees' votes ever voted 


-- uni holders count
with months AS (
    SELECT 
        generate_series((date_trunc('month', NOW()) - interval '12' month), date_trunc('month', NOW()), '1 month') AS month -- Generate all days since 365 days before
)
, transfer AS (
    SELECT 
        "from" AS address,
        -amount AS amount
    FROM uniswap."UNI_evt_Transfer" AS tr
    WHERE contract_address = '\x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'
    AND evt_block_time < (select max(month) from months limit 1)
    
UNION ALL

    SELECT 
        "to" AS address,
        amount AS amount
    FROM uniswap."UNI_evt_Transfer" AS tr
    WHERE contract_address = '\x1f9840a85d5aF5bf1D1762F925BDADdC4201F984'
    AND evt_block_time < (select max(month) from months limit 1)
),

transferAmount AS (
    SELECT 
        address,
        sum(amount)/1e18 as uni_amount,
        case when address in ('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
'\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
'\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
'\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
'\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
then 1 else 0 end as isTreasury
    FROM transfer
    GROUP BY 1
    having sum(amount)/1e18 > 0
)

SELECT (select max(month) from months limit 1) as "Month",
    count(distinct address) as "Uni Holders",
    sum(uni_amount) as "Total Uni Quantity",
    sum(case when isTreasury = 1 then uni_amount end) as "Total Treasury Quantity"
FROM transferAmount
-- where uni_amount > 0
group by 1
;




WITH l_month AS (
        SELECT 
        generate_series((date_trunc('month', NOW()) - interval '12' month), date_trunc('month', NOW()), '1 month') AS month_name -- Generate all days since 365 days before
)
, transfer as (
            SELECT "from" AS address, -amount AS amount 
            FROM uniswap."UNI_evt_Transfer" 
            WHERE evt_block_time < CAST({{month_name}} AS DATE)
            UNION ALL
            SELECT 
            "to" AS address,
            amount AS amount
            FROM uniswap."UNI_evt_Transfer"
             WHERE evt_block_time <  CAST({{month_name}} AS DATE)
            ),
            transferAmount AS (
    SELECT 
        address,
        sum(amount)/1e18 as uni_amount,
        case when address in ('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
'\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
'\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
'\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
'\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
        then 1 else 0 end as isTreasury
            FROM transfer
            GROUP BY 1
            having sum(amount)/1e18 > 0
        )
    select {{month_name}} as "Month",
    count(distinct address) as "Uni Holders",
    sum(uni_amount) as "Total Uni Quantity",
    sum(case when isTreasury = 1 then uni_amount end) as "Total Treasury Quantity"
FROM transferAmount
-- where uni_amount > 0
group by 1
;



WITH l_month AS (
        SELECT 
        generate_series((date_trunc('month', NOW()) - interval '12' month), date_trunc('month', NOW()), '1 month') AS month_name, -- Generate all days since 365 days before
        ROW_NUMBER() over() as row_num
)
, transfer1 as (
            SELECT "from" AS address, -amount AS amount 
            FROM uniswap."UNI_evt_Transfer" 
            WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1)
            UNION ALL
            SELECT 
            "to" AS address,
            amount AS amount
            FROM uniswap."UNI_evt_Transfer"
             WHERE evt_block_time <  (SELECT month_name FROM l_month WHERE row_num = 1)
            ),
transfer2 as (
            SELECT "from" AS address, -amount AS amount 
            FROM uniswap."UNI_evt_Transfer" 
            WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2)
            UNION ALL
            SELECT 
            "to" AS address,
            amount AS amount
            FROM uniswap."UNI_evt_Transfer"
             WHERE evt_block_time <  (SELECT month_name FROM l_month WHERE row_num = 2)
            ),
        transferAmount1 AS (
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
            GROUP BY 1
            having sum(amount)/1e18 > 0
        )
            ,transferAmount2 AS (
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
            GROUP BY 1
            having sum(amount)/1e18 > 0
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
        generate_series((date_trunc('month', NOW()) - interval '12' month), date_trunc('month', NOW()), '1 month') AS month_name, -- Generate all days since 365 days before
        ROW_NUMBER() over(order by generate_series((date_trunc('month', NOW()) - interval '12' month), date_trunc('month', NOW()), '1 month'))  as row_num
)
, transfer1 as (
    SELECT "from" AS address, -amount AS amount, (SELECT month_name FROM l_month WHERE row_num = 1) as month
    FROM uniswap."UNI_evt_Transfer" 
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1)
    UNION ALL
    SELECT "to" AS address, amount AS amount, (SELECT month_name FROM l_month WHERE row_num = 1) as month
    FROM uniswap."UNI_evt_Transfer"
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1)
)
, transfer2 as (
    SELECT "from" AS address, -amount AS amount, (SELECT month_name FROM l_month WHERE row_num = 2) as month
    FROM uniswap."UNI_evt_Transfer" 
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2)
    UNION ALL
    SELECT "to" AS address, amount AS amount, (SELECT month_name FROM l_month WHERE row_num = 2) as month
    FROM uniswap."UNI_evt_Transfer"
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2)
)
, transfer3 as (
    SELECT "from" AS address, -amount AS amount, (SELECT month_name FROM l_month WHERE row_num = 2) as month
    FROM uniswap."UNI_evt_Transfer" 
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 3)
    UNION ALL
    SELECT "to" AS address, amount AS amount, (SELECT month_name FROM l_month WHERE row_num = 3) as month
    FROM uniswap."UNI_evt_Transfer"
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 3)
)
, transferAmount1 AS (
    SELECT month,
        address,
        sum(amount)/1e18 as uni_amount,
        case when address in ('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
                              '\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
                              '\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
                              '\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
                              '\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
             then 1 else 0 end as isTreasury -- treasury addresses
    FROM transfer1
    GROUP BY 1,2
    HAVING sum(amount)/1e18 > 0
)
, transferAmount2 AS (
    SELECT month,
        address,
        sum(amount)/1e18 as uni_amount,
        case when address in ('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
                              '\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
                              '\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
                              '\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
                              '\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
             then 1 else 0 end as isTreasury
    FROM transfer2
    GROUP BY 1,2
    HAVING sum(amount)/1e18 > 0
)
, transferAmount3 AS (
    SELECT month,
        address,
        sum(amount)/1e18 as uni_amount,
        case when address in ('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
                              '\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
                              '\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
                              '\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
                              '\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
             then 1 else 0 end as isTreasury
    FROM transfer3
    GROUP BY 1,2
    HAVING sum(amount)/1e18 > 0
)
    select month as "Month",
    count(distinct address) as "Uni Holders",
    sum(uni_amount) as "Total Uni Quantity",
    sum(case when isTreasury = 1 then uni_amount end) as "Total Treasury Quantity"
    FROM transferAmount1
    group by 1

UNION ALL 
    select month as "Month",
    count(distinct address) as "Uni Holders",
    sum(uni_amount) as "Total Uni Quantity",
    sum(case when isTreasury = 1 then uni_amount end) as "Total Treasury Quantity"
FROM transferAmount2
group by 1

UNION ALL 
    select month as "Month",
    count(distinct address) as "Uni Holders",
    sum(uni_amount) as "Total Uni Quantity",
    sum(case when isTreasury = 1 then uni_amount end) as "Total Treasury Quantity"
FROM transferAmount3
group by 1
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
  ) AS newBalance
FROM 
  uniswap."UNI_evt_DelegateVotesChanged"
-- where evt_block_time < '2020-09-18'
GROUP BY 
  date_trunc('day', evt_block_time), delegate, "newBalance"
--   ,"previousBalance"
  ,evt_block_time
ORDER BY 
  delegate, day
)
select day, 
count(distinct delegate) as total_delegatees,
sum(newBalance) as total_new_balance
-- ,sum(Balance) as total_balance
from daily_delegate
where newBalance > 0
group by day
order by day;


-- delegatees 
with delegate as (
SELECT distinct
  date_trunc('month', evt_block_time) AS month,
  delegate, 
  first_value(evt_block_time) OVER (
    PARTITION BY delegate, date_trunc('month', evt_block_time)
    ORDER BY evt_block_time DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS max_evt_block_time,
  first_value("newBalance"/1e18) OVER (
    PARTITION BY delegate, date_trunc('month', evt_block_time)
    ORDER BY evt_block_time DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS newBalance
FROM 
  uniswap."UNI_evt_DelegateVotesChanged"
-- where evt_block_time < '2020-09-18'
GROUP BY 
  date_trunc('month', evt_block_time), delegate, "newBalance"
--   ,"previousBalance"
  ,evt_block_time
ORDER BY 
  delegate, month
)
select month, 
count(distinct delegate) as total_delegatees,
sum(newBalance) as total_new_balance
-- ,sum(Balance) as total_balance
from delegate
where newBalance > 0
group by month
order by month;