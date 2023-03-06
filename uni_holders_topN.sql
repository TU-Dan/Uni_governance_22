-- Generate all days since 12 months before
-- - []TOP10, TOP25, TOP50 holders hold UNI percentage over time (without Treasury address)


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
    WHERE "from" not in ('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
                              '\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
                              '\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
                              '\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
                              '\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
    UNION ALL
    SELECT "to" AS address, amount AS amount, 
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
    WHERE "to" not in ('\x1a9c8182c09f50c8318d769245bea52c32be35bc',
                            '\x4750c43867ef5f89869132eccf19b9b6c4286e1a',
                            '\xe3953d9d317b834592ab58ab2c7a6ad22b54075d',
                            '\x4b4e140d1f131fdad6fb59c13af796fd194e4135',
                            '\x3d30b1ab88d487b0f3061f40de76845bec3f1e94')
)
, transferAmount AS (
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 1) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
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
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer
    WHERE month_num <= 2
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 3) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer
    WHERE month_num <= 3
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 4) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer 
    left join l_month on transfer.month_num = l_month.row_num
    WHERE month_num <= 4
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 5) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer
    WHERE month_num <= 5
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 6) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer
    WHERE month_num <= 6
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 7) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer 
    left join l_month on transfer.month_num = l_month.row_num
    WHERE month_num <= 7
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 8) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer
    WHERE month_num <= 8
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 9) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer
    WHERE month_num <= 9
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 10) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer 
    left join l_month on transfer.month_num = l_month.row_num
    WHERE month_num <= 10
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 11) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer
    WHERE month_num <= 11
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 12) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer
    WHERE month_num <= 12
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 13) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer
    WHERE month_num <= 11
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 14) as month,
        address,
        sum(amount)/1e18 as uni_amount,
        rank() OVER (ORDER BY sum(amount)/1e18 DESC) AS balance_rank
    FROM transfer
    WHERE month_num <= 12
    group by 1,2
    HAVING sum(amount)/1e18 > 0
    )
)
select month as "Month",
count(distinct address) as "Uni Holders",
sum(uni_amount) as "Total Uni Quantity Without Treasury",
sum(case when balance_rank <= 10 then uni_amount end)/sum(uni_amount) as "TOP10 Holders' UNI Quantity",
sum(case when balance_rank <= 25 then uni_amount end)/sum(uni_amount) as "TOP25 Holders' UNI Quantity",
sum(case when balance_rank <= 50 then uni_amount end)/sum(uni_amount) as "TOP50 Holders' UNI Quantity",
sum(case when balance_rank <= 100 then uni_amount end)/sum(uni_amount) as "TOP100 Holders' UNI Quantity",
sum(case when balance_rank <= 1000 then uni_amount end)/sum(uni_amount) as "TOP1000 Holders' UNI Quantity"
FROM transferAmount
group by 1
;