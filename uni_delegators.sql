With temp as
(
    Select 
        evt_block_time
        ,evt_block_number
        ,delegator
        ,CASE WHEN "toDelegate" = '\x0000000000000000000000000000000000000000' THEN -1 ELSE 1 END as variation
    FROM uniswap."UNI_evt_DelegateChanged"
    WHERE "fromDelegate" = '\x0000000000000000000000000000000000000000'
)

SELECT 
    date_trunc('month', evt_block_time) as "Month"
    ,SUM(variation) over (ORDER BY evt_block_number asc rows between unbounded preceding and current row) as "Total Delegators"
FROM temp

----------------------------

WITH l_month AS (
    SELECT 
        generate_series(date_trunc('month', NOW()) - interval '28' month, (date_trunc('month', NOW())+ interval '1' month), '1 month') AS month_name, -- Generate all days since 365 days before
        ROW_NUMBER() over(order by  generate_series(date_trunc('month', NOW()) - interval '28' month, (date_trunc('month', NOW())+ interval '1' month), '1 month'))  as row_num
)
(SELECT (SELECT month_name FROM l_month WHERE row_num = 1) as month,
    count(distinct delegator) as num_delegator
FROM uniswap."UNI_evt_DelegateChanged" 
WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1)
group by 1
)
union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 2) as month,
    count(distinct delegator) as num_delegator
FROM uniswap."UNI_evt_DelegateChanged" 
WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2)
group by 1
)
;

