-- @rchen8 / Total Uniswap users over time
SELECT date, sum(users) OVER (
                              ORDER BY date ASC ROWS BETWEEN unbounded preceding AND CURRENT ROW) AS total_users
FROM
  (SELECT date, count(USER) AS users
   FROM
     (SELECT min(date) AS date,
             account AS USER
      FROM
        (SELECT date_trunc('day', min(block_time)) AS date,
                trader_a AS account
         FROM dex.trades
         WHERE project = 'Uniswap'
         GROUP BY 2
         UNION ALL SELECT date_trunc('day', min(block_time)) AS date,
                          trader_b AS account
         FROM dex.trades
         WHERE project = 'Uniswap'
         GROUP BY 2
         UNION ALL SELECT date_trunc('day', min(evt_block_time)) AS date,
                          provider AS account
         FROM uniswap."Exchange_evt_AddLiquidity"
         GROUP BY 2
         UNION ALL SELECT date_trunc('day', min(call_block_time)) AS date,
                          "to" AS account
         FROM uniswap_v2."Router01_call_addLiquidity"
         WHERE call_success
         GROUP BY 2
         UNION ALL SELECT date_trunc('day', min(call_block_time)) AS date,
                          "to" AS account
         FROM uniswap_v2."Router01_call_addLiquidityETH"
         WHERE call_success
         GROUP BY 2
         UNION ALL SELECT date_trunc('day', min(call_block_time)) AS date,
                          "to" AS account
         FROM uniswap_v2."Router02_call_addLiquidity"
         WHERE call_success
         GROUP BY 2
         UNION ALL SELECT date_trunc('day', min(call_block_time)) AS date,
                          "to" AS account
         FROM uniswap_v2."Router02_call_addLiquidityETH"
         WHERE call_success
         GROUP BY 2
         UNION ALL SELECT date_trunc('day', min(evt_block_time)) AS date,
                          "to" AS account
         FROM uniswap_v3."NonfungibleTokenPositionManager_evt_Transfer"
         WHERE "from" = '\x0000000000000000000000000000000000000000'
         GROUP BY 2) a
      GROUP BY 2) b
   GROUP BY 1
   ORDER BY 1) c
;


with last_delegate as (
        select
            evt_block_time,
            row_number() over (partition by delegator order by evt_block_time desc) as row,
            delegator,
            "toDelegate"
        from uniswap."UNI_evt_DelegateChanged"
    ), current_delegation as (
        select
            *
        from last_delegate
        where row = 1
        order by 3 desc
    ), currrent_votes as (
        select 
            delegate,
            (sum("newBalance") - sum("previousBalance")) / 10^18 as votes
        from uniswap."UNI_evt_DelegateVotesChanged"
        group by delegate
    ), temp as (
        select
            "toDelegate" as delegate,
            count(distinct delegator) as delegators
        from current_delegation
        group by delegate
    ), total_votes as (
        select voter, count("proposalId") as total_votes from uniswap_v2."GovernorAlpha_evt_VoteCast" group by voter
    ), current_proposal as (
        select voter, support
        FROM uniswap_v2."GovernorAlpha_evt_VoteCast"
        where "proposalId" in (select max(id) from uniswap_v2."GovernorAlpha_evt_ProposalCreated")
    ),
Data as (select
    rank() OVER (order by c.votes desc) as rank,
    t.*, c.votes, 
    to_char(c.votes / sum(c.votes) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) * 100, '999D99%') as "percent",
    tv.total_votes,
    support as current_vote
from temp t
join currrent_votes c on t.delegate = c.delegate
left join total_votes tv on t.delegate = tv.voter
left join current_proposal p on p.voter = t.delegate
order by c.votes desc) 

select count('rank') as number_of_delegates, sum(delegators) as delegators, sum(total_votes) as total_votes
from data
;


-- Delegatees with positive votes over time in recent 12 month
-- TOP N Delegatees with positive votes over time in recent 12 month
WITH l_month AS (
    SELECT 
        generate_series((date_trunc('month', NOW()) - interval '12' month), (date_trunc('month', NOW())+ interval '1' month), '1 month') AS month_name, -- Generate all days since 365 days before
        ROW_NUMBER() over(order by generate_series((date_trunc('month', NOW()) - interval '12' month), (date_trunc('month', NOW())+ interval '1' month), '1 month'))  as row_num
)
, delegate as (
    SELECT delegate AS delegatee
    , "newBalance" AS newbalance
    , "previousBalance" AS prebalance
    , (CASE 
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
    FROM uniswap."UNI_evt_DelegateVotesChanged" 
)
, delegate_overtime AS (
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 1) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 1
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 2) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 2
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 3) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 3
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 4) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 4
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 5) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 5
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 6) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 6
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 7) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 7
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 8) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 8
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 9) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 9
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 10) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 10
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 11) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 11
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 12) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 12
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 13) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 13
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 14) as month,
        delegatee,
        sum(newbalance - prebalance)/1e18 as uni_balance,
        rank() OVER (ORDER BY sum(newbalance - prebalance)/1e18 DESC) AS vote_rank
    FROM delegate 
    left join l_month on delegate.month_num = l_month.row_num
    WHERE month_num <= 14
    group by 1,2
    HAVING sum(newbalance - prebalance)/1e18 > 0
    )
    )
select month as "Month",
count(distinct delegatee) as "Total Delegatees",
sum(uni_balance) as "Total Uni Delegated",
sum(case when vote_rank <= 10 then uni_balance end)/sum(uni_balance) as "TOP10 Delegatees' Voting Power",
sum(case when vote_rank <= 25 then uni_balance end)/sum(uni_balance) as "TOP25 Delegatees' Voting Power",
sum(case when vote_rank <= 50 then uni_balance end)/sum(uni_balance) as "TOP50 Delegatees' Voting Power",
sum(case when vote_rank <= 100 then uni_balance end)/sum(uni_balance) as "TOP100 Delegatees' Voting Power",
sum(case when vote_rank <= 1000 then uni_balance end)/sum(uni_balance) as "TOP1000 Delegatees' Voting Power"
FROM delegate_overtime
group by 1
;







