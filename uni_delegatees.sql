-- by @web3_data
CREATE or REPLACE view dune_user_generated.Uni_DelegateesNew as    

WITH last_delegate AS 
( 
    SELECT 
    "UNI_evt_DelegateChanged".evt_block_time, 
    row_number() OVER (PARTITION BY "UNI_evt_DelegateChanged".delegator ORDER BY "UNI_evt_DelegateChanged".evt_block_time DESC) AS "row", 
    "UNI_evt_DelegateChanged".delegator, 
    "UNI_evt_DelegateChanged"."toDelegate" 
    FROM uniswap."UNI_evt_DelegateChanged" 
)

, current_delegation AS 
( 
    SELECT 
        last_delegate.evt_block_time, 
        last_delegate."row", 
        last_delegate.delegator, 
        last_delegate."toDelegate" 
    FROM last_delegate 
    WHERE (last_delegate."row" = 1) )

, currrent_votes AS
( 
    SELECT 
        "UNI_evt_DelegateVotesChanged".delegate, 
        (((sum("UNI_evt_DelegateVotesChanged"."newBalance") - sum("UNI_evt_DelegateVotesChanged"."previousBalance")))::double precision / ((10)::double precision ^ (18)::double precision)) AS votes 
    FROM uniswap."UNI_evt_DelegateVotesChanged" 
    GROUP BY "UNI_evt_DelegateVotesChanged".delegate )

, number_delegators AS 
( 
    SELECT 
        current_delegation."toDelegate" AS delegate, 
        count(DISTINCT current_delegation.delegator) AS delegators 
    FROM current_delegation 
    GROUP BY current_delegation."toDelegate" )
    
, total_votes AS 
( 
    SELECT voter, sum(total_votes) as total_votes
    FROM 
    (
        SELECT 
            voter, 
            count(DISTINCT "proposalId") AS total_votes 
        FROM uniswap_v2."GovernorAlpha_evt_VoteCast" 
        GROUP BY voter 
    
        UNION ALL
        
        SELECT 
            voter,
            count(DISTINCT "proposalId") AS total_votes 
        FROM uniswap_v3."GovernorBravoDelegate_evt_VoteCast"
        GROUP BY voter 
    ) as temp
    GROUP BY voter
)

SELECT 
    rank() OVER (ORDER BY c.votes DESC) AS "Rank", 
    t.delegate AS "Delegatee", 
    ud.clearname AS "Clear Name", 
    t.delegators AS "# Delegators", 
    (c.votes / ((10)::double precision ^ (6)::double precision)) AS "Votes in Million", 
    (c.votes / sum(c.votes) OVER ()) AS "Voting Power", 
    tv.total_votes AS "# of Proposal Votes" 
FROM 
    (((number_delegators t JOIN currrent_votes c ON ((t.delegate = c.delegate))) 
LEFT JOIN total_votes tv ON ((t.delegate = tv.voter))) 
LEFT JOIN dune_user_generated.uni_delegateesmapping ud ON (((t.delegate)::text = ud.delegatee))) 
ORDER BY c.votes DESC;


-- TOP N Delegatees with positive votes over time in recent 12 month
CREATE or REPLACE view dune_user_generated.Uni_Delegatees_12month as  
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



-- optimization
-- TOP N Delegatees with positive votes over time in recent 30 month
CREATE or REPLACE view dune_user_generated.Uni_Delegatees_30month as  
WITH l_month AS (
    SELECT 
        generate_series(date_trunc('month', NOW()) - interval '28' month, (date_trunc('month', NOW())+ interval '1' month), '1 month') AS month_name, -- Generate all days since 365 days before
        ROW_NUMBER() over(order by  generate_series(date_trunc('month', NOW()) - interval '28' month, (date_trunc('month', NOW())+ interval '1' month), '1 month'))  as row_num
)
, delegate_overtime AS (
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 1) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 1)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 2) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 2)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 3) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 3)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 4) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 4)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 5) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 5)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 6) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 6)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 7) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 7)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 8) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 8)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 9) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 9)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 10) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 10)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 11) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 11)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 12) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 12)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 13) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 13)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 14) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 14)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 15) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 15)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 16) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 16)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 17) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 17)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 18) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 18)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 19) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 19)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
 union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 20) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 20)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 21) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 21)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 22) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 22)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 23) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 23)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 24) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 24)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 25) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 25)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 26) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 26)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 27) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 27)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 28) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 28)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )
    union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 29) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 29)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
    )union all 
    (SELECT (SELECT month_name FROM l_month WHERE row_num = 30) as month,
        delegate AS delegatee,
        sum("newBalance" - "previousBalance")/1e18 as uni_balance,
        rank() OVER (ORDER BY sum("newBalance" - "previousBalance")/1e18 DESC) AS vote_rank
    FROM uniswap."UNI_evt_DelegateVotesChanged"  
    WHERE evt_block_time < (SELECT month_name FROM l_month WHERE row_num = 30)
    group by 1,2
    HAVING sum("newBalance" - "previousBalance")/1e18 > 0
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

