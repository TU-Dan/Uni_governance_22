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
