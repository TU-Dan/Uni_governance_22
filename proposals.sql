-- add total votes' percentage of voting power at the end of proposal end date
-- add new voters every proposal than all the proposals before
-- add cum partitipated voters

-- Version 1
-- Voter and Votes distribution by proposal
with proposal as (
    SELECT "proposalId"
    , count(distinct voter) as "Total Voters"
    , count(distinct case when support = 'true' then voter end) as "Voters For"
    , count(distinct case when support = 'false' then voter end) as "Voters Against"
    , sum(votes/1e18) as "Total Votes"
    , sum(case when support = 'true' then votes end)/1e18 as "Votes For"
    , sum(case when support = 'false' then votes end)/1e18 as "Votes Against"       
    FROM uniswap_v2."GovernorAlpha_evt_VoteCast" 
    GROUP BY 1
    UNION ALL
    SELECT "proposalId"
    , count(distinct voter) as "Total Voters"
    , count(distinct case when support = 1 then voter end) as "Voters For"
    , count(distinct case when support = 0 then voter end) as "Voters Against"
    , sum(votes)/1e18 as "Total Votes"
    , sum(case when support = 1 then votes end)/1e18 as "Votes For"
    , sum(case when support = 0 then votes end)/1e18 as "Votes Against"       
    FROM uniswap_v3."GovernorBravoDelegate_evt_VoteCast"
    GROUP BY 1
)
select  "proposalId"
    ,"Total Voters"
    ,"Voters For"
    ,"Voters Against"
    ,"Voters For"/"Total Voters"::double precision as "Voters For Percentage"
    ,"Voters Against"/"Total Voters"::double precision as "Voters Against Percentage"
    ,"Total Votes"
    ,"Votes For"
    ,"Votes Against"       
    ,"Votes For"/"Total Votes"::double precision as "Votes For Percentage"
    ,"Votes Against"/"Total Votes"::double precision as "Votes Against Percentage"
FROM proposal
order by  "proposalId"
;


-- Version 2
-- Voter and Votes distribution by proposal

with proposal as (
    SELECT "proposalId"
    , min(date_trunc('day',evt_block_time)) as start_date
    , max(date_trunc('day',evt_block_time)) as end_date
    , count(distinct voter) as "Total Voters"
    , count(distinct case when support = 'true' then voter end) as "Voters For"
    , count(distinct case when support = 'false' then voter end) as "Voters Against"
    , sum(votes/1e18) as "Total Votes"
    , sum(case when support = 'true' then votes end)/1e18 as "Votes For"
    , sum(case when support = 'false' then votes end)/1e18 as "Votes Against"       
    FROM uniswap_v2."GovernorAlpha_evt_VoteCast" 
    GROUP BY 1
    UNION ALL
    SELECT "proposalId"
    , min(date_trunc('day',evt_block_time)) as start_date
    , max(date_trunc('day',evt_block_time)) as end_date
    , count(distinct voter) as "Total Voters"
    , count(distinct case when support = 1 then voter end) as "Voters For"
    , count(distinct case when support = 0 then voter end) as "Voters Against"
    , sum(votes)/1e18 as "Total Votes"
    , sum(case when support = 1 then votes end)/1e18 as "Votes For"
    , sum(case when support = 0 then votes end)/1e18 as "Votes Against"       
    FROM uniswap_v3."GovernorBravoDelegate_evt_VoteCast"
    GROUP BY 1
)
,voters as (
    SELECT "proposalId", voter
    FROM uniswap_v2."GovernorAlpha_evt_VoteCast" 
    UNION ALL
    SELECT "proposalId", voter
    FROM uniswap_v3."GovernorBravoDelegate_evt_VoteCast"
)
, voter_change as (
SELECT t1."proposalId",
    t1.end_date, 
    date_trunc('month',t1.end_date)+ interval '1' month as propo_end_month,
       COUNT(DISTINCT case when t2.voter not in 
            (SELECT t2a.voter
            FROM voters t2a
            INNER JOIN proposal t1a ON t1a."proposalId" = t2a."proposalId"
            WHERE t1a."proposalId" < t1."proposalId"
           ) then t2.voter end
           ) AS Num_new_voters
FROM proposal t1
INNER JOIN voters t2 ON t1."proposalId" = t2."proposalId"
GROUP BY 1,2,3
)
, delegate as (
    SELECT "Month"
    ,"Total Delegatees"
    ,"Total Uni Delegated"
    FROM dune_user_generated.Uni_Delegatees_12month
)
, voter_proportion as (   
    SELECT voter_change."proposalId", voter_change.propo_end_month,
    delegate."Total Uni Delegated", delegate."Total Delegatees"
    FROM voter_change 
    left join delegate  on voter_change.propo_end_month = delegate."Month"
) 
select  proposal."proposalId"
    ,proposal.start_date as "Proposal Start Date"
    ,proposal.end_date as "Proposal End Date"
    ,proposal."Total Voters"
    ,proposal."Voters For"
    ,proposal."Voters Against"
    ,proposal."Voters For"/proposal."Total Voters"::double precision as "Voters For Percentage"
    ,proposal."Voters Against"/proposal."Total Voters"::double precision as "Voters Against Percentage"
    ,proposal."Total Votes"
    ,proposal."Votes For"
    ,proposal."Votes Against"       
    ,proposal."Votes For"/proposal."Total Votes"::double precision as "Votes For Percentage"
    ,proposal."Votes Against"/proposal."Total Votes"::double precision as "Votes Against Percentage"
    ,voter_change.num_new_voters as "Num New Voters"
    ,sum(voter_change.Num_new_voters) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as "Cum Total Participated Voters"
    ,voter_proportion."Total Uni Delegated"
    ,voter_proportion."Total Delegatees"
    ,proposal."Total Voters"/voter_proportion."Total Delegatees"::double precision as "Voter Turnout"
    ,proposal."Total Votes"/voter_proportion."Total Uni Delegated"::double precision as "Vote Turnout"
FROM proposal
left join voter_change on proposal."proposalId" = voter_change."proposalId"
left join voter_proportion on  proposal."proposalId" = voter_proportion."proposalId"
order by  "proposalId";

