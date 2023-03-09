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


-- version 3
-- correct mistakes before, proposalID are not unique
-- add proposals' names
with proposal_name as 
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

, proposal as (
    SELECT "proposalId"
    , contract_address
    , min(date_trunc('day',evt_block_time)) as start_date
    , max(date_trunc('day',evt_block_time)) as end_date
    , count(distinct voter) as "Total Voters"
    , count(distinct case when support = 'true' then voter end) as "Voters For"
    , count(distinct case when support = 'false' then voter end) as "Voters Against"
    , sum(votes/1e18) as "Total Votes"
    , sum(case when support = 'true' then votes end)/1e18 as "Votes For"
    , sum(case when support = 'false' then votes end)/1e18 as "Votes Against"       
    FROM uniswap_v2."GovernorAlpha_evt_VoteCast" 
    GROUP BY 1,2
    UNION ALL
    SELECT "proposalId"
    , contract_address
    , min(date_trunc('day',evt_block_time)) as start_date
    , max(date_trunc('day',evt_block_time)) as end_date
    , count(distinct voter) as "Total Voters"
    , count(distinct case when support = 1 then voter end) as "Voters For"
    , count(distinct case when support = 0 then voter end) as "Voters Against"
    , sum(votes)/1e18 as "Total Votes"
    , sum(case when support = 1 then votes end)/1e18 as "Votes For"
    , sum(case when support = 0 then votes end)/1e18 as "Votes Against"       
    FROM uniswap_v3."GovernorBravoDelegate_evt_VoteCast"
    GROUP BY 1,2
)
, proposal_realid as (
    select   '<a href="https://app.uniswap.org/#/vote/' || LEFT("real ID", 1) || '/' || TRIM( LEADING '0' FROM REPLACE(RIGHT("real ID", 2),'.','')) ||'?chain=mainnet">' || "real ID" || '</a>' as "ID & Link"
        ,"real ID"
        ,"proposalId"
        ,description
        ,proposal_name.contract_address
        ,proposal.start_date
        ,proposal.end_date
        ,proposal."Total Voters"
        ,proposal."Voters For"
        ,proposal."Voters Against"
        ,proposal."Total Votes"
        ,proposal."Votes For"
        ,proposal."Votes Against"      
    FROM proposal_name 
    LEFT JOIN proposal 
    ON proposal_name.internal_id = proposal."proposalId" AND proposal_name.contract_address = proposal.contract_address
)
, filter_proposal as (
    select *
    , row_number() over(order by start_date) as row_num
    from proposal_realid 
    where "ID & Link" is not null 
    and start_date is not null
    order by start_date
)
,voters as (
    SELECT "proposalId", contract_address, voter
    FROM uniswap_v2."GovernorAlpha_evt_VoteCast" 
    UNION ALL
    SELECT "proposalId", contract_address, voter
    FROM uniswap_v3."GovernorBravoDelegate_evt_VoteCast"
)
, voter_change as (
SELECT t1."ID & Link", t1."proposalId", t1.contract_address,
    t1.end_date, 
    date_trunc('month',t1.end_date)+ interval '1' month as propo_end_month,
       COUNT(DISTINCT case when t2.voter not in 
            (SELECT t2a.voter
            FROM voters t2a
            INNER JOIN filter_proposal t1a 
            ON t1a."proposalId" = t2a."proposalId" and t1a.contract_address = t2a.contract_address
            WHERE t1a.row_num < t1.row_num
           ) then t2.voter end
           ) AS Num_new_voters
FROM filter_proposal t1
INNER JOIN voters t2 ON t1."proposalId" = t2."proposalId" and t1.contract_address = t2.contract_address
GROUP BY 1,2,3,4
)
, delegate as (
    SELECT "Month"
    ,"Total Delegatees"
    ,"Total Uni Delegated"
    FROM dune_user_generated.Uni_Delegatees_12month
)
, voter_proportion as (   
    SELECT voter_change."ID & Link", voter_change.propo_end_month,
    delegate."Total Uni Delegated", delegate."Total Delegatees"
    FROM voter_change 
    left join delegate  on voter_change.propo_end_month = delegate."Month"
)
select     p."real ID"
    ,p."ID & Link"
    ,p.start_date as "Proposal Start Date"
    ,p.end_date as "Proposal End Date"
    ,p."Total Voters"
    ,p."Voters For"
    ,p."Voters Against"
    ,p."Voters For"/p."Total Voters"::double precision as "Voters For Percentage"
    ,p."Voters Against"/p."Total Voters"::double precision as "Voters Against Percentage"
    ,p."Total Votes"
    ,p."Votes For"
    ,p."Votes Against"       
    ,p."Votes For"/p."Total Votes"::double precision as "Votes For Percentage"
    ,p."Votes Against"/p."Total Votes"::double precision as "Votes Against Percentage"
    ,voter_change.num_new_voters as "Num New Voters"
    ,sum(voter_change.Num_new_voters) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as "Cum Total Participated Voters"
    ,voter_proportion."Total Uni Delegated"
    ,voter_proportion."Total Delegatees"
    ,p."Total Voters"/voter_proportion."Total Delegatees"::double precision as "Voter Turnout"
    ,p."Total Votes"/voter_proportion."Total Uni Delegated"::double precision as "Vote Turnout"
    ,LEFT(p.description, 50) || '....' as Title
FROM filter_proposal p
left join voter_change on p."ID & Link" = voter_change."ID & Link"
left join voter_proportion on  p."ID & Link" = voter_proportion."ID & Link"
order by p.start_date;