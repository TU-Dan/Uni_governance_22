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
order by  "proposalId";