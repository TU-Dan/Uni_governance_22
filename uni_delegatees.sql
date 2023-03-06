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
