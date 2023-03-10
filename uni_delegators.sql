


(
    Select 
        evt_block_time
        ,evt_block_number
        ,delegator
        ,CASE WHEN "toDelegate" = '\x0000000000000000000000000000000000000000' THEN -1 ELSE 1 END as variation
    FROM uniswap."UNI_evt_DelegateChanged"
    WHERE "fromDelegate" = '\x0000000000000000000000000000000000000000'
)