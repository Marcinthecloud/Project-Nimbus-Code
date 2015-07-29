USE test; 

--Demo data model for raw feed
--We can use the below data model to do quite a bit in Spark and Solr. Note that in a production environment

CREATE TABLE tx_details_raw (
    super_chain text,
    chain text,
    merchant text,
    tx_time timestamp,
    settle_time timestamp,
    amount double,
    terminal_id text,
PRIMARY KEY((super_chain, chain), merchant, tx_time)
);

--Pre aggregated roll up example
--This table, similar to the above, will contain pre aggregated values for easy visualization.

CREATE TABLE tx_details_raw (
    super_chain text,
    chain text,
    merchant text,
    tx_time timestamp,
    settle_time timestamp,
    amount double,
    terminal_id text,
PRIMARY KEY((super_chain, chain), merchant, tx_time)
);
