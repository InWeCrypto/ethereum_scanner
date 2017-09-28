CREATE TABLE IF NOT EXISTS ethtx
	(
		ttimestamp bigint NOT NULL,
		blocknumber character varying(64)  NOT NULL,
		addr_from character varying(64) NOT NULL,
		addr_to character varying(64)  NOT NULL,
		gas bigint NOT NULL,
		value bigint NOT NULL,
		hash character varying(66) NOT NULL,
        type int NOT NULL,
        method_id character varying(10),
        addr_token character varying(64) NOT NULL,
        token_value bigint NOT NULL,
		PRIMARY KEY(hash)
	);


-- default is b-tree
CREATE INDEX timestamp_index ON ethtx (ttimestamp);
CREATE INDEX blocknumber_index ON ethtx USING hash (blocknumber);
CREATE INDEX hash_index ON ethtx USING hash (hash);
CREATE INDEX from_index ON ethtx USING hash (addr_from); 
CREATE INDEX to_index ON ethtx USING hash (addr_to);

-- 1min
CREATE TABLE IF NOT EXISTS eth_statistics_1min 
   (
			id SERIAL PRIMARY KEY NOT NULL,
			start_time int8,
			end_time int8,
			sum_value decimal,
            sum_token_value decimal,
			type int4,
			addr_token char(42),
			addr_to char(42),
			sum_gas decimal,
			interval int8
    );

CREATE INDEX indexing_start_time_1min ON eth_statistics_1min (start_time); 
CREATE INDEX indexing_end_time_1min ON eth_statistics_1min (end_time); 
CREATE INDEX indexing_addr_to_1min ON eth_statistics_1min (addr_to); 
CREATE INDEX indexing_addr_token_1min ON eth_statistics_1min (addr_token); 

-- 5min
CREATE TABLE IF NOT EXISTS eth_statistics_5min 
   (
			id SERIAL PRIMARY KEY NOT NULL,
			start_time int8,
			end_time int8,
			sum_value decimal,
            sum_token_value decimal,
			type int4,
			addr_token char(42),
			addr_to char(42),
			sum_gas decimal,
			interval int8
    );

CREATE INDEX indexing_start_time_5min ON eth_statistics_5min (start_time); 
CREATE INDEX indexing_end_time_5min ON eth_statistics_5min (end_time); 
CREATE INDEX indexing_addr_to_5min ON eth_statistics_5min (addr_to); 
CREATE INDEX indexing_addr_token_5min ON eth_statistics_5min (addr_token); 

-- 10min
CREATE TABLE IF NOT EXISTS eth_statistics_10min 
   (
			id SERIAL PRIMARY KEY NOT NULL,
			start_time int8,
			end_time int8,
			sum_value decimal,
            sum_token_value decimal,
			type int4,
			addr_token char(42),
			addr_to char(42),
			sum_gas decimal,
			interval int8
    );

CREATE INDEX indexing_start_time_10min ON eth_statistics_10min (start_time); 
CREATE INDEX indexing_end_time_10min ON eth_statistics_10min (end_time); 
CREATE INDEX indexing_addr_to_10min ON eth_statistics_10min (addr_to); 
CREATE INDEX indexing_addr_token_10min ON eth_statistics_10min (addr_token); 


-- 30min
CREATE TABLE IF NOT EXISTS eth_statistics_30min 
   (
			id SERIAL PRIMARY KEY NOT NULL,
			start_time int8,
			end_time int8,
			sum_value decimal,
            sum_token_value decimal,
			type int4,
			addr_token char(42),
			addr_to char(42),
			sum_gas decimal,
			interval int8
    );

CREATE INDEX indexing_start_time_30min ON eth_statistics_30min (start_time); 
CREATE INDEX indexing_end_time_30min ON eth_statistics_30min (end_time); 
CREATE INDEX indexing_addr_to_30min ON eth_statistics_30min (addr_to); 
CREATE INDEX indexing_addr_token_30min ON eth_statistics_30min (addr_token); 

ALTER TABLE ethtx ADD COLUMN gas_used decimal;
ALTER TABLE ethtx ALTER COLUMN gas TYPE decimal;
ALTER TABLE ethtx ALTER COLUMN token_value TYPE decimal;
ALTER TABLE ethtx ALTER COLUMN value TYPE decimal;

CREATE INDEX type_index ON ethtx USING hash (type);
ALTER TABLE ethtx ALTER COLUMN blocknumber TYPE bigint USING blocknumber::bigint;
