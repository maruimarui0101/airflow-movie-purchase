CREATE SCHEMA djmax;

CREATE TABLE retail.user_purchase (
    playerid varchar(128),
    reg_date timestamp,
    region varchar(128),
    curr_level int,
    league varchar(128),
    special_event boolean,
);

COPY retail.user_purchase(playerid, reg_date, region, curr_level, league, special_event) 
FROM '/data/retail/OnlineRetail.csv' DELIMITER ','  CSV HEADER;