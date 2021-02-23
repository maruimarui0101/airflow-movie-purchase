-- CREATE SCHEMA djmax;

CREATE TABLE djmax.players (
    playerid varchar(128),
    reg_date timestamp,
    region varchar(128),
    curr_level int,
    league varchar(128),
    special_event boolean
)

