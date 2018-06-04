\set ECHO none
do $$
declare vstate text;
begin 
CREATE SERVER kafka_server
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (brokers 'localhost:9092');

CREATE USER MAPPING FOR PUBLIC SERVER kafka_server;
exception
when sqlstate '42710' then
     -- do nothing when exists 
when others then
    get stacked diagnostics vstate = returned_sqlstate;
    raise exception 'got sqlstate %', vstate;
end;
$$;
\set ECHO all