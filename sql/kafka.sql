BEGIN;
create schema kafka;

create function kafka.produce(varchar, varchar)
returns boolean as 'pg_kafka.so', 'pg_kafka_produce'
language C immutable;

comment on function kafka.produce(varchar, varchar) is
'Produces a message (topic, message).';

create function kafka.produce(varchar, varchar, varchar)
returns boolean as 'pg_kafka.so', 'pg_kafka_produce_keyed_message'
language C immutable;

comment on function kafka.produce(varchar, varchar, varchar) is
'Produces a message (topic, key, message).';

create function kafka.close() 
returns boolean as 'pg_kafka.so', 'pg_kafka_close'
language C immutable;

comment on function kafka.close() is 
'Closes the broker connections to Kafka.';

create table kafka.broker (
  host text not null,
  port integer not null default 9092,
  primary key(host, port)
);

COMMIT;
