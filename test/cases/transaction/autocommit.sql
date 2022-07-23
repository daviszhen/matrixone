-- ---------
set autocommit = 1;
drop table if exists test1;
create table test1(a int);
insert into test1 values (1),(2),(3),(4);
select * from test1;
-- -----------
set autocommit = 1;
drop table if exists test1;
begin;
create table test1(a int);
insert into test1 values (1),(2),(3),(4);
select * from test1;
set autocommit = 0;
drop table test1;
rollback;
-- -----------
drop table if exists test1;
set autocommit = 0;
drop table if exists test1;
drop table if exists test1;
create table test1(a int);
insert into test1 values (1),(2),(3),(4);
select * from test1;
set autocommit = 0;