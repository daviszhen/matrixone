drop database if exists test;
create database test;
use test;
create table t1(a int not null, b int);
create table t2(a int, b int);
create table t3(a int, b int);
insert into t1 values (null, 0);
insert into t2 values (null, null);
insert into t3 values (0, 0);
insert into t1 select * from t3;
select * from t1;
insert into t1 select * from t2;
select * from t1;
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;
create table t(a int not null, b int);
insert into t values (1, 1);
insert into t values (1, null);
insert into t values (2, null);
insert into t values (3, null);
update t set a=null;
drop table if exists t1;
create table t1 (a int primary key, b int, c int, unique key(b,c));
INSERT INTO t1 SELECT result,result,null FROM generate_series(1,1000000) g;
drop table t1;
create table t1 (a int primary key, b int);
select enable_fault_injection();
select add_fault_point('inject_send_pipeline', ':::', 'echo', 1, 't1');
INSERT INTO t1 SELECT result,result FROM generate_series(1,3000000) g;
select disable_fault_injection();
drop database if exists test;