drop database if exists fail2_test0;
create database fail2_test0;
use fail2_test0;
-- permission
drop role if exists role_r1;
drop user if exists role_u1;
create role role_r1;
create user role_u1 identified by '111' default role role_r1;
drop table if exists rename01;
create table rename01(col1 int);
insert into rename01 values(1);
insert into rename01 values(2);
grant create database on account * to role_r1;
grant show databases on account * to role_r1;
grant connect on account * to role_r1;
grant select on table * to role_r1;
grant show tables on database * to role_r1;

-- @session:id=2&user=sys:role_u1:role_r1&password=111
use fail2_test0;
alter table rename01 rename to newRename;
-- @session
drop role if exists role_r1;
drop user if exists role_u1;
drop database if exists fail2_test0;