set global enable_privilege_cache = off;
select current_role();
drop database if exists sys_db_1;
create database sys_db_1;
use sys_db_1;
create table sys_tbl_1(a int primary key, b decimal, c char, d varchar(20) );
insert into sys_tbl_1 values(1,2,'a','database'),(2,3,'b','test publication'),(3, 4, 'c','324243243');
create publication sys_pub_1 database sys_db_1;
select * from sys_tbl_1;

-- @ignore:3,5
show subscriptions;
use sys_db_1;
select rp.privilege_id,rp.with_grant_option from mo_catalog.mo_role_privs rp where rp.obj_id = 0 and rp.obj_type = "database" and rp.role_id = 0 and rp.privilege_id = 26 and rp.privilege_level = "*";
alter table sys_tbl_1 drop primary key;
drop publication sys_pub_1;
drop database sys_db_1;