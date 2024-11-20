select enable_fault_injection();
select add_fault_point('fj/debug/19787', ':::', 'echo', 0, '');


use mo_catalog;
drop table if exists cluster_table_1;
create cluster table cluster_table_1(a int, b int);

drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

insert into cluster_table_1 values(0,0,0),(1,1,0);
insert into cluster_table_1 values(0,0,1),(1,1,1);
select * from cluster_table_1;

-- @session:id=1&user=acc01:test_account&password=111
select * from mo_catalog.cluster_table_1;
create snapshot snapshot1 for account acc01;
-- @session

create snapshot snapshot2 for account sys;

drop table if exists cluster_table_1;

-- @session:id=1&user=acc01:test_account&password=111
select * from mo_catalog.cluster_table_1;
restore account acc01 from snapshot snapshot1;
select * from mo_catalog.cluster_table_1;
-- @session

restore account sys from snapshot snapshot2;

-- @session:id=1&user=acc01:test_account&password=111
select * from mo_catalog.cluster_table_1;
-- @session

drop snapshot if exists snapshot1;
drop snapshot if exists snapshot2;

drop table if exists cluster_table_1;

drop account if exists acc01;
-- @ignore:1
show snapshots;

use mo_catalog;
drop table if exists cluster_table_2;
create cluster table cluster_table_2(a int, b int);

drop account if exists acc01;
create account acc01 admin_name = 'test_account' identified by '111';

insert into cluster_table_2 values(0,0,0),(1,1,0);
insert into cluster_table_2 values(0,0,1),(1,1,1);
select * from cluster_table_2;
-- @session:id=2&user=acc01:test_account&password=111
select * from mo_catalog.cluster_table_2;
create snapshot cluster_table_sp for account acc01;
-- @session
create snapshot cluster_table_sp_2 for account sys;
drop table if exists cluster_table_2;
-- @session:id=2&user=acc01:test_account&password=111
select * from mo_catalog.cluster_table_2;
restore account acc01 from snapshot cluster_table_sp;
select * from mo_catalog.cluster_table_2;
-- @session
restore account sys from snapshot cluster_table_sp_2;
-- @session:id=2&user=acc01:test_account&password=111
select * from mo_catalog.cluster_table_2;
-- @session

drop snapshot if exists cluster_table_sp;
drop snapshot if exists cluster_table_sp_2;

drop table if exists mo_catalog.cluster_table_2;
drop account if exists acc01;
-- @ignore:1
show snapshots;

select disable_fault_injection();
