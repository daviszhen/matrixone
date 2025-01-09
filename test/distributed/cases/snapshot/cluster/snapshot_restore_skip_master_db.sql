drop database if exists test03;
create database test03;
use test03;

drop snapshot if exists sp01;
create snapshot sp01 for account;

drop table if exists pri01;
create table pri01(
deptno int unsigned comment '部门编号',
dname varchar(15) comment '部门名称',
loc varchar(50) comment '部门所在位置',
primary key(deptno)
) comment='部门表';

insert into pri01 values (10,'ACCOUNTING','NEW YORK');
insert into pri01 values (20,'RESEARCH','DALLAS');

drop database if exists test02;
create database test02;
use test02;
drop table if exists aff01;
create table aff01(
empno int unsigned auto_increment COMMENT '雇员编号',
ename varchar(15) comment '雇员姓名',
job varchar(10) comment '雇员职位',
mgr int unsigned comment '雇员对应的领导的编号',
hiredate date comment '雇员的雇佣日期',
sal decimal(7,2) comment '雇员的基本工资',
comm decimal(7,2) comment '奖金',
deptno int unsigned comment '所在部门',
primary key(empno),
constraint c1 foreign key (deptno) references test03.pri01(deptno));

insert into aff01 values (7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,10);

restore account sys database test03 from snapshot sp01 ;

drop snapshot sp01;
drop database if exists test02;
drop database if exists test03;
