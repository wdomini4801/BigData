create database data;

use data;
create table customer (id bigint, name string, address string, age int);

insert into customer values (11, "Customer1", "Address1", 25), (22, "Customer2", "Address2", 22), (33, "Customer3", "Address3", 55);

describe customer;

select * from customer;

select name, age from customer order by age desc;
