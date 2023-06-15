use orderdatabase;

create table orders (
	order_id varchar(10),
    model varchar(30),
    reservation_date DATETIME,
    first_name varchar(255),
    last_name varchar(255),
    street_addr varchar(255),
    city varchar(50),
    zipcode char(10),
    country_code char(5),
    PRIMARY KEY (order_id));
    
select * from orders;

select count(*) from orders;

delete from orders;

drop table orders;