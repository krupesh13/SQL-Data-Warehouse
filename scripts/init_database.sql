-- Creating Database and schemas  -:  it has 3 schemas bronze,silver and gold and a database name DataWarehouse


use master;


create database DataWarehouse;

use DataWarehouse;

create schema bronze;
go
create schema silver;
go
create schema gold;
go
