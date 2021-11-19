drop table "Employees", "Employees_temp";

create table "Employees"
(
    "Serial Number" numeric not null
 constraint employees_pk
            primary key,
    "Company Name" text,
    "Employee Markme" text,
    "Description" text,
    "Leave" integer
);

create table "Employees_temp"
(
    "Serial Number" numeric not null
 constraint employees_temp_pk
            primary key,
    "Company Name" text,
    "Employee Markme" text,
    "Description" text,
    "Leave" integer
);
