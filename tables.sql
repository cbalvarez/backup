create table backup_catalog ( id int not null auto_increment, vault_name varchar(50) not null, country varchar(10) not null, directory varchar(1000) not null, backup_date datetime not null, status int not null, description varchar(200) not null, primary key (id));

create table backup_files ( archive_id varchar(200) not null, backup_id int not null, file_name varchar(1000) not null, status int not null, primary key (archive_id, backup_id ));
