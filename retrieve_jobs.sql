create table retrieve_jobs ( 
       id int(11) not null auto_increment, 
       job_id varchar(700) not null,
       archive_id varchar(700) not null,
       backup_id int(11) not null references backup_catalog, 
       date_started datetime not null, 
       is_finished boolean not null,
      unique key(job_id),
      primary key (id) )

