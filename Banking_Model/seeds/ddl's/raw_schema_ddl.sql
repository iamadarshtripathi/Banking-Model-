create sequence elt_run_id_seq start = 1, increment = 1;
create or replace table raw_elt_logs 
(
    elt_run_id int NOT NULL default elt_run_id_seq.nextval,
    elt_run_status varchar(50),
    elt_run_date TIMESTAMP_NTZ NOT NULL default GETDATE(),
    ELT_MODEL VARCHAR(50),
    ELT_INSERT NUMBER(10,0),
    ELT_UPDATE NUMBER(10,0),
    ELT_DELETE NUMBER(10,0),
    UNIQUE(ELT_RUN_ID, ELT_MODEL)
);
