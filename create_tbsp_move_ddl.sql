CREATE OR REPLACE
procedure create_tbsp_move_ddl(in_tablespace_name in varchar2,
                                                 in_owner in varchar2 default null,
                                                 new_tablespace in varchar2 default null, -- leave empty to just clear empty space in tbsp
                                                 new_ind_tablespace in varchar2 default null,
                                                 new_lob_tablespace in varchar2 default null,
                                                 int_table_name varchar2 :=null,
                                                 scheduler_mode in boolean default true,
                                                 interactive_mode in boolean default false,
                                                 submit_scheduler in boolean default false,
                                                 run_interactive in boolean :=false,
                                                 runcrt in boolean                 :=false, --set to true to create interim table
                                                 runredef in boolean              :=false, --set to true to run the redefinition job in this session
                                                 genredefjob in boolean            :=false, --set to true to generate the commands to submit to the dbms_scheduler
                                                 runredefjob in boolean            :=false, --set to true to submit to the scheduler
                                                 drop_interim_table in boolean     :=false, --set to true to drop the interim table automatically after job completion when running interactively
                                                 copy_statistics in boolean        :=true)  --set to true to copy stats when running interactively. Scheduler method will always copy stats.
authid current_user
as
/*  This procedure will only work to move standard, non-partitioned, non-IOT, non-queue tables from one tablespace to another.
Partitioned tables must be handled invididually and the interim table must be properly modified to handle the partitions.  If you have a large number of partitioned tables,
modify the gen_rebuild_ddl procedure to properly generate the commands to move them.  IOT and queue tables must be handled separately using different command sets.
See metalink note 1410195.1 to move queue tables.
*/
w_in_tablespace_name varchar2(30);
w_in_owner varchar2(30);
w_new_tablespace varchar2(30);
w_new_ind_tablespace varchar2(30);
w_new_lob_tablespace varchar2(30);
w_scheduler_mode boolean;
w_interactive_mode boolean;
w_run_interactive boolean;
w_submit_scheduler boolean;
w_runcrt boolean;
w_runredef boolean;
w_genredefjob boolean;
w_runredefjob boolean;
w_drop_interim_table boolean;
w_copy_statistics boolean;
w_int_table_name varchar2(30);
begin
  w_in_tablespace_name:=in_tablespace_name;
  w_in_owner:=in_owner;
  w_new_tablespace:=new_tablespace;
  w_new_ind_tablespace:=new_ind_tablespace;
  w_new_lob_tablespace:=new_lob_tablespace;
  w_scheduler_mode:=scheduler_mode;
  w_interactive_mode:=interactive_mode;
  w_run_interactive:=run_interactive;
  w_submit_scheduler:=submit_scheduler;
  w_runcrt:=runcrt;
  w_runredef:=runredef;
  w_genredefjob:=genredefjob;
  w_runredefjob:=runredefjob;
  w_drop_interim_table:=drop_interim_table;
  w_copy_statistics:=copy_statistics;
  w_int_table_name:=int_table_name;
  if (w_submit_scheduler=TRUE)
  then
    w_submit_scheduler:=TRUE;
    w_scheduler_mode:=TRUE;
    w_runcrt:=TRUE;
    w_genredefjob:=TRUE;
    w_runredefjob:=TRUE;
    w_interactive_mode:=FALSE;
    w_run_interactive:=FALSE;
  end if;
  if (interactive_mode=TRUE)
  then
    w_runcrt:=FALSE;
    w_submit_scheduler:=FALSE;
    w_scheduler_mode:=FALSE;
    w_runredef:=FALSE;
  end if;
  if (interactive_mode=TRUE and run_interactive=TRUE)
  then
    w_runcrt:=TRUE;
    w_runredef:=TRUE;
  end if;
  if in_owner=NULL
  then
    for tab_cursor in (select owner, table_name from dba_tables where tablespace_name=in_tablespace_name and partitioned = 'NO')
    loop
      gen_rebuild_ddl(tab_cursor.owner,
                      tab_cursor.table_name,
                      w_in_tablespace_name,
                      w_new_tablespace,
                      w_int_table_name,
                      w_new_ind_tablespace,
                      w_new_lob_tablespace,
                      w_runcrt,
                      w_runredef,
                      w_genredefjob,
                      w_runredefjob,
                      w_drop_interim_table,
                      w_copy_statistics);
    end loop;
  else
    for tab_cursor in (select owner, table_name from dba_tables where tablespace_name=in_tablespace_name and owner=in_owner and partitioned = 'NO')
    loop
      gen_rebuild_ddl(tab_cursor.owner,
                      tab_cursor.table_name,
                      w_in_tablespace_name,
                      w_new_tablespace,
                      w_int_table_name,
                      w_new_ind_tablespace,
                      w_new_lob_tablespace,
                      w_runcrt,
                      w_runredef,
                      w_genredefjob,
                      w_runredefjob,
                      w_drop_interim_table,
                      w_copy_statistics);
    end loop;
  end if;
end;

/