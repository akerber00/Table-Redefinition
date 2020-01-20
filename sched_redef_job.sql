CREATE OR REPLACE
procedure sched_redef_job(in_owner in varchar2, 
                            in_table_name in varchar2, 
                            int_table in varchar2,
                            parallel_degree in integer :=8,
                            step in varchar2,
                            runit in boolean :=FALSE,
                            move_inds in boolean :=FALSE,
                            new_ind_tablespace in varchar2 := NULL)
  authid current_user is
  job_exists number;
  num_running integer;
  jobcount integer:=0;
  current_tablespace varchar2(30);
  current_ind_tablespace varchar2(30);
  numinds integer :=0;
  jobid varchar2(20);
  job_name varchar2(30);
  par_cmd1 varchar2(200);
  par_cmd2 varchar2(200);
  d_cmd3 varchar2(2000);
  d_cmd4 varchar2(2000);
  mv_back varchar2(20):='TRUE';
  myjob_action varchar2(2000);
  myjob_action2 varchar2(2000);
  myjob_action3 varchar2(4000);
  sync_action varchar2(500);
begin
  if parallel_degree>1
  then
    par_cmd1:=' alter session force parallel dml parallel '||to_char(parallel_degree);
    par_cmd2:=' alter session force parallel query parallel '||to_char(parallel_degree);
  end if;
  d_cmd3:=' alter table '||in_owner||'.'||in_table_name||' drop unused columns';
  d_cmd4:=' drop table '||in_owner||'.'||int_table||' cascade constraints';
--  index_move(in_table_owner in varchar2,in_table_name in varchar2,new_ind_tablespace in varchar2)
  select rtrim(ltrim(to_char(object_id,'00000000000000'))) into jobid  from all_objects where owner=in_owner and object_name=in_table_Name and rownum=1;
  if (move_inds=TRUE)
  then
    myjob_action:='BEGIN execute immediate'||''''||par_cmd1||''''||';'||'  execute immediate'||''''||par_cmd2||''''||'; '||
          'index_move('||''''||in_owner||''''||','||''''||in_table_name||''''||','||''''||new_ind_tablespace||''''||'); '||
          'dbms_redefinition.start_redef_table(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||',options_flag=>dbms_redefinition.cons_use_pk); END;';
    job_name:='T_'||jobid;
  else
    myjob_action:='BEGIN execute immediate'||''''||par_cmd1||''''||';'||'  execute immediate'||''''||par_cmd2||''''||'; '||
          'dbms_redefinition.start_redef_table(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||',options_flag=>dbms_redefinition.cons_use_pk); END;';
  end if;
/*
   dbms_output.put_line('dbms_scheduler.create_job(
        job_name=>'||''''||job_name||''''||',
        job_type=>'||''''||'PLSQL_BLOCK'||''''||',
        job_action=>'||''''||myjob_action||''''||',
        comments=>'||''''||'Table decomp copydep'||''''||',
        auto_drop=>false, --set this to false for debugging purposes, job will remain on dba_scheduler_jobs view until manually removed
        enabled=>true); --can also be set to false if you wish to start job manually for debugging');
--  dbms_output.put_line(myjob_action);
*/
  if (runit = TRUE and step='START')
  then
      dbms_scheduler.create_job(
        job_name=>job_name,
        job_type=>'PLSQL_BLOCK',
        job_action=>myjob_action,
        comments=>'Table decomp',
        auto_drop=>false, --set this to false for debugging purposes, job will remain on dba_scheduler_jobs view until manually removed
        enabled=>true); --can also be set to false if you wish to start job manually for debugging
  end if;
  if (move_inds=TRUE)
  then 
    myjob_action2:='declare redef_errors number; BEGIN execute immediate'||''''||par_cmd1||''''||';'||'  execute immediate'||''''||par_cmd2||''''||'; '||
          'index_move('||''''||in_owner||''''||','||''''||in_table_name||''''||','||''''||new_ind_tablespace||''''||'); '||
          'dbms_redefinition.copy_table_dependents(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||',copy_statistics=>TRUE,num_errors=>redef_errors); END;';
  else
    myjob_action2:='declare redef_errors number; BEGIN execute immediate'||''''||par_cmd1||''''||';'||'  execute immediate'||''''||par_cmd2||''''||'; '||
          'dbms_redefinition.copy_table_dependents(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||',copy_statistics=>TRUE,num_errors=>redef_errors); END;';
  end if;
/*
  dbms_output.put_line('dbms_scheduler.create_job(
        job_name=>'||''''||job_name||''''||',
        job_type=>'||''''||'PLSQL_BLOCK'||''''||',
        job_action=>'||''''||myjob_action2||''''||',
        comments=>'||''''||'Table decomp copydep'||''''||',
        auto_drop=>false, --set this to false for debugging purposes, job will remain on dba_scheduler_jobs view until manually removed
        enabled=>true); --can also be set to false if you wish to start job manually for debugging');
--  dbms_output.put_line(myjob_action2);
*/
  if (runit=true and step='COPY')
  then
    job_name:='TAB_COPYDEP'||jobid;
    dbms_scheduler.create_job(
        job_name=>job_name,
        job_type=>'PLSQL_BLOCK',
        job_action=>myjob_action2,
        comments=>'Table decomp copydep',
        auto_drop=>false, --set this to false for debugging purposes, job will remain on dba_scheduler_jobs view until manually removed
        enabled=>true); --can also be set to false if you wish to start job manually for debugging
  end if;
  if (move_inds=TRUE)
  then 
    myjob_action3:='declare redef_errors number; BEGIN execute immediate'||''''||par_cmd1||''''||';'||'  execute immediate'||''''||par_cmd2||''''||'; '||
          ' index_move('||''''||in_owner||''''||','||''''||in_table_name||''''||','||''''||new_ind_tablespace||''''||'); '||
          'dbms_redefinition.start_redef_table(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||',options_flag=>dbms_redefinition.cons_use_pk); dbms_redefinition.copy_table_dependents(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||',copy_statistics=>TRUE,num_errors=>redef_errors); dbms_redefinition.sync_interim_table(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||'); dbms_redefinition.finish_redef_table(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||');'||' execute immediate'||''''||d_cmd3||''''||';'||'  execute immediate'||''''||d_cmd4||''''||';'||' end;';
  else
    myjob_action3:='declare redef_errors number; BEGIN execute immediate'||''''||par_cmd1||''''||';'||'  execute immediate'||''''||par_cmd2||''''||'; '||
          'dbms_redefinition.start_redef_table(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||',options_flag=>dbms_redefinition.cons_use_pk); dbms_redefinition.copy_table_dependents(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||',copy_statistics=>TRUE,num_errors=>redef_errors); dbms_redefinition.sync_interim_table(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||'); dbms_redefinition.finish_redef_table(uname=>'||
          ''''||in_owner||''''||',orig_table=>'||
          ''''||in_table_Name||''''||',int_table=>'||
          ''''||int_table||''''||');'||' execute immediate '||''''||d_cmd3||''''||';'||' execute immediate '||''''||d_cmd4||''''||';'||' end;';
  end if;  
--  dbms_output.put_line(myjob_action3);
  if (step ='BOTH')
  then
--    dbms_output.put_line(myjob_action3);

/*
    dbms_output.put_line('dbms_scheduler.create_job(
        job_name=>'||''''||job_name||''''||',
        job_type=>'||''''||'PLSQL_BLOCK'||''''||',
        job_action=>'||''''||myjob_action3||''''||',
        comments=>'||''''||'Table decomp all'||''''||',
        auto_drop=>false, --set this to false for debugging purposes, job will remain on dba_scheduler_jobs view until manually removed
        enabled=>true); --can also be set to false if you wish to start job manually for debugging');
*/
    if(runit=TRUE)
    then
      dbms_scheduler.create_job(
          job_name=>job_name,
          job_type=>'PLSQL_BLOCK',
          job_action=>myjob_action3,
          comments=>'Table decomp all',
          auto_drop=>false, --set this to false for debugging purposes, job will remain on dba_scheduler_jobs view until manually removed
          enabled=>true); --can also be set to false if you wish to start job manually for debugging
    end if;
  end if;
end;

/