CREATE OR REPLACE
procedure gen_rebuild_ddl(in_owner in varchar2, 
                  in_table_name in varchar2, 
                  tablespace_name in varchar2, 
                  new_tablespace in varchar2, 
                  int_table_name in varchar2     :=null,
                  new_ind_tablespace in varchar2 :=null,
                  new_lob_tablespace in varchar2 :=null,
                  runcrt boolean                 :=false, --set to true to create interim table
                  runredef boolean              :=false, --set to true to run the redefinition job in this session
                  genredefjob boolean            :=false, --set to true to generate the commands to submit to the dbms_scheduler
                  runredefjob boolean            :=false, --set to true to submit to the scheduler
                  drop_interim_table boolean     :=false, --set to true to drop the interim table automatically after job completion when running interactively
                  copy_statistics boolean        :=true)  --set to true to copy stats when running interactively. Scheduler method will always copy stats.
authid current_user
as
h  number;
th number;
h2 number;
th2 number;
cmd_string varchar2(2000);
ind_move varchar2(2000);
ddl_text varchar2(32000);
ddl_text2 varchar2(32000);
orig_string varchar2(500);
new_string varchar2(500);
orig_string2 varchar2(500);
new_string2 varchar2(500);
orig_string3 varchar2(500);
new_string3 varchar2(500);
int_tabname varchar2(35);
int_tabcounter number :=0;
start_redef varchar2(32000);
copy_dependents varchar2(500);
finish_redef varchar2(500);
abort_redef varchar2(500);
sync_interim varchar2(500);
redef_errors number(5):=0;
opts_flag varchar2(200);
nopts_flag number;
ind_defined number :=0;
success boolean :=false;
dropcmd varchar2(2000);
dropunused varchar2(2000);
col_map_string varchar2(32000);
has_pk boolean := false;
pk_count int :=0;
ssize number;
work_tabname varchar2(30);
copy_stats varchar2(10) :='TRUE';
move_inds boolean :=FALSE;
w_runcrt boolean :=TRUE;
--indcounter number;
begin
  if (runredefjob=TRUE or runredef=TRUE)
  then
    w_runcrt:=TRUE;
  else
    w_runcrt:=runcrt;
  end if;
  dbms_output.enable(1000000);
  if (int_table_name is null)
  then
    ssize:=length(in_table_name);
    if ssize > 26
    then
      work_tabname:=substr(in_table_name,1,26);
    else
      work_tabname:=in_table_name;
    end if;
    int_tabname:=work_tabname||'_INT';
  end if;
  select count(1) into pk_count
  from all_constraints where owner=in_owner and table_name=in_table_name and constraint_type='P' and status='ENABLED';
--  dbms_output.put_line(to_char(pk_count));
  if pk_count=1 then
    has_pk:=true;
  else
    has_pk:=false;
  end if;
  pk_count:=0;
--  dbms_output.put_line(rtrim(owner)||'.'||rtrim(table_name));
  h :=     dbms_metadata.open('TABLE');
  dbms_metadata.set_filter(h,'SCHEMA',in_owner);
  dbms_metadata.set_filter(h,'NAME',in_table_name);
  th :=     dbms_metadata.add_transform(h,'MODIFY');
  if new_tablespace is not null
  then
    dbms_metadata.set_remap_param(th,'REMAP_TABLESPACE',tablespace_name,new_tablespace);
  end if;
  if int_table_name is null
  then
    int_tabname:=in_table_name||'_INT';
  else
    int_tabname:=int_table_name;
  end if;
  --map cols is used if actually changing the layout of a table.  in this case we are just degragmenting a table so no need to map columns
  --map_cols(in_owner=>in_owner,in_table_name=>in_table_name,col_map_string=>col_map_string);
  th :=     dbms_metadata.add_transform(h,'DDL');
  dbms_metadata.set_transform_param(th,'STORAGE',false);
  dbms_metadata.set_transform_param(th,'CONSTRAINTS',false);
  dbms_metadata.set_transform_param(th,'REF_CONSTRAINTS',false);
  if w_runcrt = true
  then
    dbms_metadata.set_transform_param(th,'PRETTY',false);
    dbms_metadata.set_transform_param(th,'SQLTERMINATOR',false);
  else
    dbms_metadata.set_transform_param(th,'PRETTY',true);
    dbms_metadata.set_transform_param(th,'SQLTERMINATOR',true);
  end if; 
  orig_string:='CREATE TABLE "'||rtrim(in_owner)||'"'||'.'||'"'||rtrim(in_table_name)||'"';
  new_string:='create table "'||in_owner||'"."'||int_tabname||'"';
  if copy_statistics=true
  then
    copy_stats:='TRUE';
  else
    copy_stats:='FALSE';
  end if;
  --orig_string2:='VARCHAR2(26)';
  --new_string2:='TIMESTAMP';
  --orig_string3:='CHAR(26)';
  --new_string3:='TIMESTAMP';
  --select replace(replace(replace(replace(dbms_metadata.fetch_clob(h),orig_string,new_string),' CHAR','VARCHAR2'),orig_string2,new_string2),orig_string3,new_string3)
  --into ddl_text from dual;
  --select replace(dbms_metadata.fetch_clob(h),orig_string,new_string) into ddl_text from dual;
  orig_string2:='';
  new_string2:='';
  if new_lob_tablespace is not null
  then
    orig_string2:='STORE AS SECUREFILE (
  TABLESPACE "'||new_tablespace||'" ENABLE STORAGE IN ROW ';
    new_string2:='STORE AS SECUREFILE (
  TABLESPACE "'||new_lob_tablespace||'" DISABLE STORAGE IN ROW ';
  end if;
--  orig_string2:='COLUMN STORE COMPRESS FOR QUERY HIGH NO ROW LEVEL LOCKING';
--  new_string2:='NOCOMPRESS ';
  select ltrim(replace(replace(dbms_metadata.fetch_clob(h),orig_string,new_string),orig_string2,new_string2)) into ddl_text from dual;
  if has_pk=false
  then
    opts_flag:='dbms_redefinition.cons_use_rowid';
    nopts_flag:=dbms_redefinition.cons_use_rowid;
  else
    opts_flag:='dbms_redefinition.cons_use_pk';
    nopts_flag:=dbms_redefinition.cons_use_pk;
  end if;
  if (genredefjob=FALSE and runredef=FALSE)
  then
    dbms_output.put_line(rtrim(ltrim(ddl_text)));
  end if;
--  mod_interim_table(in_owner,in_table_name,int_table_name,false);
  start_redef:='dbms_redefinition.start_redef_table(uname=>'||''''||in_owner||''''||',orig_table=>'||''''||in_table_name||''''||',int_table=>'||''''||int_tabname||''''||',options_flag=>'||opts_flag||')';
  copy_dependents:='dbms_redefinition.copy_table_dependents(uname=>'||''''||in_owner||''''||',orig_table=>'||''''||in_table_name||''''||',int_table=>'||''''||int_tabname||''''||',copy_statistics=>'||copy_stats||',num_errors=>redef_errors)';
  finish_redef:='dbms_redefinition.finish_redef_table(uname=>'||''''||in_owner||''''||',orig_table=>'||''''||in_table_name||''''||',int_table=>'||''''||int_tabname||''''||')';
  sync_interim:='dbms_redefinition.sync_interim_table(uname=>'||''''||in_owner||''''||',orig_table=>'||''''||in_table_name||''''||',int_table=>'||''''||int_tabname||''''||')';
  abort_redef:='dbms_redefinition.abort_redef_table(uname=>'||''''||in_owner||''''||',orig_table=>'||''''||in_table_name||''''||',int_table=>'||''''||int_tabname||''''||')';
  dropcmd:='drop table '||in_owner||'.'||int_tabname||' cascade constraints';
  dropunused:='alter table '||in_owner||'.'||in_table_name||' drop unused columns';
  if (genredefjob=FALSE and runredef=FALSE)
  then
    dbms_output.put_line('exec '||start_redef||';');
    dbms_output.put_line('declare');
    dbms_output.put_line('  redef_errors PLS_INTEGER;');
    dbms_output.put_line('begin');
    dbms_output.put_line('  '||copy_dependents||';');
    dbms_output.put_line('end;');
    dbms_output.put_line('/');
--  dbms_output.put_line('exec '||copy_dependents||';');
    dbms_output.put_line('exec '||sync_interim||';');
    dbms_output.put_line('exec '||finish_redef||';');
 --   dbms_output.put_line('--exec '||abort_redef||';');
 --  dbms_output.put_line('col_map_string::'||col_map_string);
    dbms_output.put_line(dropcmd||';');
    dbms_output.put_line(dropunused||';');
  end if;
  if (new_ind_tablespace is not null and runredef=false)
  then
    for indcounter in (select owner, index_name from dba_indexes where table_name=in_table_name and table_owner=in_owner and index_type !='LOB')
    loop
      move_inds:=TRUE;
      ind_move:='alter index '||indcounter.owner||'.'||indcounter.index_name||' rebuild online tablespace '||new_ind_tablespace||' parallel compute statistics;';
      dbms_output.put_line(ind_move);
    end loop;
  end if;
  if w_runcrt=true
  then
--    dbms_output.put_line('Start redef');
    execute immediate ddl_text;
--    mod_interim_table(in_owner,in_table_name,int_table_name,runcrt);
--    dbms_output.put_line('DDL TEXTED');
    if runredef=true
    then
      begin
--        dbms_output.put_line(start_redef);
        if (new_ind_tablespace is not null)
        then
          for indcounter in (select owner, index_name from dba_indexes where table_name=in_table_name and table_owner=in_owner and index_type !='LOB')
          loop
            ind_move:='alter index '||indcounter.owner||'.'||indcounter.index_name||' rebuild online tablespace '||new_ind_tablespace||' parallel compute statistics';
            execute immediate ind_move;
--            dbms_output.put_line(ind_move);
          end loop;
        end if;
        dbms_redefinition.start_redef_table(uname=>in_owner,orig_table=>in_table_name,int_table=>int_tabname,options_flag=>nopts_flag);
--        dbms_output.put_line('Finished start');
--        dbms_output.put_line(copy_dependents);
        dbms_redefinition.copy_table_dependents(uname=>in_owner,orig_table=>in_table_name,int_table=>int_tabname,num_errors=>redef_errors);
        if (redef_errors > 0)
        then
          dbms_output.put_line('Dependency Errors: '||to_char(redef_errors));
          dbms_output.put_line('Run the following commands to clean up:');
          dbms_output.put_line(abort_redef);
          dbms_output.put_line(dropcmd);
          dbms_output.put_line(dropunused);
        end if;
--        dbms_output.put_line(finish_redef);
        dbms_redefinition.finish_redef_table(uname=>in_owner,orig_table=>in_table_name,int_table=>int_tabname);
        success:=TRUE;
--        dbms_output.put_line(dropcmd);
        if (success=TRUE)
        then
          execute immediate dropunused;
          execute immediate dropcmd;
        end if;
        exception
        when others then
        success:=false;
        dbms_output.put_line('ABORTED');
        dbms_redefinition.abort_redef_table(uname=>in_owner,orig_table=>in_table_name,int_table=>int_tabname);
        raise;
      end;
    end if;
  end if;
  has_pk:=false;
  dbms_output.put_line('/* NOTE:  You must run the following commands to cleanup if jobs failed.  Remove the comments (--) to run: */ ');
  dbms_output.put_line('--'||abort_redef||';');
  dbms_output.put_line('--'||dropcmd||';');
  dbms_output.put_line('--'||dropunused||';');
  if (genredefjob=TRUE)
  then
--    dbms_output.put_line('NOTE:  You must run the following commands to cleanup after successful completion: ');
--    dbms_output.put_line(dropcmd);
--    dbms_output.put_line(dropunused);
    sched_redef_job(in_owner,in_table_name,int_tabname,8,'BOTH',runredefjob,move_inds,new_ind_tablespace);
  end if;
 end;

/