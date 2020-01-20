create or replace procedure index_move(in_table_owner in varchar2,in_table_name in varchar2,new_ind_tablespace in varchar2)
authid current_user as
begin
  for looper in (select owner,index_name from dba_indexes where table_owner=in_table_owner and table_name=in_table_name)
  loop
    execute immediate 'alter index '||looper.owner||'.'||looper.index_name||' rebuild online tablespace '||new_ind_tablespace||' parallel compute statistics';
  end loop;
end;