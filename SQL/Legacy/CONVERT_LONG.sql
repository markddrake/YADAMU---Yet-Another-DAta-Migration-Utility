set echo on
--

create or replace function LONG2CLOB(P_LONG LONG)
return CLOB
as
  V_CLOB CLOB;
begin
  V_CLOB := P_LONG;
  return V_CLOB;
end;
/
create or replace function LONGRAW2BLOB(P_LONGRAW LONG RAW)
return BLOB
as
  V_BLOB BLOB;
begin
  V_BLOB := P_LONGRAW;
  return V_BLOB;
end;
/


	