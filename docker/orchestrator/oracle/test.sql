set echo ON
--
var V1 varchar2(32)
var V2 CLOB
--
begin 
  :V1:='HR';
end;
/
--
@fixCrash.sql
--
print:V2
-- 
var V1 varchar2(32)
var V2 CLOB
--
begin 
  :V1:='SH';
end;
/
--
@fixCrash.sql
--
print:V2
-- 
var V1 varchar2(32)
var V2 CLOB
--
begin 
  :V1:='OE';
end;
/
--
@fixCrash.sql
--
print:V2
-- 
var V1 varchar2(32)
var V2 CLOB
--
begin 
  :V1:='PM';
end;
/
--
@fixCrash.sql
--
print:V2
-- 