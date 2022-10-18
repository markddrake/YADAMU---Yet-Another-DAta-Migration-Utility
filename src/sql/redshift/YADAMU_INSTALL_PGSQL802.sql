CREATE OR REPLACE FUNCTION execute(TEXT) RETURNS VOID AS $$
BEGIN EXECUTE $1; RETURN; END;
$$ LANGUAGE plpgsql STRICT;
--
CREATE OR REPLACE FUNCTION public.fn_uuid()
RETURNS character varying AS
' import uuid
 return uuid.uuid4().__str__()
 '
LANGUAGE plpythonu VOLATILE;
--
CREATE OR REPLACE PROCEDURE YADAMU_INSTALL()
as
$$
declare
   V_COMMAND                 character varying (256);
   V_RESULT                  int;
   V_INSTALLATION_TIMESTAMP  character varying(29);
begin

  select count(*),
  		 to_char(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM')
		 into V_RESULT, V_INSTALLATION_TIMESTAMP
         from pg_proc where proname = 'yadamu_instance_id';
             
  if (V_RESULT = 0)  then
    V_COMMAND  := 'CREATE OR REPLACE FUNCTION YADAMU_INSTANCE_ID() RETURNS CHARACTER VARYING STABLE AS $X$ select ''' || upper(fn_uuid()::CHAR(36)) || ''' $X$ LANGUAGE SQL';
    EXECUTE V_COMMAND;
  end if;
		  
  V_COMMAND  := 'CREATE OR REPLACE FUNCTION YADAMU_INSTALLATION_TIMESTAMP() RETURNS CHARACTER VARYING STABLE AS $X$ select ''' || V_INSTALLATION_TIMESTAMP || ''' $X$ LANGUAGE SQL';
  EXECUTE V_COMMAND;
end;
--
$$ LANGUAGE plpgsql;
--
select YADAMU_INSTALL();
--
select YADAMU_INSTANCE_ID(), YADAMU_INSTALLATION_TIMESTAMP();
--
exit