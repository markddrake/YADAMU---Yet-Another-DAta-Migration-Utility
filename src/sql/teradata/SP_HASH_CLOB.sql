replace procedure YADAMU.HASH_CLOB(in P_CLOB clob)
sql security invoker
begin
    declare V_START_POS integer;
	declare V_END_POS integer;
	declare V_LENGTH integer;
	declare V_HASH_VALUE VARCHAR(32000);
	declare V_CHUNK VARCHAR(32000);
	declare V_CHUNK_HASH VARCHAR(8);
	
	set V_LENGTH = characters(P_CLOB);
	set V_START_POS = 1;
	set V_HASH_VALUE = '';
	
	while (V_START_POS <= V_LENGTH) DO
	  set V_END_POS = V_START_POS + 31999;
	  set V_CHUNK =  SUBSTR(P_CLOB,V_START_POS,V_END_POS);
	  set V_CHUNK_HASH = from_bytes('00'xb || HASHROW(V_CHUNK),'base16');
	  set V_HASH_VALUE = V_HASH_VALUE ||V_CHUNK_HASH;
	  if (characters(V_HASH_VALUE) = 32000) then
	    set V_HASH_VALUE = from_bytes('00'xb || HASHROW(V_HASH_VALUE),'base16');
      end if;
	  set V_START_POS = V_END_POS + 1;
	end while;
	if (characters(V_HASH_VALUE) > 8) then
      set V_HASH_VALUE = from_bytes('00'xb || HASHROW(V_HASH_VALUE),'base16');
	end if;
end;