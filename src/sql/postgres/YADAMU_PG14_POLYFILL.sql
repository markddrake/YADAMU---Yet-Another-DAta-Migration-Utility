DO 
$x$
BEGIN
IF count(*) = 0 FROM pg_proc where proname = 'trim_array' THEN
--
CREATE FUNCTION trim_array(point[],int) 
/*
**
** Polyfill for PG-13 TRIM_ARRAY function
**
*/
RETURNS point[] 
AS
$$
select array_agg(p) 
  from unnest($1) with ordinality as u(p,i) 
 where i < array_length($1,1);
$$
LANGUAGE SQL;
--
END IF;
END;
$x$;
--
DO $x$
BEGIN
IF count(*) = 0 FROM pg_proc where proname = 'trim_scale' THEN
--
CREATE FUNCTION  trim_scale(numeric) 
/*
**
** Polyfill for PG-13 trim_scale function
**
*/
RETURNS numeric 
AS 
$$
SELECT CASE WHEN $1 = 0 then 0 WHEN trim($1::text, '0')::numeric = $1 THEN trim($1::text, '0')::numeric ELSE $1 END 
$$
LANGUAGE SQL;
--
END IF;
END;
$x$;
--
DO 
$x$
BEGIN
IF count(*) = 0 FROM pg_proc where proname = 'gen_random_uuid' THEN
--
CREATE FUNCTION  gen_random_uuid() 
/*
**
** Polyfill for PG-13 gen_random_uuid function
**
*/
RETURNS uuid 
AS 
$$
SELECT uuid_in(overlay(overlay(md5(random()::text || ':' || clock_timestamp()::text) placing '4' from 13) placing to_hex(floor(random()*(11-8+1) + 8)::int)::text from 17)::cstring)
$$
LANGUAGE SQL;
--
END IF;
END;
$x$;
