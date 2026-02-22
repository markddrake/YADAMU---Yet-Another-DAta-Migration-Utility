select
  "StateProvinceID",
  "StateProvinceCode",
  "StateProvinceName",
  "CountryID",
  "SalesTerritory",
  case
    when t."Border" is null then
      null
    when t."Border".st_isvalid() = 1 then
      t."Border".get_wkb()
    when sdo_geom.validate_geometry_with_context(
           t."Border",
           0.00001
         ) in ('NULL','13032') then
      null
    else
      t."Border".get_wkb()
  end as "Border",
  "LatestRecordedPopulation",
  "LastEditedBy",
  to_char(
    "ValidFrom",
    'YYYY-MM-DD"T"HH24:MI:SS.FF7"Z"'
  ),
  to_char(
    "ValidTo",
    'YYYY-MM-DD"T"HH24:MI:SS.FF7"Z"'
  )
from "WWI_Application2"."StateProvinces_Archive" t
