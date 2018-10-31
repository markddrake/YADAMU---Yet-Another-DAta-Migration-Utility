@SET TGT=%~1
@SET VER=%~2
@SET ID=%~3
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=Northwind%ID%        --OWNER=\"dbo\"            --FILE=%TGT%\Northwind%VER%.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks%ID%   --OWNER=\"Sales\"          --FILE=%TGT%\Sales%VER%.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks%ID%   --OWNER=\"Person\"         --FILE=%TGT%\Person%VER%.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks%ID%   --OWNER=\"Production\"     --FILE=%TGT%\Production%VER%.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks%ID%   --OWNER=\"Purchasing\"     --FILE=%TGT%\Purchasing%VER%.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorks%ID%   --OWNER=\"HumanResources\" --FILE=%TGT%\HumanResources%VER%.json
node node\export --USERNAME=sa --PASSWORD=oracle --HOSTNAME=192.168.1.250 --DATABASE=AdventureWorksDW%ID% --OWNER=\"dbo\"            --FILE=%TGT%\AdventureWorksDW%VER%.json
