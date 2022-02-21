mkdir src\node
mkdir src\node\cfg
mkdir src\node\clarinet
mkdir src\node\cli
mkdir src\node\core
mkdir src\node\dbi
mkdir src\node\deprecated
mkdir src\node\gui
mkdir src\node\lib
mkdir src\node\server
mkdir src\node\util
mkdir src\node\dbi\awsS3
mkdir src\node\dbi\azure
mkdir src\node\dbi\base
mkdir src\node\dbi\cfg
mkdir src\node\dbi\cloud
mkdir src\node\dbi\example
mkdir src\node\dbi\file
mkdir src\node\dbi\loader
mkdir src\node\dbi\mariadb
mkdir src\node\dbi\mongodb
mkdir src\node\dbi\mssql
mkdir src\node\dbi\mssql\2014
mkdir src\node\dbi\mysql
mkdir src\node\dbi\mysql\57
mkdir src\node\dbi\oracle
mkdir src\node\dbi\oracle\112
mkdir src\node\dbi\oracle\18
mkdir src\node\dbi\postgres
mkdir src\node\dbi\redshift
mkdir src\node\dbi\shared
mkdir src\node\dbi\snowflake
mkdir src\node\dbi\vertica
mkdir src\qa
mkdir src\qa\cfg
mkdir src\qa\cli
mkdir src\qa\core
mkdir src\qa\dbi
mkdir src\qa\lib
mkdir src\qa\util
mkdir src\qa\dbi\awsS3
mkdir src\qa\dbi\azure
mkdir src\qa\dbi\file
mkdir src\qa\dbi\loader
mkdir src\qa\dbi\mariadb
mkdir src\qa\dbi\mongodb
mkdir src\qa\dbi\mssql
mkdir src\qa\dbi\mysql
mkdir src\qa\dbi\oracle
mkdir src\qa\dbi\postgres
mkdir src\qa\dbi\redshift
mkdir src\qa\dbi\snowflake
mkdir src\qa\dbi\vertica

git mv src\YADAMU\common\yadamuDefaults.json		      src\node\cfg
													     
git mv src\YADAMU\clarinet\clarinet.js                    src\node\clarinet
git mv src\YADAMU\clarinet\clarinet.cjs                   src\node\clarinet
													     
git mv src\YADAMU\common\copy.js                          src\node\cli
git mv src\YADAMU\common\decrypt.js                       src\node\cli
git mv src\YADAMU\common\directLoad.js                    src\node\cli
git mv src\YADAMU\common\encrypt.js                       src\node\cli
git mv src\YADAMU\common\export.js                        src\node\cli
git mv src\YADAMU\common\import.js                        src\node\cli
git mv src\YADAMU\common\load.js                          src\node\cli
git mv src\YADAMU\common\unload.js                        src\node\cli
git mv src\YADAMU\common\upload.js                        src\node\cli
git mv src\YADAMU\common\yadamuCLI.js                     src\node\cli
git mv src\YADAMU\common\yadamuCmd.js                     src\node\cli
													     
git mv src\YADAMU\common\dbReader.js                      src\node\core
git mv src\YADAMU\common\dbReaderFile.js                  src\node\core
git mv src\YADAMU\common\dbReaderParallel.js              src\node\core
git mv src\YADAMU\common\dbWriter.js                      src\node\core
git mv src\YADAMU\common\yadamu.js                        src\node\core
git mv src\YADAMU\common\yadamuException.js               src\node\core
git mv src\YADAMU\common\yadamuLogger.js                  src\node\core
git mv src\YADAMU\common\yadamuRejectManager.js           src\node\core
													     
git mv src\YADAMU\common\defaultParser.js                 src\node\deprecated
													     
git mv src\YADAMU\common\yadamuGUI.js                     src\node\gui
git mv src\YADAMU_UI\node\logWriter.js                    src\node\gui
git mv src\YADAMU_UI\node\yuiControls.js                  src\node\gui
git mv src\YADAMU_UI\node\yuiLogWindow.js                 src\node\gui
													     
													     
git mv src\YADAMU\common\yadamuConstants.js               src\node\lib
git mv src\YADAMU\common\yadamuLibrary.js                 src\node\lib
git mv src\YADAMU\common\yadamuSpatialLibrary.js          src\node\lib
													     
git mv src\YADAMU_SVR\node\httpDBI.js                     src\node\server
git mv src\YADAMU_SVR\node\main.js                        src\node\server
git mv src\YADAMU_SVR\node\yadamuRouter.js                src\node\server
git mv src\YADAMU_SVR\node\yadamuServer.js                src\node\server
													     
git mv src\YADAMU\common\arrayReadable.js                 src\node\util
git mv src\YADAMU\common\bufferWriter.js                  src\node\util
git mv src\YADAMU\common\hexBinToBinary.js                src\node\util
git mv src\YADAMU\common\nullWritable.js                  src\node\util
git mv src\YADAMU\common\nullWriter.js                    src\node\util
git mv src\YADAMU\common\stringDecoderStream.js           src\node\util
git mv src\YADAMU\common\stringWriter.js                  src\node\util
git mv src\YADAMU\common\performanceReporter.js           src\node\util													     
													     
git mv src\YADAMU\common\dbiConstants.js                  src\node\dbi\base
git mv src\YADAMU\common\yadamuCopyManager.js             src\node\dbi\base
git mv src\YADAMU\common\yadamuDBI.js                     src\node\dbi\base
git mv src\YADAMU\common\yadamuOutputManager.js           src\node\dbi\base
git mv src\YADAMU\common\yadamuParser.js                  src\node\dbi\base
git mv src\YADAMU\common\yadamuWriter.js                  src\node\dbi\base

git mv src\YADAMU\dbShared\mysql\57\statementGenerator.js src\node\dbi\shared\mysql\57
													      
git mv src\YADAMU\example\node\exampleConstants.js         src\node\dbi\example
git mv src\YADAMU\example\node\exampleDBI.js               src\node\dbi\example
git mv src\YADAMU\example\node\exampleException.js         src\node\dbi\example
git mv src\YADAMU\example\node\exampleOutputManager.js     src\node\dbi\example
git mv src\YADAMU\example\node\exampleParser.js            src\node\dbi\example
git mv src\YADAMU\example\node\exampleReader.js            src\node\dbi\example
git mv src\YADAMU\example\node\exampleWriter.js            src\node\dbi\example
git mv src\YADAMU\example\node\statementGenerator.js       src\node\dbi\example
													     
git mv src\YADAMU\file\node\errorDBI.js                   src\node\dbi\file
git mv src\YADAMU\file\node\errorOutputManager.js         src\node\dbi\file
git mv src\YADAMU\file\node\fileDBI.js                    src\node\dbi\file
git mv src\YADAMU\file\node\fileException.js              src\node\dbi\file
git mv src\YADAMU\file\node\jsonOutputManager.js          src\node\dbi\file
git mv src\YADAMU\file\node\jsonParser.js                 src\node\dbi\file
git mv src\YADAMU\file\node\streamSwitcher.js             src\node\dbi\file

git mv src\YADAMU\loader\awsS3\awsS3Constants.js          src\node\dbi\awsS3 
git mv src\YADAMU\loader\awsS3\awsS3DBI.js                src\node\dbi\awsS3
git mv src\YADAMU\loader\awsS3\awsS3Exception.js          src\node\dbi\awsS3
git mv src\YADAMU\loader\awsS3\awsS3StorageService.js     src\node\dbi\awsS3

git mv src\YADAMU\loader\azure\azureConstants.js          src\node\dbi\azure
git mv src\YADAMU\loader\azure\azureDBI.js                src\node\dbi\azure
git mv src\YADAMU\loader\azure\azureException.js          src\node\dbi\azure
git mv src\YADAMU\loader\azure\azureStorageService.js     src\node\dbi\azure

git mv src\YADAMU\loader\node\cloudDBI.js                 src\node\dbi\cloud

git mv src\YADAMU\loader\node\arrayOutputManager.js       src\node\dbi\loader
git mv src\YADAMU\loader\node\csvLibrary.js               src\node\dbi\loader
git mv src\YADAMU\loader\node\csvOutputManager.js         src\node\dbi\loader
git mv src\YADAMU\loader\node\csvTransform.js             src\node\dbi\loader
git mv src\YADAMU\loader\node\jsonOutputManager.js        src\node\dbi\loader
git mv src\YADAMU\loader\node\jsonParser.js               src\node\dbi\loader
git mv src\YADAMU\loader\node\loaderConstants.js          src\node\dbi\loader
git mv src\YADAMU\loader\node\loaderDBI.js                src\node\dbi\loader
git mv src\YADAMU\loader\node\loaderParser.js             src\node\dbi\loader
              
git mv src\YADAMU\mariadb\node\mariadbConstants.js        src\node\dbi\mariadb
git mv src\YADAMU\mariadb\node\mariadbDBI.js              src\node\dbi\mariadb
git mv src\YADAMU\mariadb\node\mariadbException.js        src\node\dbi\mariadb
git mv src\YADAMU\mariadb\node\mariadbOutputManager.js    src\node\dbi\mariadb
git mv src\YADAMU\mariadb\node\mariadbParser.js           src\node\dbi\mariadb
git mv src\YADAMU\mariadb\node\mariadbStatementLibrary.js src\node\dbi\mariadb
git mv src\YADAMU\mariadb\node\mariadbWriter.js           src\node\dbi\mariadb

git mv src\YADAMU\mongodb\node\mongoConstants.js          src\node\dbi\mongodb
git mv src\YADAMU\mongodb\node\mongoDBI.js                src\node\dbi\mongodb
git mv src\YADAMU\mongodb\node\mongoException.js          src\node\dbi\mongodb
git mv src\YADAMU\mongodb\node\mongoOutputManager.js      src\node\dbi\mongodb
git mv src\YADAMU\mongodb\node\mongoParser.js             src\node\dbi\mongodb
git mv src\YADAMU\mongodb\node\mongoWriter.js             src\node\dbi\mongodb
git mv src\YADAMU\mongodb\node\statementGenerator.js      src\node\dbi\mongodb
               
 
git mv src\YADAMU\mssql\node\2014\mssqlStatementLibrary.js src\node\dbi\mssql\2014
git mv src\YADAMU\mssql\node\2014\statementGenerator.js    src\node\dbi\mssql\2014
git mv src\YADAMU\mssql\node\dbFileLoader.js               src\node\dbi\mssql
git mv src\YADAMU\mssql\node\mssqlConstants.js             src\node\dbi\mssql
git mv src\YADAMU\mssql\node\mssqlDBI.js                   src\node\dbi\mssql
git mv src\YADAMU\mssql\node\mssqlException.js             src\node\dbi\mssql
git mv src\YADAMU\mssql\node\mssqlOutputManager.js         src\node\dbi\mssql
git mv src\YADAMU\mssql\node\mssqlParser.js                src\node\dbi\mssql
git mv src\YADAMU\mssql\node\mssqlReader.js                src\node\dbi\mssql
git mv src\YADAMU\mssql\node\mssqlStatementLibrary.js      src\node\dbi\mssql
git mv src\YADAMU\mssql\node\mssqlWriter.js                src\node\dbi\mssql
git mv src\YADAMU\mssql\node\stagingTable.js               src\node\dbi\mssql
git mv src\YADAMU\mssql\node\statementGenerator.js         src\node\dbi\mssql
    
git mv src\YADAMU\mysql\node\57\mysqlStatementLibrary.js  src\node\dbi\mysql\57
git mv src\YADAMU\mysql\node\mysqlConstants.js            src\node\dbi\mysql
git mv src\YADAMU\mysql\node\mysqlDBI.js                  src\node\dbi\mysql
git mv src\YADAMU\mysql\node\mysqlException.js            src\node\dbi\mysql
git mv src\YADAMU\mysql\node\mysqlOutputManager.js        src\node\dbi\mysql
git mv src\YADAMU\mysql\node\mysqlParser.js               src\node\dbi\mysql
git mv src\YADAMU\mysql\node\mysqlStatementLibrary.js     src\node\dbi\mysql
git mv src\YADAMU\mysql\node\mysqlWriter.js               src\node\dbi\mysql
git mv src\YADAMU\mysql\node\statementGenerator.js        src\node\dbi\mysql
               
git mv src\YADAMU\oracle\node\112\oracleStatementLibrary.js src\node\dbi\oracle\112
git mv src\YADAMU\oracle\node\112\statementGenerator.js     src\node\dbi\oracle\112
git mv src\YADAMU\oracle\node\18\oracleStatementLibrary.js  src\node\dbi\oracle\18
git mv src\YADAMU\oracle\node\oracleConstants.js            src\node\dbi\oracle
git mv src\YADAMU\oracle\node\oracleDBI.js                  src\node\dbi\oracle
git mv src\YADAMU\oracle\node\oracleException.js            src\node\dbi\oracle
git mv src\YADAMU\oracle\node\oracleOutputManager.js        src\node\dbi\oracle
git mv src\YADAMU\oracle\node\oracleParser.js               src\node\dbi\oracle
git mv src\YADAMU\oracle\node\oracleStatementLibrary.js     src\node\dbi\oracle
git mv src\YADAMU\oracle\node\oracleWriter.js               src\node\dbi\oracle
git mv src\YADAMU\oracle\node\statementGenerator.js         src\node\dbi\oracle
               
git mv src\YADAMU\postgres\node\postgresConstants.js        src\node\dbi\postgres
git mv src\YADAMU\postgres\node\postgresDBI.js              src\node\dbi\postgres
git mv src\YADAMU\postgres\node\postgresException.js        src\node\dbi\postgres
git mv src\YADAMU\postgres\node\postgresOutputManager.js    src\node\dbi\postgres
git mv src\YADAMU\postgres\node\postgresParser.js           src\node\dbi\postgres
git mv src\YADAMU\postgres\node\postgresStatementLibrary.js src\node\dbi\postgres
git mv src\YADAMU\postgres\node\postgresWriter.js           src\node\dbi\postgres
git mv src\YADAMU\postgres\node\statementGenerator.js       src\node\dbi\postgres

git mv src\YADAMU\redshift\node\redshiftConstants.js         src\node\dbi\redshift
git mv src\YADAMU\redshift\node\redshiftDBI.js               src\node\dbi\redshift
git mv src\YADAMU\redshift\node\redshiftException.js         src\node\dbi\redshift
git mv src\YADAMU\redshift\node\redshiftOutputManager.js     src\node\dbi\redshift
git mv src\YADAMU\redshift\node\redshiftParser.js            src\node\dbi\redshift
git mv src\YADAMU\redshift\node\redshiftStatementLibrary.js  src\node\dbi\redshift
git mv src\YADAMU\redshift\node\redshiftWriter.js            src\node\dbi\redshift
git mv src\YADAMU\redshift\node\statementGenerator.js        src\node\dbi\redshift
															 
git mv src\YADAMU\snowflake\node\snowflakeConstants.js        src\node\dbi\snowflake
git mv src\YADAMU\snowflake\node\snowflakeDBI.js              src\node\dbi\snowflake
git mv src\YADAMU\snowflake\node\snowflakeException.js        src\node\dbi\snowflake
git mv src\YADAMU\snowflake\node\snowflakeOutputManager.js    src\node\dbi\snowflake
git mv src\YADAMU\snowflake\node\snowflakeParser.js           src\node\dbi\snowflake
git mv src\YADAMU\snowflake\node\snowflakeReader.js           src\node\dbi\snowflake
git mv src\YADAMU\snowflake\node\snowflakeStatementLibrary.js src\node\dbi\snowflake
git mv src\YADAMU\snowflake\node\snowflakeWriter.js           src\node\dbi\snowflake
git mv src\YADAMU\snowflake\node\statementGenerator.js        src\node\dbi\snowflake
															 															 
git mv src\YADAMU\vertica\node\statementGenerator.js        src\node\dbi\vertica
git mv src\YADAMU\vertica\node\verticaConstants.js          src\node\dbi\vertica
git mv src\YADAMU\vertica\node\verticaDBI.js                src\node\dbi\vertica
git mv src\YADAMU\vertica\node\verticaException.js          src\node\dbi\vertica
git mv src\YADAMU\vertica\node\verticaOutputManager.js      src\node\dbi\vertica
git mv src\YADAMU\vertica\node\verticaParser.js             src\node\dbi\vertica
git mv src\YADAMU\vertica\node\verticaReader.js             src\node\dbi\vertica
git mv src\YADAMU\vertica\node\verticaStatementLibrary.js   src\node\dbi\vertica
git mv src\YADAMU\vertica\node\verticaWriter.js             src\node\dbi\vertica


git mv src\YADAMU_QA\common\node\compareRules.json          src\qa\cfg
git mv src\YADAMU_QA\common\node\yadamuDefaults.json        src\qa\cfg
															
git mv src\YADAMU_QA\common\node\test.js                    src\qa\cli
															
git mv src\YADAMU_QA\common\node\yadamuQALibrary.js         src\qa\lib
															
git mv src\YADAMU_QA\common\node\yadamuTest.js              src\qa\core\yadamu.js
git mv src\YADAMU_QA\common\node\yadamuLogger.js            src\qa\core
git mv src\YADAMU_QA\common\node\yadamuMetrics.js           src\qa\core
git mv src\YADAMU_QA\common\node\yadamuQA.js                src\qa\core
															
git mv src\YADAMU_QA\common\node\rowCounter.js              src\qa\util

git mv src\YADAMU_QA\file\node\fileQA.js                    src\qa\dbi\file
git mv src\YADAMU_QA\file\node\statisticsCollector.js       src\qa\dbi\file

git mv src\YADAMU_QA\loader\awsS3\awsS3QA.js                src\qa\dbi\awsS3
git mv src\YADAMU_QA\loader\azure\azureQA.js                src\qa\dbi\azure
git mv src\YADAMU_QA\loader\node\loaderQA.js                src\qa\dbi\loader
git mv src\YADAMU_QA\loader\node\arrayWriter.js             src\qa\dbi\loader

git mv src\YADAMU_QA\mariadb\node\mariadbQA.js              src\qa\dbi\mariadb
git mv src\YADAMU_QA\mongodb\node\mongoQA.js                src\qa\dbi\mongodb
git mv src\YADAMU_QA\mssql\node\mssqlQA.js                  src\qa\dbi\mssql
git mv src\YADAMU_QA\mysql\node\mysqlQA.js                  src\qa\dbi\mysql
git mv src\YADAMU_QA\oracle\node\oracleQA.js                src\qa\dbi\oracle
git mv src\YADAMU_QA\postgres\node\postgresQA.js            src\qa\dbi\postgres
git mv src\YADAMU_QA\redshift\node\redshiftQA.js            src\qa\dbi\redshift
git mv src\YADAMU_QA\snowflake\node\snowflakeQA.js          src\qa\dbi\snowflake
git mv src\YADAMU_QA\vertica\node\verticaQA.js              src\qa\dbi\vertica

git mv src\YADAMU_QA\utilities\node\compareArrayContent.js   src\qa\util
git mv src\YADAMU_QA\utilities\node\compareFileSizes.js      src\qa\util


git mv YADAMU_UI\html\index.html html\gui
git mv YADAMU_UI\html\logWindow.html html\gui
git mv YADAMU_UI\js\yuiControls.cjs js\gui
git mv YADAMU_UI\js\yuiLogWindow.cjs js\gui