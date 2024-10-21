function resetFolder($PATH) {

  if (Test-Path -Path $PATH) {
	Remove-Item -Recurse -Force -Path $PATH | Out-Null
  }

  New-Item -ItemType directory -force -Path $PATH | Out-Null
} 
	
function initializeLogging($TESTNAME) {

   $YADAMU_LOG_PATH = Join-Path -Path $YADAMU_LOG_ROOT -ChildPath $TESTNAME
   
   if (-not (Test-Path YADAMU_TIMESTAMP)) {
	 $YADAMU_TIMESTAMP = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHHmmss.fffZ')
   }
   
   $YADAMU_LOG_PATH = Join-Path -Path $YADAMU_LOG_PATH -ChildPath $YADAMU_TIMESTAMP
   New-Item -force -ItemType Directory $YADAMU_LOG_PATH | Out-Null
   $YADAMU_IMPORT_LOG = Join-Path -Path $YADAMU_LOG_PATH -ChildPath "yadamu.log"
   $YADAMU_EXPORT_LOG = Join-Path -Path $YADAMU_LOG_PATH -ChildPath "yadamu.log"
   return $YADAMU_LOG_PATH
}
 
function runRegressionTest($TESTNAME) {

  if ( -not(Test-Path NODE_NO_WARNINGS)) {
    $NODE_NO_WARNINGS=1;
  }
  
  $YADAMU_LOG_PATH = initializeLogging $TESTNAME

  resetFolder $YADAMU_LOG_PATH
  $YADAMU_LOG_FILE = join-path $YADAMU_LOG_PATH ($TESTNAME + ".log")
  node $YADAMU_HOME\src\qa\cli\test.js CONFIG=$YADAMU_QA_HOME\regression\$TESTNAME.json EXCEPTION_FOLDER=$YADAMU_LOG_PATH 2>&1 | tee-Object -FilePath $YADAMU_LOG_FILE
}

function createOutputFolders($PATH) {
	
  resetFolder $PATH

  New-Item -ItemType directory -force -Path (Join-Path $PATH "mysql" )  | Out-Null  
  New-Item -ItemType directory -force -Path (Join-Path $PATH "mssql")  | Out-Null
  New-Item -ItemType directory -force -Path (Join-Path $PATH "oracle" )  | Out-Null
  New-Item -ItemType directory -force -Path (Join-Path $PATH "cmdLine"     )  | Out-Null
  New-Item -ItemType directory -force -Path (Join-Path $PATH "postgres"      )  | Out-Null

}
 
function createSharedFolders($PATH) {
  New-Item -ItemType directory -force -Path (Join-Path $PATH "log"         )  | Out-Null
  New-Item -ItemType directory -force -Path (Join-Path $PATH "longRegress" )  | Out-Null  
  New-Item -ItemType directory -force -Path (Join-Path $PATH "shortRegress")  | Out-Null
  New-Item -ItemType directory -force -Path (Join-Path $PATH "stagingArea" )  | Out-Null
  New-Item -ItemType directory -force -Path (Join-Path $PATH "cmdLine"     )  | Out-Null
  New-Item -ItemType directory -force -Path (Join-Path $PATH "output"      )  | Out-Null
  New-Item -ItemType directory -force -Path (Join-Path $PATH "scratch"     )  | Out-Null
  New-Item -ItemType directory -force -Path (Join-Path $PATH "test"        )  | Out-Null
  New-Item -ItemType directory -force -Path (Join-Path $PATH "work"        )  | Out-Null
}

function linkOutputFolders($LINK_FOLDER,$TARGET_FOLDER) {
  New-Item -ItemType SymbolicLink -Path (Join-Path $LINK_FOLDER "log"         ) -Value (Join-Path $TARGET_FOLDER "log"         ) | Out-Null
  New-Item -ItemType SymbolicLink -Path (Join-Path $LINK_FOLDER "longRegress" ) -Value (Join-Path $TARGET_FOLDER "longRegress" ) | Out-Null  
  New-Item -ItemType SymbolicLink -Path (Join-Path $LINK_FOLDER "shortRegress") -Value (Join-Path $TARGET_FOLDER "shortRegress") | Out-Null
  New-Item -ItemType SymbolicLink -Path (Join-Path $LINK_FOLDER "stagingArea" ) -Value (Join-Path $TARGET_FOLDER "stagingArea" ) | Out-Null
  New-Item -ItemType SymbolicLink -Path (Join-Path $LINK_FOLDER "cmdLine"     ) -Value (Join-Path $TARGET_FOLDER "cmdLine"     ) | Out-Null
  New-Item -ItemType SymbolicLink -Path (Join-Path $LINK_FOLDER "output"      ) -Value (Join-Path $TARGET_FOLDER "output"      ) | Out-Null
  New-Item -ItemType SymbolicLink -Path (Join-Path $LINK_FOLDER "scratch"     ) -Value (Join-Path $TARGET_FOLDER "scratch"     ) | Out-Null
  New-Item -ItemType SymbolicLink -Path (Join-Path $LINK_FOLDER "test"        ) -Value (Join-Path $TARGET_FOLDER "test"        ) | Out-Null
  New-Item -ItemType SymbolicLink -Path (Join-Path $LINK_FOLDER "work"        ) -Value (Join-Path $TARGET_FOLDER "work"        ) | Out-Null
}                                                                                                                     


$YADAMU_HOME = Get-Location
$YADAMU_QA_HOME = Join-Path $YADAMU_HOME "qa"

$YADAMU_MOUNT_FOLDER = Join-Path $YADAMU_HOME "mnt"
createSharedFolders $YADAMU_MOUNT_FOLDER

$YADAMU_LOG_ROOT = Join-Path $YADAMU_MOUNT_FOLDER "log"

switch ($ENV:YADAMU_TEST_NAME) {
 
 "shortRegression" {
    source qa/bin/resetFolders.sh mnt/stagingArea/export/json/postgres
    runRegressionTest "shortRegression"
 }
  
  "export" {
     resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\mysql")
     resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\mssql")
     resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\oracle")
     runRegressionTest "export"
     break
  }
  
  "reset" {
     resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\mysql")
     resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\mssql")
     resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\oracle")
     resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\postgres")
     runRegressionTest "initialize"
     runRegressionTest "stageDataSets"
     runRegressionTest "import"
     runRegressionTest "importDataTypes"
     break
  }
    
  "all" {
    source qa/bin/createOutputFolders.sh mnt
    runRegressionTest "shortRegression"
    runRegressionTest "postgresDataTypes"
    runRegressionTest "export"
    runRegressionTest "import"
    runRegressionTest "fileRoundtrip"
    runRegressionTest "uploadRoundtrip"
    runRegressionTest "dbRoundtrip"
    runRegressionTest "mongoTestSuite"
    runRegressionTest "lostConnection"
    runRegressionTest "loaderTestSuite"
    runRegressionTest "awsTestSuite"
    runRegressionTest "azureTestSuite"
    break
  }	
  
  "everything" {
    resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\mysql")
    resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\mssql")
    resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\oracle")
    runRegressionTest "initialize"
    runRegressionTest "stageDataSets"
    runRegressionTest "shortRegression"
    runRegressionTest "verticaShortRegression"
	runRegressionTest "vertica10ShortRegression"
    runRegressionTest "snowflakeShortRegression"
    runRegressionTest "postgresDataTypes"
    runRegressionTest "verticaDataTypes"
    runRegressionTest "vertica10DataTypes"
    runRegressionTest "snowflakeDataTypes"
    runRegressionTest "export"
    runRegressionTest "import"
    runRegressionTest "fileRoundtrip"
    runRegressionTest "uploadRoundtrip"
    runRegressionTest "dbRoundtrip"
    runRegressionTest "mongoTestSuite"
    runRegressionTest "lostConnection"
    runRegressionTest "loaderTestSuite"
    runRegressionTest "awsTestSuite"
    runRegressionTest "azureTestSuite"
    runRegressionTest "verticaTestSuite"
	runRegressionTest "vertica10TestSuite"
    runRegressionTest "oracleCopy"
	runRegressionTest "postgresCopy"
	runRegressionTest "mysqlCopy"
	runRegressionTest "mariadbCopy"
	runRegressionTest "verticaCopy"
	runRegressionTest "vertica10Copy"
	runRegressionTest "oracleCopy"
	runRegressionTest "postgresCopy"
	runRegressionTest "mysqlCopy"
	runRegressionTest "mariadbCopy"
	runRegressionTest "verticaCopy"
	runRegressionTest "vertica10Copy"
    runRegressionTest "snowflakeTestSuite"
	runRegressionTest "snowflakeCopy"
    runRegressionTest "snowflakeCopy"
	runRegressionTest "mssql2014ShortRegression"
    runRegressionTest "mssql2014DataTypes"
    runRegressionTest "mssql2014TestSuite"
    break
  }	
 
  "loader" {
    runRegressionTest "loaderTestSuite"
    runRegressionTest "awsTestSuite"
    runRegressionTest "azureTestSuite"
  }
 
  "oracle" {
	runRegressionTest "oracleShortRegression"
    runRegressionTest "oracleDataTypes"
    runRegressionTest "oracleTestSuite"
    runRegressionTest "oraclecCopy"
    runRegressionTest "oraclecCopy"
    break
  }"

  oracle23c" {
	runRegressionTest "oracle23cShortRegression"
    runRegressionTest "oracle23cDataTypes"
    runRegressionTest "oracle23cTestSuite"
    runRegressionTest "oracle23cLostConnection"
    runRegressionTest "oracle23cCopy"
    runRegressionTest "oracle23cCopy"
    break
  }

  oracle21c" {
	runRegressionTest "oracle21cShortRegression"
    runRegressionTest "oracle21cDataTypes"
    runRegressionTest "oracle21cTestSuite"
    runRegressionTest "oracle21cLostConnection"
    runRegressionTest "oracle21cCopy"
    runRegressionTest "oracle21cCopy"
    break
  }

  "oracle19c" {
	runRegressionTest "oracle19cShortRegression"
    runRegressionTest "oracle19cDataTypes"
    runRegressionTest "oracle19cTestSuite"
    runRegressionTest "oracle19cLostConnection"
    runRegressionTest "oracle19cCopy"
    runRegressionTest "oracle19cCopy"
    break
  }

  "oracle11g" {
	runRegressionTest "oracle11gShortRegression"
    runRegressionTest "oracle11gDataTypes"
    runRegressionTest "oracle11gTestSuite"
    runRegressionTest "oracle11gLostConnection"
    runRegressionTest "oracle11gCopy"
    runRegressionTest "oracle11gCopy"
    break
  }

  "mssql" {
	runRegressionTest "mssqlShortRegression"
    runRegressionTest "mssqlDataTypes"
    runRegressionTest "mssqlTestSuite"
    break
  }

  "mssql19" {
	runRegressionTest "mssql2019ShortRegression"
    runRegressionTest "mssql2019DataTypes"
    runRegressionTest "mssql2019TestSuite"
    break
  }

  "mssql17" {
	runRegressionTest "mssql2017ShortRegression"
    runRegressionTest "mssql2017DataTypes"
    runRegressionTest "mssql2017TestSuite"
    break
  }

  "mssql14" {
	runRegressionTest "mssql2014ShortRegression"
    runRegressionTest "mssql2014DataTypes"
    runRegressionTest "mssql2014TestSuite"
    break
  }

  "mssql12" {
	runRegressionTest "mssql2012ShortRegression"
    runRegressionTest "mssql2012DataTypes"
    runRegressionTest "mssql2012TestSuite"
    break
  }

  "mariadb" {
    runRegressionTest "mariadbShortRegression"
    runRegressionTest "mariadbDataTypes"
    runRegressionTest "mariadbTestSuite"
	runRegressionTest "mariadbCopy"
	runRegressionTest "mariadbCopy"
    break
  }

  "db2" {
	runRegressionTest "db2ShortRegression"
    runRegressionTest "db2DataTypes"
    runRegressionTest "db2TestSuite"
    break
  }

  "snowflake" {
    runRegressionTest "snowflakeShortRegression"
    runRegressionTest "snowflakeDataTypes"
    runRegressionTest "snowflakeTestSuite"
	runRegressionTest "snowflakeCopy"
	runRegressionTest "snowflakeCopy"
    break
  }

  "vertica" {
	runRegressionTest "verticaShortRegression"
    runRegressionTest "verticaDataTypes"
    runRegressionTest "verticaTestSuite"
	runRegressionTest "verticaCopy"
	runRegressionTest "verticaCopy"
    break
  }

  "vertica12" {
	runRegressionTest "vertica12ShortRegression"
    runRegressionTest "vertica12DataTypes"
    runRegressionTest "vertica12TestSuite"
	runRegressionTest "vertica12Copy"
	runRegressionTest "vertica12Copy"
    break
  }

  "vertica11" {
	runRegressionTest "vertica11ShortRegression"
    runRegressionTest "vertica11DataTypes"
    runRegressionTest "vertica11TestSuite"
	runRegressionTest "vertica11Copy"
	runRegressionTest "vertica11Copy"
    break
  }
    
  "vertica10" {
	runRegressionTest "vertica10ShortRegression"
    runRegressionTest "vertica10DataTypes"
	runRegressionTest "vertica10TestSuite"
	runRegressionTest "vertica10Copy"
	runRegressionTest "vertica10Copy"
    break
  }

  "vertica09" {
	runRegressionTest "vertica09ShortRegression"
    runRegressionTest "vertica09DataTypes"
    runRegressionTest "vertica09TestSuite"
	runRegressionTest "vertica09Copy"
	runRegressionTest "vertica09Copy"
    break
  }

 "mongo" {
	runRegressionTest "mongoShortRegression"
    runRegressionTest "mongoDataTypes"
    runRegressionTest "mongoTestSuite"
    break
  }

  "ydb" {
	runRegressionTest "ydbShortRegression"
    runRegressionTest "ydbDataTypes"
    runRegressionTest "ydbTestSuite"
	runRegressionTest "ydbCopy"
	runRegressionTest "ydbCopy"
    break
  }

  "cdb" {
	runRegressionTest "cdbShortRegression"
    runRegressionTest "cdbDataTypes"
    runRegressionTest "cdbTestSuite"
    break
  }

  "copy" { 
    runRegressionTest "oracleCopy"
	runRegressionTest "postgresCopy"
	runRegressionTest "mysqlCopy"
	runRegressionTest "mariadbCopy"
	runRegressionTest "verticaCopy"
	runRegressionTest "vertica10Copy"
    break
  }

  "copy2" { 
    runRegressionTest "oracleCopy"
	runRegressionTest "postgresCopy"
	runRegressionTest "mysqlCopy"
	runRegressionTest "mariadbCopy"
	runRegressionTest "verticaCopy"
	runRegressionTest "vertica10Copy"
    runRegressionTest "oracleCopy"
	runRegressionTest "postgresCopy"
	runRegressionTest "mysqlCopy"
	runRegressionTest "mariadbCopy"
	runRegressionTest "verticaCopy"
	runRegressionTest "vertica10Copy"
    break
  }

  "regression" {
    runRegressionTest $ENV:TESTNAME
	break
  } 
  
  "custom" {
    cmd /c  $YADAMU_SCRIPT_DIR/runCustomTest.bat $ENV:TESTNAME
	break
  } 

  "cmdLine" { 
    cmd /c   $YADAMU_SCRIPT_DIR/runCmdLineTests.bat
	break
  }

  "interactive" {
    wait-Event
  } 
  
  default {
    Write-Output "Invalid Test ${YADAMU_TEST_NAME}: Valid values are shortRegression, export, snowflake, vertica, vertica09, cmdLine, copy, interactive or custom"
  } 
}