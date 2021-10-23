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
  node $YADAMU_HOME\src\YADAMU_QA\common\node\test.js CONFIG=$YADAMU_QA_HOME\regression\$TESTNAME.json EXCEPTION_FOLDER=$YADAMU_LOG_PATH 2>&1 | tee-Object -FilePath $YADAMU_LOG_FILE
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

  "regression" {
    runRegressionTest $ENV:TESTNAME
	break
  } 
  
  "custom" {
    cmd /c  $YADAMU_SCRIPT_DIR/runCustomTest.bat $ENV:TESTNAME
	break
  } 

  "interactive" {
    wait-Event
  } 
  
  "export" {
     resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\mysql")
     resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\mssql")
     resetFolder (Join-Path -Path $YADAMU_MOUNT_FOLDER -ChildPath "stagingArea\export\json\oracle")
	 runRegressionTest "export"
	break
  }
  
  "all" {
    createOutputFolders $YADAMU_MOUNT_FOLDER
    runRegressionTest "shortRegression"
    runRegressionTest "postgresDataTypes"
    runRegressionTest "export"
    runRegressionTest "import"
    runRegressionTest "fileRoundtrip"
    runRegressionTest "dbRoundtrip"
    runRegressionTest "mongoTestSuite"
    runRegressionTest "lostConnection"
    runRegressionTest "loaderTestSuite"
    runRegressionTest "awsTestSuite"
    runRegressionTest "azureTestSuite"
	break
  }	
  
  "snowflake" {
    runRegressionTest "snowflakeDataTypes"
    runRegressionTest "snowflakeTestSuite"
	break
  }

  "vertica" {
    runRegressionTest "vertica11DataTypes"
    runRegressionTest "vertica10DataTypes"
    runRegressionTest "vertica11TestSuite"
	runRegressionTest "vertica10TestSuite"
	break
  }
    
  "cmdLine" { 
    cmd /c   $YADAMU_SCRIPT_DIR/runCmdLineTests.bat
	break
  }

  "copy" { 
    runRegressionTest "oracleCopy"
	runRegressionTest "postgresCopy"
	runRegressionTest "mysqlCopy"
	runRegressionTest "mariadbCopy"
	runRegressionTest "vertica10Copy"
	runRegressionTest "vertica11Copy"
	runRegressionTest "snowflakeCopy"
	# runRegressionTest "redshiftCopy"
	break
  }

  "vertica09" {
    runRegressionTest "vertica09DataTypes"
    runRegressionTest "vertica09TestSuite"
	runRegressionTest "vertica09Copy"
	break
  }

  default {
    Write-Output "Invalid Test ${YADAMU_TEST_NAME}: Valid values are shortRegression, export, snowflake, vertica, vertica09, cmdLine, copy, interactive or custom"
  }
}