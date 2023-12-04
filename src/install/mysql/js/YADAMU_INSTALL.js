try {
  try {
    var UUID;
    var resultSet = session.runSql("select YADAMU_INSTANCE_ID() YADAMU_INSTANCE_ID");
    var row = resultSet.fetchOneObject();
    println('YADAMU_INSTANCE_ID():',row['YADAMU_INSTANCE_ID']);
  } catch(e) {
    if (e.code === 1305) {
      try {
        var resultSet = session.runSql('select UUID() INSTANCE_ID');
        var row = resultSet.fetchOneObject();
	    UUID = row['INSTANCE_ID']
	    var sql = "create function YADAMU_INSTANCE_ID() returns VARCHAR(36) DETERMINISTIC begin return '" + UUID + "'; end;"
	    resultSet = session.runSql(sql);
	    row = resultSet.fetchOneObject();
        println('CREATE FUNCTION: YADAMU_INSTANCE_ID()' );
	  } catch (e) {
	    prinln('Error creating YADAMU_INSTANCE_ID()')
	    println (e)
	  }
    }
    else {
      println ('Error invoking YADAMU_INSTANCE_ID()')
	  println (e)
    }
  }
  var sql = "drop function if exists YADAMU_INSTALLATION_TIMESTAMP"
  resultSet = session.runSql(sql);
  sql = "select DATE_FORMAT(convert_tz(now(), @@session.time_zone, '+00:00'),'%Y-%m-%dT%T.%fZ') YADAMU_INSTALLATION_TIMESTAMP"
  resultSet = session.runSql(sql);
  row = resultSet.fetchOneObject();
  var TIMESTAMP = row['YADAMU_INSTALLATION_TIMESTAMP'];
  println('Installation Timestamp:',TIMESTAMP);
  sql = "create function YADAMU_INSTALLATION_TIMESTAMP() returns VARCHAR(36) DETERMINISTIC begin return '" + TIMESTAMP + "'; end;"
  resultSet = session.runSql(sql);
  row = resultSet.fetchOneObject();
  println('CREATE FUNCTION: YADAMU_INSTALLATION_TIMESTAMP()' );
} catch (e) {
  println ('Unexpected Error')
  println (e)
}

\sql 

\source src/install/mysql/sql/INSTALL_YADAMU.sql

\quit