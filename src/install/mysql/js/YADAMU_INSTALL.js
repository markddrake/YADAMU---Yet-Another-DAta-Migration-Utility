try {
  try {
    var UUID;
    var resultSet = session.runSql("select YADAMU_INSTANCE_ID() YADAMU_INSTANCE_ID");
    var row = resultSet.fetchOneObject();
    println('YADAMU_INSTANCE_ID():',row['YADAMU_INSTANCE_ID']);
  } catch(e) {
    let code = e.code
    if (!code && e.message && e.message.includes('YADAMU_INSTANCE_ID does not exist')) code = 1305
    if (code === 1305) {
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

\source sql/SET_VENDOR_TYPE_MAPPINGS.sql
\source sql/MAP_MYSQL_DATATYPE.sql
\source sql/GENERATE_SQL.sql
\source sql/GENERATE_STATEMENTS.sql
\source sql/YADAMU_IMPORT.sql
--
select YADAMU_INSTANCE_ID(), YADAMU_INSTALLATION_TIMESTAMP();
--
\quit