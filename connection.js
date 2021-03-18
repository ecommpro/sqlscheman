const mysql = require('mysql');
const { promisify } = require('util');

module.exports = (settings) => {
  
	const connection = mysql.createConnection(settings);
	
	const end = () => promisify(connection.end).call(connection);

  return {
    connect: () => promisify(connection.connect).call(connection),
    query: (sql, args) => promisify(connection.query).call(connection, sql, args),
    rawQuery: (sql, args) => connection.query(sql, args),
    queryStream: (sql, args) => connection.query(sql, args).stream(),
    end,
    close: end,
	};
	
}
