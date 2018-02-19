const fs = require('fs');
const sql = require('mssql');


// Query for tables in database
let sql1 = 'SELECT * FROM INFORMATION_SCHEMA.TABLES';
// Query for columns in table
let sql2 = 'SELECT * FROM INFORMATION_SCHEMA.COLUMNS ' +
           'WHERE TABLE_NAME = N\'...\'';
let texts = '# ' +  sql1 + '\n#   ' + sql2 + '\n\n';


sql.connect({user: 'steemit', password: 'steemit', server: 'sql.steemsql.com'},
            function(err) {
  if (err) {
    throw err;
  } // if (err)

  // create Request object with sql1
  new sql.Request().query(sql1, function(err, res) {
    if (err) {
      throw err;
    } // if (err)

    // Start querying tables
    queryTable(0, res.recordset, function(err) {
      if (err) {
        throw err;
      } // if (err)

      // All queried, finish up by writing result to file
      fs.writeFile('db.txt', texts, 'utf8', function(err) {
        if (err) {
          throw err;
        } // if (err)
        console.log('done');
      }); // fs.writeFile('db.txt', texts, 'utf8', function(err) { ... });
    }); // queryTable(0, res, function(err) { ... });
  }); // new sql.Request().query(sql1, function(err, res) { ... });
}); // sql.connect( ... );


let queryTable = function(idx, tables, callback) {
  let name = tables[idx].TABLE_NAME;
  texts += (tables.length>9&&idx<9?' ':'') + (idx+1) + '/' +
           tables.length + ' ' + name;

  new sql.Request().query(sql2.replace('...', name), function(err, res) {
    if (err) {
      throw err;
    } // if (err)

    texts += ' ' + res.recordset.length + '\n';
    let max = Math.max.apply(null, Object.values(res.recordset)
                      .map( (e)=>e.COLUMN_NAME.length ));
    // Check each column
    res.recordset.forEach(function(e, idx1, arr) {
      let col = e.COLUMN_NAME;
      while (col.length < max) {
        col += ' ';
      } // while (col.length < max)
      texts += '  ' + (arr.length>9&&idx1<9?' ':'') + (idx1+1) + '/' +
               arr.length + ' ' + col + ' ' + e.DATA_TYPE + '\n';
      // console.log(JSON.stringify(e,null,2));
    }); // res.recordset.forEach(function(e, idx1, arr) { ... });

    // Next one of the iteration, if any
    if (idx === tables.length - 1) {
      if (callback) {
        callback();
      } // if (callback)
    } else {
      queryTable(idx+1, tables, callback);
    } // else - if (idx === tables.length - 1)
  }); // new sql.Request().query( ... );
}; // let queryTable = function(idx, tables, callback) { ... };
