/**
 * Main entry of the steemit blog autobot
 * @author  MarcoXZh3
 * @version 1.6.3
 */
const CronJob = require('cron').CronJob;
const encryption = require('./libencryption');
const fs = require('fs');
const steem = require('steem');


// const queryCNTags = require('./jobs/QueryCnTags');
const queryUtopianTypes = require('./jobs/QueryUtopianTypes');


let password = fs.readFileSync('pw.log', 'utf8').toString().trim();
// Generate keys if necessary
if (!fs.existsSync('keys')) {
  if (!fs.existsSync('keys.log')) {
    throw new ReferenceError('No key files found');
  } // if (!fs.existsSync('keys.log'))
  let obj = JSON.parse(fs.readFileSync('keys.log').toString());
  obj = JSON.stringify(obj, null, 4);
  encryption.exportFileSync(obj, 'keys', password);
} // if (!fs.existsSync('keys'))
let keys = JSON.parse(encryption.importFileSync('keys', password));
let options = JSON.parse(fs.readFileSync('options.json', 'utf8').toString());
options.author.posting = keys.posting;
options.db.uri = 'mongodb://' + options.db.user + ':' + keys.dbkey +
                 '@localhost:27017/' + options.db.name;

// Config steem to avoid unhandled error WebSocket not open
steem.api.setOptions({url: 'https://api.steemit.com'});

/*
// The job to query CN tags
new CronJob('00 05 00 * * *', function() {
  queryCNTags(options, function(blog) {
    // Do nothing
  }); // queryCNTags(options, function(blog) { ... });
}, null, true, 'UTC'); // new CronJob( ... );
*/

// The job to query Utopian.io tags
new CronJob('00 35 00 * * *', function() {
  queryUtopianTypes(options, function(blog) {
    // Do nothing
  }); // queryUtopianTypes(options, function(blog) { ... });
}, null, true, 'UTC'); // new CronJob( ... );
