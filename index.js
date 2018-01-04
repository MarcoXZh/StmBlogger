/**
 * Main entry of the steemit blog autobot
 * @author  MarcoXZh3
 * @version 1.3.2
 */
const CronJob = require('cron').CronJob;
const encryption = require('./libencryption');
const fs = require('fs');
const queryCNTags = require('./jobs/QueryCnTags');
const QueryUtopianTypes = require('./jobs/QueryUtopianTypes');


var password = fs.readFileSync('pw.log', 'utf8').toString().trim();
var keys = JSON.parse(encryption.importFileSync('keys_xuzhen', password));
var options = JSON.parse(fs.readFileSync('options.json', 'utf8').toString());
options.author.posting = keys.posting;
options.db.uri = 'mongodb://' + options.db.user + ':' + keys.dbkey
                              + '@localhost:27017/' + options.db.name;

// The job to query CN tags
new CronJob('00 05 00 * * *', function() {
    queryCNTags(options, function(blog) {
        // Do nothing
    }); // queryCNTags(options, function(blog) { ... });
}, null, true, 'UTC'); // new CronJob( ... );

// The job to query Utopian.io tags
new CronJob('00 05 01 * * *', function() {
    QueryUtopianTypes(options, function(blog) {
        // Do nothing
    }); // QueryUtopianTypes(options, function(blog) { ... });
}, null, true, 'UTC'); // new CronJob( ... );
