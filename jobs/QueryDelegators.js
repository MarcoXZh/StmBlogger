/**
 * The job to query cnbuddy's delegators
 * @author  MarcoXZh3
 * @version 1.0.0
 */
const encryption = require('../libencryption');
const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;
const steem = require('steem');


const name = __filename.replace(/\.js$/g, '');
const epsilon = 1e-8;


/**
 * Entry function -- load options from setting
 * @param {json}      parentOptions   options from the parent method
 * @param {function}  callback        (optional) the callback function
 */
module.exports = function(parentOptions, callback) {
  fs.readFile(name + '.json', {encoding: 'utf8', flag: 'r'},
              function(err, data) {
    if (err) {
      throw err;
    } // if (err)
    let options = {};
    for (let k in parentOptions) {
      if ({}.hasOwnProperty.call(parentOptions, k)) {
        options[k] = parentOptions[k];
      } // if ({}.hasOwnProperty.call(parentOptions, k))
    } // for (let k in parentOptions)
    let obj = JSON.parse(data.toString());
    for (let k in obj) {
      if ({}.hasOwnProperty.call(obj, k)) {
        options[k] = obj[k];
      } // if ({}.hasOwnProperty.call(obj, k))
    } // for (let k in obj)

    // Overwrite author-key
    if (!fs.existsSync(name)) {
      if (!fs.existsSync(name + '.log')) {
        throw new ReferenceError('No key files found for "' + name + '"');
      } // if (!fs.existsSync(name + '.log'))
      let obj = JSON.parse(fs.readFileSync(name + '.log'));
      obj = JSON.stringify(obj, null, 4);
      encryption.exportFileSync(obj, name, options.password);
    } // if (!fs.existsSync(name))
    let keys = JSON.parse(encryption.importFileSync(name, options.password));
    options.author.posting = keys.posting;
    console.log('QueryDelegators: option loaded - ' + new Date().toISOString());

    // Run the job
    runJob(options, callback);
  }); // fs.readFile( ... );
}; // module.exports = function(parentOptions, callback) { ... };


/**
 * Run the job
 * @param {json}    options   settings for the job
 * @param {function}  callback  (optional) the callback function
 */
let runJob = function(options, callback) {
  let today = new Date();
  today.setUTCHours(0, 0, 0, 0);
  MongoClient.connect(options.db.uri, function(err, client) {
    if (err) {
      console.error(err);
      return;
    } // if (err)
    client.db(options.db.name).collection('cners').find({})
          .toArray(function(err, res) {
      if (err) {
        console.error(err);
      } // if (err)
      let data = {total: res.length, today: today};
      steem.api.getDynamicGlobalProperties(function(err, re) {
        if (err) {
          console.error(err);
          return;
        } // if (err)
        let totalVests = Number(re.total_vesting_shares.split(' ')[0]);
        let totalSteem = Number(re.total_vesting_fund_steem.split(' ')[0]);
        data.delegators = res.map(function(e, i, arr) {
          if (e.vests <= epsilon) {
            return null;
          } // if (e.vests <= epsilon)
          e.sp = steem.formatter.vestToSteem(e.vests, totalVests, totalSteem);
          return e;
        }).filter((e)=>e).sort((a, b)=>b.vests-a.vests);
        console.log('QueryDelegators: delegators found - ' +
                    new Date().toISOString());

        // Write the blog
        prepareBlog(options, data, callback);
      }); // steem.api.getDynamicGlobalProperties(function(err, re) { ... });
    }); // client.db( ... ).collection( ... ).find( ... );
  }); // MongoClient.connect(options.db.uri, function(err, client) { ... });
}; // let runJob = function(options, callback) { ... };


/**
 * Prepare writing the blog
 * @param {json}      options   settings for the job
 * @param {json}      data      the data for the blog
 * @param {function}  callback  (optional) the callback function
 */
let prepareBlog = function(options, data, callback) {
  let decimal = Math.round(Math.abs(Math.log10(options.decimal)));
  fs.readFile(name + options.body_ext, {encoding: 'utf8', flag: 'r'},
              function(err, text) {
    if (err) {
      throw err;
    } // if (err)
    let body = data.delegators.map(function(e, i) {
      let sp = '' + e.sp;
      let idx = sp.indexOf('.');
      if (idx < 0) {
        sp += '.00';
      } else if (idx < sp.length - decimal) {
        sp += '00';
      } // if ... else if ...
      sp = sp.substring(0, idx + decimal + 1);
      return '| ' + (i + 1) + ' | @' + e.name + ' | ' + sp + ' | ' +
             e.membertime.toISOString().split('.')[0] + ' |';
    }).join('\n'); // let body = data.delegators.map( ... ).join('\n');
    let blog = {
      title:  options.title + ' ' + data.today.toISOString().split('T')[0],
      author: options.author.name,
      json_metadata: options.json_metadata,
      body:   text.toString()
                  .replace('$TODAY',      data.today.toISOString())
                  .replace('$NOW',        new Date().toISOString())
                  .replace('$COUNT',      data.delegators.length)
                  .replace('$TOTAL',      data.total)
                  .replace('$DELEGATORS', body),
    }; // let body = { ... };

    // Go publish it
    publishBlog(options, blog, callback);

    // Log the blog data
    log(options, data);
  }); // fs.readFile( ... );
}; // let prepareBlog = function(options, data, callback) { ... };


/**
 * Publish the blog
 * @param {json}    options   settings for the job
 * @param {json}    blog    the blog in json object
 * @param {function}  callback  (optional) the callback function
 */
let publishBlog = function(options, blog, callback) {
  console.log('QueryDelegators: publishing - ' + new Date().toISOString());
  let permlink = options.author.name + options.permlink +
                 new Date().toISOString().split('T')[0];
  steem.broadcast.comment(options.author.posting, '', 'cn', options.author.name,
              permlink, blog.title, blog.body, blog.json_metadata,
              function(err, re) {
    if (err) {
      throw err;
    } // if (err)
    console.log('QueryDelegators: published - ' + new Date().toISOString());
    if (callback) {
      callback(blog);
    } // if (callback)
  }); // steem.broadcast.comment( ... );
}; // let publishBlog = function(options, blog, callback) { ... };


/**
 * Log the message
 * @param {json}  options   settings for the job
 * @param {json}  body    the message body
 */
let log = function(options, body) {
  MongoClient.connect(options.db.uri, function(err, client) {
    if (err) {
      console.error(err);
      return;
    } // if (err)
    client.db(options.db.name)
        .collection(options.db_collection)
        .insertOne(body, function(err, res) {
      if (err) {
        console.error(err);
      } // if (err)
      console.log('QueryDelegators: logged - ' + new Date().toISOString());
    }); // client.db( ... ).collection( ... ).insert( ... );
  }); // MongoClient.connect(options.db.uri, function(err, client) { ... });
}; // let log = function(options, body) { ... };
