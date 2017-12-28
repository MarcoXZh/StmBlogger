/**
 * The job to query tags of CN blogs
 * @author  MarcoXZh3
 * @version 1.0.0
 */
const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;
const sort = require('alphanum-sort');
const sql = require("mssql");
const steem = require('steem');


/**
 * Entry function -- load options from setting
 * @param {json}        parentOptions   options from the parent method
 * @param {function}    callback        (optional) the callback function
 */
module.exports = function(parentOptions, callback) {
    fs.readFile(__filename.replace(/\.js$/g, '.json'),
                { encoding:'utf8', flag:'r'}, function(err, data) {
        if (err) {
            throw err;
        } // if (err)
        var options = JSON.parse(data.toString());
        for (k in parentOptions) {
            options[k] = parentOptions[k];
        } // for (k in parentOptions)
        runJob(options, callback);
    }); // fs.readFile( ... );
}; // module.exports = function(parentOptions, callback) { ... };


/**
 * Run the query cn tags job
 * @param {json}        options     settings for the job
 * @param {function}    callback    (optional) the callback function
 */
var runJob = function(options, callback) {
    var today = new Date(new Date().getTime() - 86400000 * options.days_before);
    today.setUTCHours(0, 0, 0, 0);
    var yesterday = new Date(today.getTime() - 86400000);
    var query = (`SELECT * FROM Comments ` +                // the target table
                 `WHERE depth = 0 ` +                       // only blogs
                 `AND json_metadata LIKE '%"cn"%' ` +       // with "CN" tag
                 `AND created >= 'YESTERDAY' ` +            // from yesterday
                 `AND created < 'TODAY'`)                   // to today
                .replace('YESTERDAY', yesterday.toISOString())
                .replace('TODAY', today.toISOString());
    console.log('QueryCnTags: querying - from ' + yesterday.toISOString()
                                       + ' to ' + today.toISOString());
    options.today = today;
    options.yesterday = yesterday;

    sql.connect(options.sqlserver, function(err) {
        if (err) {
            throw err;
        } // if (err)
        new sql.Request().query(query, function(err, res) {
            if (err) {
                throw err;
            } // if (err)
            console.log('QueryCnTags: found - blogs=' + res.recordset.length);
            if (res.recordset.length > 0) {
                analyzeData(options, res.recordset, callback);
            } else {
                log(options, { count:0 });
            } // else - if (res.recordset.length > 0)
            sql.close();
        }); // new sql.Request().query(sql1, function(err, res) { ... });
    }); // sql.connect(options.sqlserver, function(err) { ... });
}; // var runJob = function(options, callback) { ... };


/**
 * Analyze the data
 * @param {json}        options     settings for the job
 * @param {array}       records     blogs to be analyzed
 * @param {function}    callback    (optional) the callback function
 */
var analyzeData = function(options, records, callback) {
    var tags = {};
    // var categories = {};
    records.forEach( function(r, i, arr) {
        // gether tags
        JSON.parse(r.json_metadata).tags.forEach(function(tag) {
            if (tag.trim() === '') {
                return ;
            } // if (tag.trim() === '')
            if (tag in tags) {
                tags[tag].count ++;
                tags[tag].net_votes += r.net_votes;
                tags[tag].children += r.children;
                tags[tag].total_payout_value += r.total_payout_value;
            } else {
                tags[tag] = {
                    count:              1,
                    net_votes:          r.net_votes,
                    children:           r.children,
                    total_payout_value: r.total_payout_value
                }; // tags[tag] = { ... };
            } // if (tag in tags)
        }); // JSON.parse(r.json_metadata).tags.forEach( ... );

        // // gether categories
        // var cat = r.category;
        // if (cat in categories) {
        //     categories[cat].count ++;
        //     categories[cat].net_votes += r.net_votes;
        //     categories[cat].total_payout_value += r.total_payout_value;
        // } else {
        //     categories[cat] = {
        //         count:              1,
        //         net_votes:          r.net_votes,
        //         total_payout_value: r.total_payout_value
        //     }; // categories[cat] = { ... };
        // } // else - if (cat in categories)

        // Show results
        if (i === arr.length - 1) {
            statistics(options, { tags:tags/*, categories:categories*/ }, callback);
        } // if (i === arr.length - 1)
    }); // records.forEach( function(r) { ... };
}; // var analyzeData = function(options, records, callback) { ... };


/**
 * do some statistics
 * @param {json}        options     settings for the job
 * @param {json}        results     the result in json object
 * @param {function}    callback    (optional) the callback function
 */
var statistics = function(options, results, callback) {
    var tags  = results.tags;
    var keys = Object.keys(tags);
    console.log('QueryCnTags: found - tags=' + keys.length);

    var stats = { count:keys.length };
    // Frequency of each tag sorted
    stats.tag_freq =  sort(keys.map( (k)=>tags[k].count+'/'+k ))
                            .map((e)=>e.split('/') );
    // Total votes by tag, sorted
    stats.tag_votes =  sort(keys.map( (k)=>tags[k].net_votes+'/'+k ))
                            .map((e)=>e.split('/') );
    // Total children by tag, sorted
    stats.tag_child =  sort(keys.map( (k)=>tags[k].children+'/'+k ))
                            .map((e)=>e.split('/') );
    // Total payouts by tag, sorted
    stats.tag_pay =  sort(keys.map( (k)=>tags[k].total_payout_value+'/'+k ))
                            .map((e)=>e.split('/') );
    // Payouts per blog by tag, sorted
    stats.tag_pay_avg =  sort(keys.map( (k)=>(10000*tags[k].total_payout_value/tags[k].count)+'/'+k ))
                            .map((e)=>e.split('/') );

    // var categories = results.categories;
    // delete categories[''];
    // keys = Object.keys(categories);
    // // Frequency of each category sorted
    // stats.cat_freq =  sort(keys.map( (k)=>categories[k].count+'/'+k ))
    //                         .map((e)=>e.split('/') );
    // // Total votes by category, sorted
    // stats.cat_votes =  sort(keys.map( (k)=>categories[k].net_votes+'/'+k ))
    //                         .map((e)=>e.split('/') );
    // // Total children by category, sorted
    // stats.cat_child =  sort(keys.map( (k)=>categories[k].children+'/'+k ))
    //                         .map((e)=>e.split('/') );
    // // Total payouts by category, sorted
    // stats.cat_pay =  sort(keys.map( (k)=>categories[k].total_payout_value+'/'+k ))
    //                         .map((e)=>e.split('/') );
    // // Payouts per blog by category, sorted
    // stats.cat_pay_avg =  sort(keys.map( (k)=>(10000*categories[k].total_payout_value/categories[k].count)+'/'+k ))
    //                         .map((e)=>e.split('/') );

    prepareBlog(options, stats, callback);
}; // var statistics = function(options, result, callback) { ... };


/**
 * Prepare writing the blog
 * @param {json}        options     settings for the job
 * @param {json}        stats       the statistics in json object
 * @param {function}    callback    (optional) the callback function
 */
var prepareBlog = function(options, stats, callback) {
    var half = Math.ceil(0.5 * options.count);
    var precision = Math.round(1.0 / options.decimal);
    fs.readFile(__filename.replace(/\.js$/g, options.body_ext),
                { encoding:'utf8', flag:'r'}, function(err, data) {
        if (err) {
            throw err;
        } // if (err)

        // Format the rows in the tables
        var body = {
            tag_freq:       [],
            tag_votes:      [],
            tag_child:      [],
            tag_pay:        [],
            tag_pay_avg:    []
        }; // var body = { ... };
        Object.keys(body).forEach(function(e) {
            var cnt = 1;
            while (cnt <= half) {
                // Left columns
                var idx1 = stats.count - cnt;
                var values1 = stats[e][idx1];//.split('~');
                // Index - centered
                idx1 = cnt + '';
                while (idx1.length < options.fmt_width_idx) {
                    idx1 = ' ' + idx1 + ' '
                } // while (idx1.length < options.fmt_width_idx)
                if (idx1.length > options.fmt_width_idx) {
                    idx1 = idx1.substr(1);
                } // if (idx1.length > options.fmt_width_idx)
                // Tag count - centered
                if (e.includes('pay')) {
                    values1[0] = parseFloat(values1[0]);
                    if (e.includes('pay_avg')) {
                        values1[0] /= 10000.0;
                    } // if (e.includes('pay_avg'))
                    values1[0] = '$' + (Math.round(values1[0] * precision) / precision);
                } // if (e.includes('pay'))
                while (values1[0].length < options.fmt_width_cnt) {
                    values1[0] = ' ' + values1[0] + ' '
                } // while (values1[0].length < options.fmt_width_cnt)
                if (values1[0].length > options.fmt_width_cnt) {
                    values1[0] = values1[0].substr(1);
                } // if (values1[0].length > options.fmt_width_cnt)
                // Tag name - left aligned
                values1[1] = ' ' + values1[1];
                while (values1[1].length < options.fmt_width_name) {
                    values1[1] += ' ';
                } // while (values1[1].length < options.fmt_width_name)

                // Right columns
                var idx2 = stats.count - cnt - half;
                var values2 = stats[e][idx2];//.split('~');
                if (idx2 < stats.count - options.count) {
                    idx2 = '';
                    values2 = ['', ''];
                } else {
                    idx2 = cnt + half + '';
                } // else - if (idx2 < stats.count - options.count)
                // Index - centered
                while (idx2.length < options.fmt_width_idx) {
                    idx2 = ' ' + idx2 + ' '
                } // while (idx1.length < options.fmt_width_idx)
                if (idx2.length > options.fmt_width_idx) {
                    idx2 = idx2.substr(1);
                } // if (idx2.length > options.fmt_width_idx)
                // Tag count - centered
                if (e.includes('pay')) {
                    values2[0] = parseFloat(values2[0]);
                    if (e.includes('pay_avg')) {
                        values2[0] /= 10000.0;
                    } // if (e.includes('pay_avg'))
                    values2[0] = '$' + (Math.round(values2[0] * precision) / precision);
                } // if (e.includes('pay'))
                while (values2[0].length < options.fmt_width_cnt) {
                    values2[0] = ' ' + values2[0] + ' '
                } // while (values2[0].length < options.fmt_width_cnt)
                if (values2[0].length > options.fmt_width_cnt) {
                    values2[0] = values2[0].substr(1);
                } // if (values2[0].length > options.fmt_width_cnt)
                // Tag name - left aligned
                values2[1] = ' ' + values2[1];
                while (values2[1].length < options.fmt_width_name) {
                    values2[1] += ' ';
                } // while (values2[1].length < options.fmt_width_name)

                body[e].push('   |' + idx1 + '|' + values1[0] + '|' + values1[1] + '|'
                                     + idx2 + '|' + values2[0] + '|' + values2[1] + '|');
                cnt ++;
            } // while (cnt <= half)
        }); // Object.keys(body).forEach(function(e) { ... });

        var strNow = new Date().toISOString();
        body.count = stats.count;
        body.today = options.today;
        body.yesterday = options.yesterday;
        body.title = options.title + strNow.split('T')[0];
        body.author = options.author.name;
        body.json_metadata = options.json_metadata;

        // Go publish it
        publishBlog(options, {
            title:          body.title,
            author:         body.author,
            json_metadata:  body.json_metadata,
            body:   data.toString()
                        .replace('$YESTERDAY',  options.yesterday.toISOString())
                        .replace('$TODAY',      options.today.toISOString())
                        .replace('$NOW',        strNow)
                        .replace('$COUNT',      stats.count)
                        .replace('$tag_freq',   body.tag_freq.join('\n'))
                        .replace('$tag_votes',  body.tag_votes.join('\n'))
                        .replace('$tag_child',  body.tag_child.join('\n'))
                        .replace('$tag_pay_total',  body.tag_pay.join('\n'))
                        .replace('$tag_pay_avg',    body.tag_pay_avg.join('\n'))
        }, callback);

        // Log the body
        log(options, body);
    }); // fs.readFile( ... );
}; // var prepareBlog = function(options, stats, callback) { ... };


/**
 * Publish the blog
 * @param {json}        options     settings for the job
 * @param {json}        blog        the blog in json object
 * @param {function}    callback    (optional) the callback function
 */
var publishBlog = function(options, blog, callback) {
    console.log('QueryCnTags: publishing - ' + new Date().toISOString());
    var permlink = 'xuzhen-cn-tag-analysis-' + new Date().toISOString().split('T')[0];
    steem.broadcast.comment(options.author.posting, '', 'cn', options.author.name,
                            permlink, blog.title, blog.body, blog.json_metadata,
                            function(err, re) {
        if (err) {
            throw err
        } // if (err)
        console.log('QueryCnTags: published - ' + new Date().toISOString());
        if (callback) {
            callback(blog);
        } // if (callback)
    }); // steem.broadcast.comment( ... );
}; // var publishBlog = function(options, blog, callback) { ... };


/**
 * Log the message
 * @param {json}    options     settings for the job
 * @param {json}    body        the message body
 */
var log = function(options, body) {
    MongoClient.connect(options.db.uri, function(err, client) {
        if (err) {
            console.error(err);
            return ;
        } // if (err)
        client.db(options.db.name)
              .collection(options.db_collection)
              .insertOne(body, function(err, res) {
            if (err) {
                console.error(err);
            } // if (err)
        }); // client.db( ... ).collection( ... ).insert( ... );
    }); // MongoClient.connect(options.db.uri, function(err, client) { ... });
}; // var log = function(options, body) { ... };
