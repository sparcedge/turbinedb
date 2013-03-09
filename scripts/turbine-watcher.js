#!/usr/bin/env node

/*
	Usage:
	$ ./turbine-watcher.js <db> <collection> <query>

	Example:
	$ ./turbine-watcher.js  scheduler-default 51394deeb47039b72f82f05a-atl01  '[{"up":{"eq":1}}]'
	
*/

var http = require('http');

if (!process.argv[2] || !process.argv[3] || !process.argv[4])
  return console.log("\n\nUsage:\n\t$ turbine-watcher.js [DB] [COLLECTION] [JSON QUERY]\n\n");

http
	.get({
		headers: {
			'Content-Type': 'application/json'
		},
		host: 'localhost',
		port: 8080,
		path: '/notify/' + process.argv[2] + '/' + process.argv[3] + '?m=' + encodeURIComponent(process.argv[4])
	}, function(r) {
		r.setEncoding('utf8');

		r.on('data', function (chunk) {
			if (chunk.toString().match(/connected/)) return;

			var event = JSON.parse(chunk);
			var txt = "--- " + new Date(event.timestamp) + " ---\n";
			txt += JSON.stringify(event.data) + "\n";

			console.log(txt);
		});

	});