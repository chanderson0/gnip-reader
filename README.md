gnip-reader
===========

A simple node package to read [Gnip records](http://support.gnip.com/sources/twitter/data_format.html) from the [Gnip Search API](http://support.gnip.com/apis/search_api/).

## Features
* Get multiple pages of Gnip query results with a single call.
* Get an estimated count for a Gnip query.
* Output is raw, untouched, [Gnip JSON](http://support.gnip.com/sources/twitter/data_format.html).
* Abort a multi-page query at any point.
* Manual paging is also supported.

If you want to put geolocated Gnip records on a map, feed the output to [esri-gnip](https://www.npmjs.org/package/esri-gnip).


## Requirements
* A [Gnip](http://gnip.com/) account.
* [node.js](http://nodejs.org)

## Usage
The module provides a single class. Initialize an instance of the class using your Gnip credentials and some Search API stream info, then execute queries and estimates.

### Installing
    $ npm install gnip-reader

### Creating a Reader
Initialize a `GnipReader` object with `username`, `password`, `accountName`, and `stream`.

For example, if your account name is `FooInc` and your stream name is `test`, your query URL will be something like `https://search.gnip.com/accounts/FooInc/search/test.json`. Create your `GnipReader` like this:

``` JavaScript
var GnipReader = require('gnip-reader');

var myReader = new GnipReader({
  username: 'foo@fooinc.com',
  password: 'apassword',
  accountName: 'FooInc',
  stream: 'test'
});
```

Optionally, you can construct your template URL yourself. Include a %s for the optional `/counts` path.

``` JavaScript
var GnipReader = require('gnip-reader');

var myReader = new GnipReader({
  username: 'foo@fooinc.com',
  password: 'apassword',
  url: 'https://search.gnip.com/accounts/FooInc/search/test%s.json'
});
```

### Querying Gnip
Call `.fullSearch(query, recordLimit, pageCallback(gnipRecords, pageNumber), finalCallback(err, allRecords))` to retrieve records page-by-page.

* `query` can either be a simple string as specified [here](http://support.gnip.com/apis/powertrack/rules.html) or an object providing parameters as specified [here](http://support.gnip.com/apis/search_api/api_reference.html#SearchRequests).
* `recordLimit` is the maximum total number of records you want to retrieve matching the query. Pass `null` to keep paging until the query results are exhausted (or until `pageCallback()` returns `false` - see below). Use this to avoid burning through your Gnip allowance (tweets and requests). Note: this is different to the [Gnip parameter](http://support.gnip.com/apis/search_api/api_reference.html#SearchRequests) of `maxResults` which affects how Gnip pages query results.
* `pageCallback()` should return `true` to get more records, or `false` to stop paging.
    * `gnipRecords` is an array of raw Gnip records for this page.
    * `pageNumber` starts at 1.
* `finalCallback()` when retrieval has completed. `allRecords` will contain all the records retrieved up to that point. Unless an error caused the retrieval to complete, `err` will be `null`.

For example, the following will return all mentions of 'esri' in the past 30 days:
``` JavaScript
myReader.fullSearch('esri', null, function(data, pageNum) {
  console.log(data.length + ' records in page ' + pageNum);
  return true; 
}, function(err, allData) {
  if (!err) {
    console.log('Got ' + allData.length + ' records!');
  } else {
    console.error(err);
  }
});
```

`.fullSearch()` will optimize page sizes according to published Gnip ranges (currently 10...500 records per page) and any value passed into `recordLimit`. It will attempt to minimize the number of requests made, and if possible minimize tweets requested.

`.fullSearch()` will also detect duplicate tweets (by id) across pages. If you use `.search()` and `.next()` manually (see below), duplicates are not detected automatically.

### Getting an Estimate
It's often best to get an estimate of the amount of data a query might return (since Gnip charge for the tweets AND the requests needed to get those tweets). An estimate returns an approximate number of tweets, and will tell you how they're broken down by day, hour, or minute (by default, `hour` is used).

Call `.estimate(query, callback(err, gnipEstimates))` to retrieve time-partitioned counts for the query.

* `query` can either be a simple string as specified [here](http://support.gnip.com/apis/powertrack/rules.html) or an object providing parameters as specified [here](http://support.gnip.com/apis/search_api/api_reference.html#CountRequests). Use an object to also specify `fromDate` or `toDate`, or that you want results partitioned by `bucket` size of `day` or `minute`.
* `callback()` should be a function that accepts 2 parameters:
    * `err`: An error object (or null). See below.
    * `gnipEstimates`: An array of Gnip Estimates as described [here](http://support.gnip.com/apis/search_api/api_reference.html#CountRequests) (or null if `err` is not null).

``` JavaScript
myReader.estimate('esri', function(err, gnipEstimates) {
  if (err) {
    console.error(err);
  } else {
    console.log('Got ' + gnipEstimates.length + ' Gnip Estimates.');
  }
});
```

### Querying Gnip with Manual Paging
It is recommended that you use `.fullSearch()`, but if for some reason `.fullSearch()` doesn't cut it for you, you can use `.search()` and `.next()` to page through results manually. Here's how:

#### Initial query
Call `.search(query, callback(err, gnipRecords, moreRecordsAvailable))` to retrieve records for a given query.

* `query` can either be a simple string as specified [here](http://support.gnip.com/apis/powertrack/rules.html) or an object providing parameters as specified [here](http://support.gnip.com/apis/search_api/api_reference.html#SearchRequests).
* `callback()` should be a function that accepts 3 parameters:
    * `err`: An error object (or null). See below.
    * `gnipRecords`: An array of Gnip records as described [here](http://support.gnip.com/sources/twitter/data_format.html#SamplePayloads) (or null if `err` is not null).
    * `moreRecordsAvailable`: `true` if there are more records to retrieve for this query. `false` otherwise.

For example:

``` JavaScript
myReader.search('esri', function(err, gnipRecords, moreRecords) {
  if (err) {
    console.error(err);
  } else {
    console.log('Got ' + gnipRecords.length + ' Gnip Records. There are ' + 
                (moreRecords?'':'no ') + 'more records.');
  }
});
```

#### Get subsequent pages
If `moreRecordsAvailable` is `true`, you can get pages of subsequent records with `.next()`, which takes the same parameters as `.search()`.

**Note:** As specified by Gnip, the `query` should not be modified between calls to `.search()` and `.next()` or between calls to `.next()` and `.next()`.

``` JavaScript
myReader.search('esri', function(err, gnipRecords, moreRecords) {
  if (!err) {
    console.log('Got ' + gnipRecords.length + ' Gnip Records. There are ' + 
                (moreRecords?'':'no ') + 'more records.');
    if (moreRecords) {
      var count = 2,
          totalRecords = gnipRecords;

      var getMoreRecords = function() {
        myReader.next('esri', function(err, gnipRecords, moreRecords) {
          if (!err) {
            console.log('Page ' + count++ + ': ' + gnipRecords.length + ' more records');
            totalRecords = totalRecords.concat(gnipRecords);
            if (moreRecords) {
              getMoreRecords();
            } else {
              console.log('Got a total of ' + totalRecords.length + ' records!');
            }
          } else {
            console.error('Error geting page ' + --count + '. Aborting: ' + err);
          }
        });
      };

      // Kick off loading additional pages…
      getMoreRecords();
    }
  } else {
    console.error(err);
  }
});
```

Or, using [Async.js](https://github.com/caolan/async)…

``` JavaScript
var async = require('async');

myReader.search('esri', function(err, gnipRecords, moreRecords) {
  if (err) {
    console.error(err);
  } else {
    console.log('Got ' + gnipRecords.length + ' Gnip Records. There are ' + 
                (moreRecords?'':'no ') + 'more records.');
    if (moreRecords) {
      var count = 2,
          totalRecords = gnipRecords;

      async.whilst(
        function() { return moreRecords; },
        function(callback) {
          myReader.next('esri', function(err, gnipRecords, getAnotherPage) {
            if (!err) {
              console.log('Page ' + count++ + ': ' + gnipRecords.length + 
                          ' more records');
              totalRecords = totalRecords.concat(gnipRecords);
              moreRecords = getAnotherPage;              
            }
            callback(err);
          });
        },
        function(err) {
          if (err) {
            console.error('Error geting page ' + --count + '. Aborting: ' + err);
          } else {
            console.log('Got a total of ' + totalRecords.length + ' records!');
          }
        }
      );
    }
  }
});
```

## Query format
A `query` parameter to `.fullSearch()`, `.estimate()`, `.search()` and `.next()` can be a single-line string as described [here](http://support.gnip.com/apis/powertrack/rules.html), or an object with parameters as described in the Gnip documenation [here](http://support.gnip.com/apis/search_api/api_reference.html#SearchRequests) (or [here](http://support.gnip.com/apis/search_api/api_reference.html#CountRequests) for estimates).

Note that the Gnip `fromDate` and `toDate` parameters can be one of the following:

* A JavaScript `Date` object.
* A [moment-js](http://momentjs.com) `moment`.
* A string in the Gnip date format 'YYYYMMDDHHmm'.
* A string that can be [parsed by moment-js](http://momentjs.com/docs/#/parsing/string/) to a valid date.

In the case of estimates, the Gnip `bucket` parameter can be one of the following:

* `day`
* `hour` (the default)
* `minute`

Here is a sample complex query that searches for 'esri' today, and returns pages of 10 records at a time:

``` JavaScript
var moment = require('moment');

var startTime = moment().startOf('day');
var endTime = new Date();

var complexQuery = {
  query: 'esri', 
  maxResults: 10, 
  fromDate: startTime, 
  toDate: endTime
};
```

**Note**: When calling `.fullSearch()`, the value of `maxResults` may be overriden to reduce the number of requests made against the Gnip Search API. When calling `.search()`, the provided value of `maxResults` is honored.

## Error format
Errors are an object of the following structure:

``` JavaScript
{ 
  statusCode: <int>,
  url: <searchStreamUrl>,
  parameters: <stringifiedJSON>,
  error: { // As returned from Gnip
    message: <ErrorString>,
    sent: <UTCTimeString>
  }
}
```

For example:

``` JavaScript
{ 
  statusCode: 422,
  url: 'https://search.gnip.com/accounts/FooInc/search/test.json',
  parameters: '{"query":"esri","maxResults":10,"fromDate":"201408190000","toDate":"201408200000","publisher":"twitter"}',
  error: { 
    message: 'Could not accept your search request: Invalid date for query parameter \'toDate\'. Can\'t ask for activities from the future.\n',
    sent: '2014-08-19T22:22:49+00:00'
  }
}
```

## Known Limitations
* If `fromDate` and `toDate` are strings and fail to parse, an exception is thrown.
* In edge-cases, while paging manually with `.search()` and `.next()`, [Gnip can return duplicate records](http://support.gnip.com/apis/search_api/api_reference.html#SearchRequests) across pages. `gnip-reader` does not detect these duplicates. Duplicates are detected (by id) and removed when using `.fullSearch()`.

## Resources

* [node.js documentation](http://nodejs.org/api/)
* [Gnip Documentation](http://support.gnip.com/sources/twitter/data_format.html) (incomplete - not all the attributes that are returned are described in this documentation).

### Dependencies
gnip-reader makes use of the following amazing packages:

* [Request-JS](https://github.com/mikeal/request)
* [Moment-JS](http://momentjs.com)
* [LoDash](http://lodash.com)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an Issue.

## Contributing

Anyone and everyone is welcome to contribute. 

## Licensing
Copyright 2014 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's [license.txt](license.txt) file.
[](Esri Tags: NodeJS REST Gnip)
[](Esri Language: JavaScript)
