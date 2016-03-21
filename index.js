var request = require('request'),
    util = require('util'),
    _ = require('lodash'),
    moment = require('moment');

var dateUtils = require('./lib/dates.js'),
    pageUtils = require('./lib/pages.js');

function parseOptions(options) {
  if ({}.toString.call(options) === '[object String]') {
    options = {
      query: options
    };
  } else {
    dateUtils.parseDates(options);
    pageUtils.limitPageSize(options);
  }
  return options;
}



function GnipReader(options) {
  stream = options.stream || 'test';

  // Object properties
  var __options = {
    accountName: options.accountName,
    stream: stream,
  };

  if (options.password) {
    __options.username = options.username;
    __options.password = options.password;
  } else if (options.gnipAuthKey) {
    __options.gnipAuthKey = gnipAuthKey;
  }

  var templateUrl;
  if (options.url) {
    templateUrl = options.url;
  } else {
    templateUrl = util.format('https://search.gnip.com/accounts/%s/search/%s%s.json', __options.accountName, __options.stream);
  }

  var publisherOptions = {};
  if (options.publisher) {
    publisherOptions.publisher = options.publisher;
  }

  this.nextKey = null;
  this.requestPageSize = options.requestPageSize || null;

  var self = this;

  function overridableOptions() {
    var overridables = {};
    if (self.requestPageSize) {
      overridables.requestPageSize = self.requestPageSize;
    }
    return overridables;
  }


  function buildOptions(additionalPayload, getEstimate) {
    var authPayload = (__options.gnipAuthKey !== undefined) ? {
      headers: {
        'authorization': 'Basic ' + __options.gnipAuthKey
      }
    } : {
      auth: {
        'user': __options.username, 
        'pass': __options.password
      }
    };
    return _.merge({
      url: util.format(templateUrl, (getEstimate === true)?'/counts':''),
      gzip: true,
      json: true
    }, authPayload, additionalPayload);
  }

  function doQuery(optionsOrQuery, getEstimate, useNext, callback) {
    // Get parameters based off 1) instance defaults, 2) passed overrides, and 3) required values.
    var nextParameters = useNext&&self.nextKey?{next: self.nextKey}:{},
        gnipParameters = _.merge(overridableOptions(), parseOptions(optionsOrQuery), nextParameters, publisherOptions);

    // Prepare them for sending to Gnip
    var requestOptions = buildOptions({
      body: gnipParameters
    }, getEstimate);

    // And send them
    request.post(requestOptions, function(err, response, body) {
      if (!err && response.statusCode == 200) {
        // Store the next key so the user can get more records
        self.nextKey = body.hasOwnProperty('next')?body.next:null;
        callback(null, body.results, self.nextKey !== null);
      } else {
        // Parse the error that gets returned. It will likely be in the body: 
        // http://support.gnip.com/apis/search_api/api_reference.html
        var gnipErr = {
          statusCode: response.statusCode,
          url: requestOptions.url,
          parameters: requestOptions.form,
          error: err?err:body?body.error:'Unknown Error'
        };
        callback(gnipErr, null, null);
      }
    });
  }

  this.estimate = function(optionsOrQuery, callback) {
    doQuery(optionsOrQuery, true, false, callback);
  };

  this.fullEstimate = function(optionsOrQuery, callback) {
    var totalRecords = [];
    var options = parseOptions(optionsOrQuery);

    doQuery(options, true, false, function loadNextPage(err, pageData, morePages) {
      if (!err) {
        totalRecords = totalRecords.concat(pageData);

        if (morePages) {
          doQuery(options, true, true, loadNextPage);
        } else {
          return callback(null, totalRecords);
        }
      } else {
        return callback(err, totalRecords);
      }
    });
  };

  this.search = function(optionsOrQuery, callback) {
    doQuery(optionsOrQuery, false, false, callback);
  };

  this.next = function(optionsOrQuery, callback) {
    doQuery(optionsOrQuery, false, true, callback);
  };

  this.fullSearch = function(optionsOrQuery, maxRecords, pageCallback, finalCallback) {
    if ({}.toString.call(pageCallback) !== '[object Function]') {
      // The caller must specify a page callback, even if it just returns true.
      // This is to ensure that the caller consciously burns through their 
      // Gnip allowance, and not by omission or mistake.
      return finalCallback('You must provide a pageCallback() function, ' +
                    'which must return true to load the next page of records, ' +
                    'or false to stop loading.', null);
    }

    // Likewise, the caller must pass in a maxRecords value or explicitly set it to null.
    if (maxRecords !== null &&
        ({}.toString.call(maxRecords) !== '[object Number]' ||
        Math.round(maxRecords) !== maxRecords ||
        maxRecords < 1)) {
      return finalCallback('You must provide an integer maxRecord value (or null to retrieve all records)', null);
    }

    // Now get an options object we can use over and over again, adding our own
    // defaults or calculated value to so that we can optimize paging where necessary.
    var options = parseOptions(optionsOrQuery);
    pageUtils.optimizePagesize(options, maxRecords);

    // Some variables for building up the final result.
    var totalRecords = [],
        uniqueIds = [],
        pageNumber = 0;

    var readProgress = 0,
        expectedPageCount = null;

    if (maxRecords !== null) {
      expectedPageCount = Math.ceil(maxRecords / pageUtils.getPageSize(options));
      console.log('Expecting ~' + expectedPageCount + ' pages.');
    }

    // Fire off the first page request...
    doQuery(options, false, false, function loadNextPage(err, pageData, morePages) {
      if (!err) {
        pageNumber += 1;
        if (expectedPageCount !== null) {
          if (pageNumber > expectedPageCount) {
            expectedPageCount = pageNumber + 1;
          }
          readProgress = 100 * pageNumber / expectedPageCount;
        }

        // Check for and skip duplicates (can happen in edge-cases).
        var duplicates = _.remove(pageData, function(gnipRecord) {
          return _.contains(uniqueIds, gnipRecord.id);
        });
        if (duplicates.length > 0) {
          console.log('Page ' + pageNumber + ' had ' + duplicates.length + ' duplicate(s)');
          console.log(_.pluck(duplicates, 'id'));
        }

        // Update running collections
        totalRecords = totalRecords.concat(pageData);
        uniqueIds = uniqueIds.concat(_.pluck(pageData, 'id'));

        // See if the caller wants more pages
        var continueRequested = pageCallback(pageData, pageNumber, readProgress);

        // If so, get the next page unless we've hit our requested limit (if any)
        if (morePages && continueRequested && 
            (maxRecords === null || maxRecords > totalRecords.length)) {
          // Adjust the final request size if need be.
          if (maxRecords !== null && 
              maxRecords - totalRecords.length < pageUtils.getPageSize(options)) {
            // Don't get more tweets than we asked for (costs $$$)
            options.maxResults = maxRecords - totalRecords.length;
          }
          // Make the next request
          doQuery(options, false, true, loadNextPage);
        } else {
          // We're done.
          return finalCallback(null, totalRecords);
        }
      } else {
        // Abort immediately with any error we got.
        return finalCallback(err, totalRecords);
      }
    });
  };
}

module.exports = exports = GnipReader;
