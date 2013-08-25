
"use strict";

var assert = require('assert')
  , format = require('util').format


exports.cmp = cmp
function cmp(a, b) {
  //assert.ok(typeof a == 'string')
  //assert.ok(typeof b == 'string')

  if (a > b) return 1
  if (b > a) return -1
  return 0
}

/**
 * Repeat a string N times.
 *   Stolen form https://developer.mozilla.org/en-US/docs/JavaScript/Reference/Global_Objects/String
 * @param {string} str
 * @param {number} n
 * @returns {string}
 */
exports.repeat = repeat
function repeat(str, n) {
  var sd = ""
    , s2 = n > 0 ? str : ""
    , mask
  for (mask = n; mask > 1; mask >>= 1) {
    if (mask & 1) sd += s2
    s2 += s2
  }
  return s2 + sd
}

/**
 * Increment a string ala perl's ++ operator on a string
 *
 * @param {String} str
 * @returns {String}
 */
exports.inc = inc
function inc(str){
  if (str.length == 0) return "a"

  var last = str[str.length-1]
    , rest = str.substr(0, str.length-1)

  if (last == "z") {
    if (rest.length == 0) return "aa"
    else return inc(rest) + "a"
  }
  if (last == "Z") {
    if (rest.length == 0) return "AA"
    else return inc(rest) + "A"
  }
  if (last == "9") {
    if (rest.length == 0) return "10"
    else return inc(rest) + "0"
  }

  return rest + String.fromCharCode( last.charCodeAt(0)+1 )
}
