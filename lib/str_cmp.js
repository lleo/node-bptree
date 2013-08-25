
var assert = require('assert')
  , format = require('util').format

module.exports = exports = cmp

function cmp(a, b) {
  assert.ok(typeof a == 'string')
  assert.ok(typeof b == 'string')

  /* this is not what I meant.
   * I wanted:
   *   "a"
   *   "aa"
   *   "aaa"
   *   "ab"
   * what this givs me:
   *   "a"
   *   "aa"
   *   "ab"
   *   "aaa"
   *
   *   if ( a.length > b.length ) return 1
   *   if ( a.length < b.length ) return -1
   *   if ( a < b ) return -1
   *   if ( a > b ) return 1
   *   if ( a === b ) return 0
   */

  if (a > b) return 1
  if (b > a) return -1
  if (a == b) return 0

  throw new TypeError(format("WTF!! a=\"%s\" b=\"%s\"", a, b) )
}