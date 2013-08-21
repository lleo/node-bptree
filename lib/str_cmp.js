
var assert = require('assert')
  , format = require('util').format

module.exports = exports = cmp

function cmp(a, b) {
  assert.ok(typeof a == 'string')
  assert.ok(typeof b == 'string')
  if ( a.length > b.length ) return 1
  if ( a.length < b.length ) return -1
  if ( a < b ) return -1
  if ( a > b ) return 1
  if ( a === b ) return 0
  throw new TypeError(format("WTF!! a=\"%s\" b=\"%s\"", a, b) )
}