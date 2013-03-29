
var util = require('util')
  , inherits = util.inherits
  , format = util.format
  , assert = require('assert')
  , Key = require('../key')

exports = module.exports = UInt32BE
function UInt32BE(i) {
  assert.equal(typeof i, 'number')
  assert.ok(i>=0, "negative number")
  assert.equal(i%1, 0, "not an Integer")
  Key.call(this)
  this.d = i
}

inherits(UInt32BE, Key)


UInt32BE.unpack = function(buffer, offset){
  return buffer.readUInt32BE(offset)
}


UInt32BE.prototype.cmp = function(other){
  assert.ok(typeof other == typeof this)
  return this.d===other.d ? 0 : this.d < other.d ? -1 : 1
}


UInt32BE.prototype.packLength = function(){
  return 4
}


UInt32BE.prototype.pack = function(buffer, offset){
  buffer.writeUInt32BE(offset)
}


UInt32BE.prototype.toString = function(){
  return this.d.toString()
  //return format("UInt32BE(%d)", this.d)
}


//