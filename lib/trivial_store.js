
var assert = require('assert')

function Handle(id) {
  this.id = id
}

Handle.prototype.packLength = function(){
  throw ("never gonna be implemented")
}

Handle.prototype.pack = function(buf, off){
  throw ("never gonna be implemented")
}

Handle.prototype.unpack = function(buf, off){
  throw ("never gonna be implemented")
}

Handle.prototype.cmp = function(other){
  assert(other instanceof Handle, "other !instanceof Handle")
  if (this.id > other.id) return 1
  if (this.id < other.id) return -1
  if (this.id == other.id) return 0
  throw new Error("TrivialStore.Handle.cmp WTF!!")
}

Handle.prototype.toString = function(){
  return ""+this.id
}

module.exports = exports = TrivialStore
function TrivialStore() {
  this.nextId = 0
  this.things = {}
}

TrivialStore.Handle = Handle

TrivialStore.prototype.nextHandle = function(){
  var hdl = new Handle( this.nextId )
  this.nextId += 1
  return hdl
}

TrivialStore.prototype.store = function(thing, hdl, cb) {
  var self = this
  if ( typeof hdl == 'function' ) {
    cb = hdl
    hdl = this.nextHandle()
  }
  else if ( hdl == null && typeof cb == 'function' ) {
    //hdl == null is true even if typeof hdl === 'undefined'; screwy ==, but ok
    hdl = this.nextHandle()
  }

  setImmediate(function(){
    self.things[hdl] = thing
    cb(null, hdl)
  })
//  process.nextTick(function(){
//    self.things[hdl] = thing
//    cb(null, hdl)
//  })
}

TrivialStore.prototype.load = function(hdl, cb) {
  var self = this
  if ( typeof this.things[hdl] == 'undefined' ) {
    cb( new Error("undefined Handle hdl="+hdl) )
    return
  }

  setImmediate(function(){
    cb(null, self.things[hdl])
  })
//  process.nextTick(function(){
//    cb(null, self.things[hdl])
//  })
}

//the end