// File: trivial_store.js
// Abstract:
//"use strict";

var assert = require('assert')
  , format = require('util').format
  , u = require('lodash')

function Handle(id) {
  this.id = id
}

Handle.fromJSON = function(h){
  return new Handle(h.id)
}

Handle.toJSON = function(hdl){
  return { "id" : hdl.id }
}

Handle.prototype.cmp = function(other){
  assert(other instanceof Handle, "other !instanceof Handle")
  if (this.id > other.id) return 1
  if (this.id < other.id) return -1
  if (this.id == other.id) return 0
  throw new Error("TrivialStore.Handle.cmp WTF!!")
}

Handle.prototype.toString = function(){
  return "H"+this.id
}

module.exports = exports = TrivialStore
function TrivialStore(delay) {
  this.nextId = 0
  this.things = {}
  if (typeof delay == 'undefined')
    this.delay = -1
  else {
    assert.ok(typeof delay == 'number')
    assert.ok(delay%1 == 0) //is an integer
    this.delay = delay
  }
}

TrivialStore.TrivialStore = TrivialStore
TrivialStore.Handle = Handle

TrivialStore.prototype.nextHandle = function(){
  var hdl = new Handle( this.nextId )
  this.nextId += 1
  return hdl
}


/**
 * Reserve a handle for a given sized buffer.
 *
 * @param {integer} sz
 * @param {function} cb cb(err, hdl)
 * @api public
 */
TrivialStore.prototype.reserve = function(sz, cb) {
  var hdl = this.nextHandle()
  setImmediate(function(){ cb(null, hdl) })
}

/**
 * Release a handle space from storage.
 * Trivial in the case of TrivialStore
 *
 * @param {Handle} hdl
 * @param {function} cb cb(err)
 * @api public
 */
TrivialStore.prototype.release = function(hdl, cb) {
  delete this.things[hdl]
  setImmediate(function(){ cb(null) })
}

/**
 * Load a buffer for a given Handle
 *
 * @param {Handle} hdl
 * @param {function} cb cb(err, thing)
 * @api public
 */
TrivialStore.prototype.load = function(hdl, cb) {
  var self = this

  if  (u.isUndefined( this.things[hdl] ) ) {
    console.warn("TrivialStore#load: hdl=%s", hdl)
    console.warn("TrivialStore#load: this.things=%j", this.things)
    throw new Error(format("typeof this.things[%s] == 'undefined'", hdl))
  }

  if (this.delay < 0)
    setImmediate(function(){
      cb(null, self.things[hdl])
    })
  else
    setTimeout(function(){
      cb(null, self.things[hdl])
    }, this.delay)
}

/**
 * Store a buffer in a give Handle
 *
 * @param {Any} thing
 * @param {Handle} [hdl]
 * @param {function} cb cb(err, hdl)
 * @api public
 */
TrivialStore.prototype.store = function(n, hdl, cb) {
  var self = this
  if ( typeof hdl == 'function' ) {
    cb = hdl
    hdl = this.nextHandle()
  }
  else if ( hdl == null && typeof cb == 'function' ) {
    //hdl == null is true even if typeof hdl === 'undefined'; screwy ==, but ok
    hdl = this.nextHandle()
  }
  else if ( hdl instanceof Handle && typeof cb == 'function') {
    if (!this.things.hasOwnProperty(hdl.toString())) {
      console.warn(format("this.things[%s] does not exist", hdl))
      console.warn(format("this.things = %j", this.things))
    }
  }

  assert.ok(hdl instanceof Handle)
  assert.ok(typeof n != 'undefined')

  if (n.type == "Leaf") {
    assert.equal(n.keys.length, n.children.length)
  }
  else if (n.type == "Branch"){
    assert.equal(n.keys.length+1, n.children.length)
  }
  else {
    throw new Error("unknown n.type; n=%j", n)
  }

  var c = u.cloneDeep(n)

  if (c.type == "Leaf") {
    assert.equal(c.keys.length, c.children.length)
  }
  else if (c.type == "Branch"){
    assert.equal(c.keys.length+1, c.children.length)
  }
  else {
    throw new Error("unknown c.type; n=%j", n)
  }

  if (this.delay < 0)
    setImmediate(function(){
      self.things[hdl] = c
      cb(null, hdl)
    })
  else
    setTimeout(function(){
      self.things[hdl] = c
      cb(null, hdl)
    }, this.delay)

  //  process.nextTick(function(){
//    self.things[hdl] = n
//    cb(null, hdl)
//  })
}

//the end