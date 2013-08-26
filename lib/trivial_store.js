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
  return ""+this.id
}

module.exports = exports = TrivialStore
function TrivialStore(delay) {
  this.nextId = 0
  this.things = {}
  this.delay = delay //truthy value for testin setImmediate vs setTimeout-1ms
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
TrivialStore.prototype.reserve = function(hdl, cb) {
  throw new Error("Not Implemented")
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
  throw new Error("Not Implemented")
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

  setImmediate(function(){
    cb(null, self.things[hdl])
  })
//  process.nextTick(function(){
//    cb(null, self.things[hdl])
//  })
}

/**
 * Store a buffer in a give Handle
 *
 * @param {Any} thing
 * @param {Handle} [hdl]
 * @param {function} cb cb(err, hdl)
 * @api public
 */
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

  assert.ok(hdl instanceof Handle)
  assert.ok(typeof thing != 'undefined')

  assert.ok(typeof thing != undefined)

  var clone = u.cloneDeep(thing)

  if (this.delay) {
    setTimeout(function(){
      self.things[hdl] = clone
      cb(null, hdl)
    }, 1)
  }
  else {
    setImmediate(function(){
      self.things[hdl] = clone
      cb(null, hdl)
    })
  }
//  process.nextTick(function(){
//    self.things[hdl] = thing
//    cb(null, hdl)
//  })
}

//the end