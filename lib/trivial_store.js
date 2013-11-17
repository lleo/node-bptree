// File: trivial_store.js
// Abstract:
"use strict";

var assert = require('assert')
  , format = require('util').format
  , u = require('lodash')


/**
 * Handle Constructor
 *
 * @constructor
 * @param {Number} id positive integer greater than or equal to 0
 */
function Handle(id) {
  this.id = id
}


/**
 * Test if this Handle equals another
 *
 * @param {Handle} other
 * @return {Boolean}
 */
Handle.prototype.equals = function(other){
  return this.id === other.id
}


/**
 * Create a unique string representation of the Handle
 *
 * @return {String}
 */
Handle.prototype.toString = function(){
  return "H"+this.id
}


/**
 * Return the size of space (in bytes) that the Handle points to.
 * This is a fake handle size is 1MB.
 *
 * @return {Number} non-zero positive integer number of bytes
 */
Handle.prototype.size = function(){
  return 1024*1024
}


/**
 * TrivialStore Constructor.
 *
 * @constructor
 * @param {integer} delay optional argument
 */
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
  this.rootHdl = null
}

TrivialStore.TrivialStore = TrivialStore
TrivialStore.Handle = Handle


/**
 * Ask the underlying stoage if handle can contain Plain ole JSON object n
 *
 * @param {Handle} hdl
 * @param {Object} n Plain ole JSON object
 */
TrivialStore.prototype.isBigEnough = function(hdl, n){
  assert.ok(hdl instanceof Handle)
  return true
}


/**
 * Load root Handle
 *
 * @param {Function} cb cb(err, rootHdl)
 */
TrivialStore.prototype.loadRootHandle = function(cb){
  var self = this
  setImmediate(function(){
    cb(null, self.rootHdl)
  })
}


/**
 * Store root Handle in its special place (don't ask:)
 *
 * @param {Handle} rootHdl
 * @param {Function} cb cb(err)
 */
TrivialStore.prototype.storeRootHandle = function(rootHdl, cb){
  var self = this
  setImmediate(function(){
    self.rootHdl = rootHdl
    cb(null)
  })
}


/**
 * Reserve a new Handle from the underlying store
 *
 * @param {Object} n Plain ole JSON object to be stored
 * @return {Handle}
 */
TrivialStore.prototype.reserve = function(n){
  var hdl = new Handle( this.nextId )
  this.nextId += 1
  return hdl
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
  var self = this
  setImmediate(function(){
    if (delete self.things[hdl])
      cb(null)
    else
      cb(new Error(format("TrivialStore#release: did not contain hdl=%s", hdl)))
  })
}


/**
 * Convert a Handle to a storable plain ole JSON object
 *
 * @param {Handle} hdl
 * @return {Object} plain ole JSON object
 */
TrivialStore.prototype.handleToJson = function(hdl){
  return { id: hdl.id }
}


/**
 * Convert a plain ole JSON object to a Handle
 *
 * @param {Object} json plain ole JSON object
 * @return {Handle}
 */
TrivialStore.prototype.handleFromJson = function(json){
  return new Handle(json.id)
}


/**
 * Load a buffer for a given Handle
 *
 * @param {Handle} hdl
 * @param {function} cb cb(err, thing, hdl)
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
    hdl = this.reserve(n)
  }
  else if ( hdl == null && typeof cb == 'function' ) {
    //hdl == null is true even if typeof hdl === 'undefined'; screwy ==, but ok
    hdl = this.reserve(n)
  }
//  else if ( hdl instanceof Handle && typeof cb == 'function') {
//    if (!this.things.hasOwnProperty(hdl.toString())) {
//      console.warn(format("TrivialStore#store: this.things[%s] does not exist", hdl))
//      console.warn(format("TrivialStore#store: this.things = %j", this.things))
//    }
//  }

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
}


/**
 * Flush out all data to the underlying store; noop here
 *
 * @param {Function} cb cb(err)
 */
TrivialStore.prototype.flush = function(cb){
  //noop
  setImmediate(function(){
    cb(null)
  })
}


/**
 * Close the underlying store; noop here
 *
 * @param {Function} cb cb(err)
 */
TrivialStore.prototype.close = function(cb){
  //noop
  setImmediate(function(){
    cb(null)
  })
}

//