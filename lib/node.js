/**
 * @fileOverview Definition of Inner Node class
 * @author LLeo
 * @version 0.0.0
 */

var assert = require('assert')

/**
 * Constructor for an base class node of B+Tree
 *
 * @constructor
 * @param {Number} order
 * @param {Array} keys Array of Key objects in order
 * @param {Array} children Array of Node objects in order
 * @param {Function} keyCmp function for comparing keys; return 0,1, -1
 */
function Node(order, keys, children, keyCmp) {
  assert.ok(keys instanceof Array)
  assert.ok(children instanceof Array)
  assert.ok(typeof keyCmp == 'function')

  this.order = order
  this.min = Math.floor(order/2)
  this.max = order - 1

  this.hdl = null    /* type: Handle */
  this.parent = null /* type: Node */

  //storable data
  this.keys = keys
  this.children = children

  this.keyCmp = keyCmp

  this.dirty = false //bool flag if node has been modified

  Node.CNT += 1
}

Node.Node = Node

exports = module.exports = Node

Node.CNT = 0


/**
 * How wide is this Node with respect to order
 *
 * @returns {number}
 */
Node.prototype.size = function(){
  return this.keys.length
} //.size()


/**
 * This is to small.
 *
 * @returns {boolean}
 */
Node.prototype.toSmall = function(){
  assert.ok(!isNaN(this.min))
  return this.size() < this.min
}


/**
 * This is to big.
 * @returns {boolean}
 */
Node.prototype.toBig = function(){
  assert.ok(!isNaN(this.max))
  return this.size() > this.max
}


/**
 * NOT IMPLEMENTED
 * Add operation on this Node. Should be followed by a length check and
 * possible split call.
 *
 * This is all part of the B+Tree algorithm; trust it.
 *
 * @param {Key} key linear search finds the i'th placement for this Key
 * @param {Leaf|Node} node spliced into the i+1'th placement
 * @returns {Node} this object just for shit-n-giggles.
 */
Node.prototype.add = function(key, node) {
  throw new Error("not implemented")
} //.add()


/**
 * NOT IMPLEMENTED
 * Remove a key and child from node
 *
 */
Node.prototype.remove = function(key) {
  throw new Error("not implemented")
} //.remove()


/**
 * NOT IMPLEMENTED
 * Perform a split operation on this Node.
 *
 * @returns {Array} [pKey, rNode]
 */
Node.prototype.split = function(){
  throw new Error("not implemented")
} //.split()


/**
 * Perform a merge operation on this Node and its immediate right sibling.
 *
 * @param {Node} rSib
 * @returns {Node} this node
 */
Node.prototype.merge = function(rSib){
  throw new Error("not implented")
} //.merge()


//THE END