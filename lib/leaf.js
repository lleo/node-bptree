/**
 * @fileOverview Definition of Leaf Node class
 * @author LLeo
 * @version 0.0.0
 */

var util = require('util')
  , format = util.format
  , inherits = util.inherits
  , assert = require('assert')
  , Node = require('./node')
  , u = require('lodash')

/**
 * Constructor for a leaf node of the B+Tree
 *
 * Rule: #keys == #children
 *
 * @constructor
 * @param {Number} order
 * @param {Array} keys Array of Key objects in order
 * @param {Array} data Array of Data objects in order
 * @param {Function} keyCmp function for comparing keys; return 0,1, -1
 */
function Leaf(order, keys, data, keyCmp) {
  Node.call(this, order, keys, data, keyCmp)
  assert.equal(keys.length, data.length)
} //Constructor

inherits(Leaf, Node)

Leaf.Leaf = Leaf

exports = module.exports = Leaf


/** Uses super class (Node) .size() .*/

/** Uses super class (Node) .toSmall() .*/

/** Uses super class (Node) .toBig() .*/


/**
 * Retrieve the data item from the Leaf node w/o deleting it.
 *
 * @param {Key} key
 * @returns {object} data
 */
Leaf.prototype.get = function(key) {
  for (var i=0; i<this.keys.length; i+=1) {
    if (this.keyCmp(key, this.keys[i]) === 0)
      return this.children[i]
  }
  return undefined
} //.get()


/**
 * Add a Key and data item to the Leaf node.
 *
 * @param {Key} key
 * @param {object} value
 * @returns {Leaf} this object just for shit-n-giggles.
 */
Leaf.prototype.add = function(key,value) {
  var i, len, cmp
  for (i=0, len=this.keys.length; i<len; i+=1) {
    cmp = this.keyCmp(key, this.keys[i])
    if (cmp === 0) {
      this.children[i] = value
      return this
    }
    if (cmp < 0) { //key < keys[i]
      //shoe-horn (key,value) in the ith position
      this.keys.splice(i,0,key)
      this.children.splice(i,0,value)
      return this
    }
  }
  this.keys.push(key)
  this.children.push(value)
  return this
} //add()


/**
 * Remove the key and data item from the Leaf node
 *
 * @param {Key} key
 * @returns {Leaf} this object
 */
Leaf.prototype.remove = function(key) {
  var i, cmp
  for (i=0; i<this.keys.length; i+=1) {
    cmp = this.keyCmp(key, this.keys[i])
    if (cmp === 0) {
      this.keys.splice(i,1)     //=> [key]
      this.children.splice(i,1) //=> [data]
      return this
    }
  }
  console.error("***Leaf.remove: failed to find key = %j", key)
  console.error("***Leaf.remove: this = %j", this)
  throw new Error(format("Leaf#remove: failed to find key=%j", key))
} //.remove()


/**
 * Perform a split operation on this Leaf. It shortens the current Leaf by half
 * and constructs a new Leaf out of the other half, then returns the new Leaf.
 *
 * @returns {[Key, Leaf]}
 */
Leaf.prototype.split = function(){
  var m = Math.ceil( this.keys.length / 2 )
    , tKeys, tChildren, nLeaf, pKey

  tKeys = this.keys.splice(m)
  tChildren = this.children.splice(m)

  pKey = tKeys[0]
  nLeaf = new Leaf(this.order, tKeys, tChildren, this.keyCmp) //next sibling leaf
  nLeaf.parent = this.parent

  return [ pKey, nLeaf ]
} //.split()


/**
 * Perform a merge operation on this Leaf and its immediate right sibling
 *
 * @param {Leaf} rLeaf
 * @param {Key} lmKey UNUSED
 * @returns {Leaf} this leaf
 */
Leaf.prototype.merge = function(rLeaf, lmKey){
  assert.ok(rLeaf instanceof Leaf)

  this.keys     = this.keys.concat(rLeaf.keys)
  this.children = this.children.concat(rLeaf.children)

  return this
} //.merge()


/**
 * Visit each key-value pair in the leaf in-order.
 *
 * @param {function} visit visit(key, value)
 */
Leaf.prototype.forEach = function(visit){
  var self = this
  this.keys.map(function(key, i){
    return visit(key, self.children[i])
  })
}


Leaf.prototype.toString = function(){
  var kStr = this.keys
             .map(function(k){ return JSON.stringify(k) })
             .join(", ")

  var cStr = this.children
             .map(function(v){ return v.toString() })
             .join(", ")

  return format("Leaf(hdl=%s, min=%d, max=%d, keys=[%s], children=[%s])"
               , this.hdl, this.min, this.max, kStr, cStr)
}

//