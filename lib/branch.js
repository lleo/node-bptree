/**
 * @fileOverview Definition of Branch (inner node) class of the B+Tree
 * @author LLeo
 * @version 0.0.0
 */

var util = require('util')
  , format = util.format
  , inherits = util.inherits
  , assert = require('assert')
  , Leaf = require('./leaf')
  , Node = require('./node')
  , u = require('lodash')

/**
 * Constructor for an inner node of B+Tree
 *
 * Rule: #keys+1 == #children
 *
 * @constructor
 * @param {Number} order
 * @param {Array} keys Array of Key objects in order
 * @param {Array} children Array of Leaf xor Branch objects in order
 * @param {Function} keyCmp keyCmp function for comparing keys; return 0,1, -1
 */
function Branch(order, keys, children, keyCmp) {
  Node.call(this, order, keys, children, keyCmp)
  assert.equal(keys.length+1, children.length)

  for (var i=0; i<children.length; i+=1) {
    assert( typeof children[i] != 'undefined'
          , format("typeof children[%d] == 'undefined'", i) )
  }
} //constructor

inherits(Branch, Node)

Branch.Branch = Branch

exports = module.exports = Branch


/** Uses super class (Node) .size() .*/

/** Uses super class (Node) .toSmall() .*/

/** Uses super class (Node) .toBig() .*/


/**
 * Add operation on this Node. Should be followed by a length check and
 * possible split call.
 *
 * The only way a Branch (or Leaf) will be added is if it is the result of
 * a nNode = oNode.split(order). We know nNode and oNode are children of this
 * Node, we want to add nNode into this Node, and nNode > oNode. The node passed
 * as an argument is nNode in this scenario.
 *
 * Key is used to find the placement index in the array of keys and children.
 * The index is used as the place to splice key into this.keys. And index+1
 * is used to splice node into this.children.
 *
 * This is all part of the B+Tree algorithm; trust it.
 *
 * @param {Key} key linear search finds the i'th placement for this Key
 * @param {Node} hdl spliced into the i+1'th placement
 * @returns {Branch} this object just for shit-n-giggles.
 */
Branch.prototype.add = function(key, lHdl, rHdl) {
  var i, len, cmp, errstr

  for (i=0, len=this.keys.length; i<len; i+=1) {
    cmp = this.keyCmp(key, this.keys[i])

    assert(cmp !== 0, format(
      "trying to add a key(%s) that already exists in Branch; cur=%s, new=%s",
      key, this.children[i+1], rHdl))

    if (cmp < 0) //key < keys[i]
      break
  }

  this.keys.splice(i, 0, key)      //insert key  at position i
  this.children.splice(i, 0, lHdl) //insert lHdl at position i
  this.children[i+1] = rHdl //replace rHdl at position i+1
  // children is always 1 greater length than keys

  this.dirty = true

  return this
} //.add()


/**
 * Remove a key and handle from a Branch.
 *
 * @param {Key} pKey
 * @returns {Handle} the removed handle children[keyIdx+1]
 */
Branch.prototype.remove = function(pKey) {
  var keyIdx, cmp, rmHdl
  for (keyIdx=0; keyIdx<this.keys.length; keyIdx+=1) {
    cmp = this.keyCmp(pKey, this.keys[keyIdx])
    if (cmp == 0) {
      this.keys.splice(keyIdx, 1)

      rmHdl = this.children[keyIdx+1]
      this.children.splice(keyIdx+1, 1)

      this.dirty = true

      return rmHdl
    }
  }
  console.error("***Branch#remove: failed to find pKey = %s", pKey)
  console.error("***Branch#remove: this = %s", this)
  throw new Error(format("Branch#remove: failed to find pKey = %s", pKey))
} //.remove()


/**
 * Remove a hdl and key from a Branch.
 * Mustn't & Won't remove the first child
 *
 * @param {Key} key
 * //depricated: @returns {[idx, key]}
 * @returns {Key}
 */
Branch.prototype.updateHdl = function(oldHdl, newHdl) {
  var i, key

  for (i=0; i<this.children.length; i+=1) {
    if ( oldHdl.equals(this.children[i]) ) {
      this.children[i] = newHdl

      this.dirty = true

      return oldHdl
    }
  }

  console.error("***Branch#updateHdl: hdl = %s not found", oldHdl)
  console.error("***Branch#updateHdl: this.children=[%j]"
               , this.children.map(function(c){ return c.toString() })
                 .join(', ') )
  throw new Error(format("Branch#updateHdl: failed to find oldHdl = %s", oldHdl))
} //.updateHdl()


/**
 * Perform a split operation on this Node. It shortens the current Node by half
 * and constructs a new Node out of the other half, then returns the new Node.
 *
 *
 * @returns {Array} pair [Key, Node]
 */
Branch.prototype.split = function(){
  /* if #children == 4 then #keys == 3
   *   m = pivotKeys  = 1  #this.keys  == 1 #nNode.keys  == 2
   *   n = pivotChldn = 2  #this.chldn == 2 #nNode.chldn == 2
   * if #children == 5 then #keys == 4
   *   m = pivotKeys  = 2  #this.keys  == 2 #nNode.keys  == 2
   *   n = pivotChldn = 3  #this.chldn == 3 #nNode.chldn == 2
   */
  var m = Math.floor( this.keys.length / 2 )
    , n = Math.ceil ( this.children.length / 2 )
    , tKeys, tChildren, nNode, pKey

  tKeys = this.keys.splice(m)
  tChildren = this.children.splice(n)

  pKey = tKeys.shift() //really this works and is necessary; diagram it!
  nNode = new Branch(this.order, tKeys, tChildren, this.keyCmp)
  nNode.parent = this.parent

  this.dirty = true

  return [pKey, nNode]
} //.split()

/**
 * Perform a merge operation on this Branch and its immediate right sibling.
 *
 * @param {Branch} rBranch
 * @param {Key} lmKey
 * @returns {Branch} this Branch
 */
Branch.prototype.merge = function(rBranch, lmKey){
  assert.ok(rBranch instanceof Branch)

  this.keys = this.keys.concat([lmKey], rBranch.keys)
  this.children = this.children.concat(rBranch.children)

  this.dirty = true

  return this
}


Branch.prototype.toString = function(){
  var kStr = this.keys
             .map(function(k){ return JSON.stringify(k) })
             .join(", ")

  var cStr = this.children
             .map(function(hdl){
               return u.isUndefined(hdl)?'undefined':hdl.toString()
             })
             .join(", ")

  return format("Branch(hdl=%s, min=%d, max=%d, keys=[%s], children=[%s])"
               , this.hdl, this.min, this.max, kStr, cStr)
}

//THE END