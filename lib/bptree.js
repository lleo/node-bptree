/**
 * @fileOverview Top level B+Tree class with BlockFile block store
 * @author LLeo
 * @version 0.0.0
 */

var assert = require('assert')
  , util = require('util')
  , format = util.format
  , async = require('async')
  , u = require('lodash')
  , Node = require('./node')
  , Branch = require('./branch')
  , Leaf = require('./leaf')
  , strOps = require('./str_ops')
  , strCmp = strOps.cmp
  , TrivialStore = require('./trivial_store')
  , MemStore = require('./mem_store')
  , msgpack = require('msgpack-js')


/**
 * B+Tree data struture
 *
 * @param {number} order
 * @param {Function} keyType
 */
exports = module.exports = BpTree
function BpTree(order, keyCmp, storage) {
  assert(typeof order === 'number', "order must be an number")
  assert(order%1===0, "order must be an integer")
  assert(order > 1, "order must be greater than 1")

  this.order = order
  if (keyCmp == null || keyCmp == 'string')
    this.keyCmp = strCmp
  else
    this.keyCmp = keyCmp

  if (typeof storage == 'undefined')
    this.storage = new TrivialStore(true)
  else
    this.storage = storage

  assert.ok(typeof this.keyCmp == 'function')
  assert.ok(u.isObject(this.storage))

  this.root = null
  this.writing = false; console.warn("BpTree constructor this.writing = %j", this.writing)

} //constructor

BpTree.Node = Node
BpTree.Leaf = Leaf
BpTree.Branch = Branch


/**
 * Insert a Key-Value pair in the B+Tree
 *
 * @param {Key} key
 * @param {object} value
 * @param {Function} cb cb(err)
 */
BpTree.prototype.put = function(key, value, cb){
  var self = this

  assert(!u.isUndefined(value), "BpTree#put: value of undefined is not allowed")

  if (this.root === null) { //empty tree
    //create leaf node
    console.warn("BpTree#put: creating newRoot")
    var newRoot = new Leaf(this.order, [key], [value], this.keyCmp)
    this.replaceRoot(newRoot, [], [], cb)
    return
  }

  console.warn("BpTree#put: .findLeaf key=%s", key)
  this.findLeaf(key, function(err, leaf, path){
    if (err) { cb(err); return }
    console.warn("BpTree#put: found Leaf: leaf=%s; path=%j", leaf, path)
    self.insertIntoLeaf(leaf, path, key, value, function(err, newHdls, oldHdls){
      if (err) { cb(err); return }
      cb(null)
    }) //insertIntoLeaf
  }) //findLeaf

} //.put()


/**
 * Retrieve data for a given Key w/o deleting it.
 *
 * @param {Key} key
 * @param {Function} cb cb(err, data)
 * @returns {object} data value assosiated with key.
 */
BpTree.prototype.get = function(key, cb){
  var self = this
  this.findLeaf(key, function(err, leaf, path){
    if (err) { cb(err); return }
    cb(null, leaf.get(key))
    return
  })
} //.get()


/**
 * Remove a Key-Value pair from the B+Tree
 *
 * @param {Key} key
 * @param {Function} cb cb(err, data)
 * @returns {object} data value assosiated with key.
 */
BpTree.prototype.del = function(key, cb){
  var self = this
  throw Error("not implemented")

  this.findLeaf(key, function(err, leaf, path){
    var idx, data
    if (err) { cb(err); return }

    data = leaf.get(key)
    if ( u.isUndefined(data) ) {
      cb(null) //undefined means not found
      return
    }

    self.removeFromLeaf(leaf, path, key, function(err, oldHdls, newHdls){
      if (err) { cb(err); return }
      //do something with oldHdls & newHdls
      cb(null, data)
    })
  })
} //.del()


/**
 * Visit each node pre-order traversal.
 *
 * @param {Function} visit visit(node, isLeaf, depth)
 * @param {Function} [done] done(err, res)
 */
BpTree.prototype.traversePreOrder = function(visit, done){
  var self = this
    , res = []

  function traverseNodePreOrder(node, depth, nodeDone){
    if (node instanceof Branch) {
      res.push( visit(node, false, depth) )
      async.parallel(
        node.children.map(function(hdl){
          return function(pcb){ self.load(hdl, pcb) }
        })
      , function(err, childs){
          if (err) { done(err); return }
          async.parallel(
            childs.map(function(n){
              return function(pcb){ traverseNodePreOrder(n, depth+1, pcb) }
            })
          , function(err) {
              if (err) { nodeDone(err); return }
              nodeDone(null)
            }) //async.parallel traverse each node
        }) //async.parallel hdl
    }
    else if (node instanceof Leaf){
      res.push( visit(node, true, depth) )
      nodeDone(null)
    }
  }

  if (this.root === null) {
    done(null, [])
    return
  }
  traverseNodePreOrder(this.root, 0, function(err){
    var hasDone = typeof done === 'function'
    if (err) {
      if (hasDone) { done(err); return }
      else throw new Error(err)
    }
    hasDone && done(null, u.flatten(res))
  })
} //.traversePreOrder()


/**
 * Visit each node in-order traversal.
 *
 * @param {Function} visit visit(node, isLeaf, depth)
 * @param {Function} [done] done(err, res)
 */
BpTree.prototype.traverseInOrder = function(visit, done){
  var self = this
    , res = []

  function traverseNodeInOrder(node, depth, nodeDone){
    if (node instanceof Branch) {
      res.push( visit(node, false, depth) )
      async.series(
        node.children.map(function(hdl, i){
          return function(next){
            self.load(hdl, function(err, child){
              traverseNodeInOrder(child, depth+1, next) })
          }
        })
      , nodeDone) //async.series hdl
    }
    else if (node instanceof Leaf){
      res.push( visit(node, true, depth) )
      nodeDone(null)
    }
  }

  if (this.root === null) {
    done(null, [])
    return
  }
  traverseNodeInOrder(this.root, 0, function(err){
    if (done && typeof done === 'function') {
      if (err) { done(err); return }
      done(null, u.flatten(res))
    }
    else {
      if (err) throw new Error(err)
    }
  })
} //.traverseInOrder()


/**
 * Visit each key/data pair in order.
 *
 * @param {Function} visit visit(key, data)
 * @param {Function} [done] done(err, res)
 */
BpTree.prototype.forEach = function(visit, done){
  this.traverseInOrder(function(node){
    if (node instanceof Leaf) {
      node.forEach(visit)
    }
  }, done)
} //.forEach()


/**
 * Search BpTree for a leaf node which would have a given key
 *
 * @param {Key} key
 * @param {Function} cb cb(err, leaf, path) path isa Array of Branch objects
 */
BpTree.prototype.findLeaf = function(key, cb){
  if (this.root === null) { cb(null, null); return }

  var self = this
    , path = []

  function find(err, node) {
    if (err) { cb(err); return }

    if (node instanceof Branch) {
      //node isa Branch so node.children contains Hdls
      path.push(node)
      for (var i=0; i<node.keys.length; i++)
        if ( self.keyCmp(key, node.keys[i]) < 0 ) break

      //if (typeof node.children[i] == 'undefined')
      //  throw new Error(format("BpTree#findLeaf: find: node.children[%d] == undefined; node.keys = %j; node.children = %j",i, node.keys, node.children))

      self.load(node.children[i], find)
      return
    }
    assert.ok(node instanceof Leaf)
    cb(null, node, path)
  }

  find(null, this.root)
} //.findLeaf()


/**
 * Insert a Key and child into a Node of this BpTree.
 *
 * @param {Node} node inner Node to insert key and Node|Leaf into.
 * @param {Array(Branch)} path array of Branch objects from root to parent
 * @param {Key} key
 * @param {Node} cNode
 * @param {Function} cb cb(err, [...newHdls], [...oldHdls])
 */
BpTree.prototype.insertIntoLeaf = function(leaf, path_, key, child, cb) {
  assert(leaf instanceof Leaf, "WTF! this is supposed to be a Leaf")

  var self = this
    , path = u.clone(path_)
    , newHdls = []

  leaf.add(key, child)
  if ( !leaf.toBig() ) { //no split is necessary, just store
    this.storeLeaf(leaf, path, cb)
    return
  }
  //node.toBig() needs node.split()

  var lLeaf = leaf
    , pair  = leaf.split()
    , pKey  = pair[0]
    , rLeaf = pair[1]

  this.storeSplitLeaf(lLeaf, pKey, rLeaf, path, cb)
} //.insert()

/**
 * A single leaf node needs be stored and its parent needs to have the handle
 * update. There is no modification of size or keys and all parents are Branch
 * nodes. Further, given no modifications of size means this update can be
 * cascaded up to the root.
 *
 * @param {Leaf} leaf leaf object that must be stored
 * @param {Array} path array of branch nodes being the path to root
 * @param {Function} cb cb(err, newHdls, oldHdls) *Hdls are arrays of Handles
 */
BpTree.prototype.storeLeaf = function(leaf, path, cb){
  this.storeSimplePath(leaf, path, [], [], cb)
}


/**
 * Store and node and replace the new handle in parent, then bubble up the
 * changes to the parent.
 *
 * @param {Node} node
 * @param {Array} path array of branch nodes being the path to root
 * @param {Array} newHdls array of newly stored Handles
 * @param {Array} oldHdls array of obsoleted Handles
 * @param {Function} cb cb(err, newHdls, oldHdls)
 */
BpTree.prototype.storeSimplePath = function(node, path, newHdls, oldHdls, cb){
  var self = this

  oldHdls.push(node.hdl)

  this.store(node, function(err, newHdl){
    if (err) { cb(err); return }

    newHdls.push(node.hdl) // node.hdl === newHdl after .store

    if ( path.length > 0 ) {
      var parent = path.pop() //top is last
      //RULE: if parent exists then leaf must have been stored before
      //RULE: if a node was stored before it has an non-null hdl
      if ( node.hdl.cmp(newHdl) != 0 ) { // node.hdl != newHdl
        parent.updateHdl(node.hdl, newHdl)

        //node.hdl = newHdl //lets assume .store sets the hdl

        self.storeSimplePath(parent, path, newHdls, oldHdls, cb)

        return
      } //if newHdl needs to be stored
    } //if parent isnot undefined

    //node.hdl = newHdl //lets assume .store sets the hdl

    cb(null, newHdls, oldHdls)
  })
}


/**
 * Store a split leaf (lLeaf, pKey, rLeaf) and bubble up the changes
 *
 * @param {Node} lLeaf
 * @param {Key} pKey
 * @param {Node} rLeaf
 * @param {Array} path array of branch nodes being the path to root
 * @param {Function} cb cb(err, newHdls, oldHdls)
 */
BpTree.prototype.storeLeafSplit = function(lLeaf, pKey, rLeaf, path, cb){
  this.storeSplitPath(lLeaf, pKey, rLeaf, path, [], [], cb)
}


/**
 * Store a split node (lNode, pKey, rNode) and bubble up the changes
 *
 * @param {Node} lNode
 * @param {Key} pKey
 * @param {Node} rNode
 * @param {Array} path array of branch nodes being the path to root
 * @param {Array} newHdls array of newly stored Handles
 * @param {Array} oldHdls array of obsoleted Handles
 * @param {Function} cb cb(err, newHdls, oldHdls)
 */
BpTree.prototype.storeSplitPath = function(lNode, pKey, rNode, path, newHdls, oldHdls, cb){
  var self = this

  var lHdl, rHdl

  async.series(
    [ function(scb){
        self.store(lNode, function(err, hdl){
          if (err) { cb(err); return }
          //lHdl = hdl // lNode.hdl === hdl after .store
          scb(hdl)
        })
      }
    , function(scb){
        self.store(rNode, function(err, hdl){
          if (err) { cb(err); return }
          //rHdl = hdl // rNode.hdl === hdl after .store
          scb(hdl)
        })
      }
    ]
  , function(err, res){
      newHdls.push(lHdl)
      oldHdls.push(lNode.hdl)

      newHdls.push(rHdl)
      oldHdls.push(rNode.hdl)

      lNode.hdl = lHdl
      rNode.hdl = rHdl

      if ( path.length == 0 || self.root == null ) {
        assert.strictEqual(self.root, null)
        assert.strictEqual(path.length, 0)

        var newRoot = new Branch(self.order, [pKey], [lNode, rNode], self.keyCmp)
        this.replaceRoot(newRoot, newHdls, oldHdls, cb)

        return
      }

      var parent = path.pop()

      parent.add(pKey, lNode.hdl, rNode.hdl)

      if ( !parent.toBig() ) {
        self.storeSimplePath(parent, path, newHdls, oldHdls, cb)
        return
      }
      //
      //parent.toBig needs split

      var pair = parent.split()
        , nKey  = pair[0]
        , nNode = pair[1]

      //                  lNode   pKey  rNode
      self.storeSplitPath(parent, nKey, nNode, path, newHdls, oldHdls, cb)
    })

}


/**
 * Remove a child from a Leaf by Key
 * and propengate changes up the tree if needed.
 *
 * @param {Leaf} leaf
 * @param {Array} path
 * @param {Key} key
 * @param {Function} cb cb(err, [...oldHdls], [...newHdls])
 *
 */
BpTree.prototype.removeFromLeaf = function(leaf, path_, key, cb) {
  assert(leaf instanceof Leaf, "WTF! this is supposed to be a Leaf")

  var self = this
    , path = u.clone(path_)

  var data = leaf.remove(key)

  if (u.isUndefined(data)) {
    //Q? what is the right way to indicate key does not exist
    cb(new Error("leaf.remove(key) failed"))
    return
  }

  if (leaf === this.root) {
    assert.strictEqual(path.length, 0)

    if (leaf.size() > 0) {
      ///Leaf root leafs MAY have only one key-value pair aka size()>0
      this.storeLeaf(leaf, path, cb)
    }
    else {
      this.replaceRoot(null, [], [], cb)
    }

    return
  }

  if ( !leaf.toSmall() ) {
    this.storeLeaf(leaf, path, cb)
    return
  }
  //leaf.toSmall() gotta merge/split/store rinse&repeat

  this.doMerge(leaf, path, function(err, mergedLeaf, pKey, deadLeaf){

    var parent = path.pop()

    if ( !mergedLeaf.toBig() ) {
      self.store(mergedLeaf, function(err, hdl){
        if (err) { cb(err); return }

        //mergedLeaf.hdl = hdl //store should set the hdl

        self.removeFromBranch( parent, path, pKey, mergedLeaf
                             , [null], [deadLeaf.hdl], cb)
      })
      return
    }
    //mergedLeaf.toBig needs a .split

     var pair = mergedLeaf.split()
       , npKey = pair[0]
       , rLeaf = pair[1]

    parent.remove(pKey, mergedLeaf.hdl) //<-before any store

    async.series(
      [ function(scb){
          self.store(mergedLeaf, function(err, hdl){
            if (err) { scb(err); return }
            //mergedLeaf.hdl = hdl //store should set the hdl
            scb()
          })
        }
      , function(scb){
          self.store(rLeaf, function(err, hdl){
            if (err) { scb(err); return }
            //rLeaf.hdl = hdl //store should set the hdl
            parent.add(npKey, rLeaf.hdl)
            scb()
          })
        }
      ]
    , function(err){
        if (err) { cb(err); return }

        path.push(parent)

        //could have called .storeLeaf except for deadNode
        self.storeSimplePath( mergedLeaf, path, [null], [deadLeaf.hdl], cb)
      }) //async.series

  }) //.doMerge
} //.removeFromLeaf()


/**
 * Remove a child from a Branch by key
 * and propengate changes up the tree if needed.
 *
 * @param {Node} branch node being modified
 * @parem {Key} pKey key of the column to be removed
 * @param {Node} rNode the replacement node for i+1
 * @param {Array} path
 * @param {Array} newHdls
 * @param {Array} oldHdls
 * @param {Function} cb cb(err, [...oldHdls], [...newHdls])
 *
 */
BpTree.prototype.removeFromBranch = function(branch, pKey, rNode, path, newHdls, oldHdls, cb){
  assert(branch instanceof Branch, "WTF! this is supposed to be a Branch")

  var self = this

  branch.remove(pKey, rNode.hdl)

  if (this.root === branch) {
    if (branch.size() > 1) {
      oldHdls.push(branch.hdl)
      this.store(branch, function(err, hdl){
        if (err) { cb(err); return }
        newHdls.push(branch.hdl) //remember branch.hdl == hdl after .store
        cb(null, newHdls, oldHdls)
      })
    }
    else { // branch.children.lenth == 1
      assert.ok(branch.children.length == 1)
      this.replaceRoot(rNode, newHdls, oldHdls, cb)
    }

    return
  }

  if ( !branch.toSmall() ) {
    this.storeSimplePath(branch, path, newHdls, oldHdls, cb)
    return
  }
  //branch.toSmall needs .doMerge

  this.doMerge(branch, path, function(err, mergedBranch, pKey, deadBranch){

    var parent = path.pop()

    oldHdls.push(mergedBranch.hdl)

    if ( !mergedBranch.toBig ) {
      self.store(mergedBranch, function(err, hdl){
        if (err) { cb(err); return }

        //mergedBranch.hdl = hdl // mergedBranch.hdl === hdl after .store
        newHdls.push(mergedBranch.hdl)

        self.removeFromBranch(parent, path, pKey, mergedBranch
                             , newHdls, oldHdls, cb)
      })
    }
  })
} //.removedFromBranch


BpTree.prototype.replaceRoot = function(newRoot, newHdls, oldHdls, cb){
  var self = this
    , oldRoot = this.root

  this.root = newRoot

  oldHdls.push(oldRoot ? oldRoot.hdl : null)

  if (newRoot) {
    this.store(newRoot, function(err, hdl){
      if (err) { cb(err); return }
      newHdls.push(newRoot.hdl) // newRoot.hdl === hdl after .store
      self.cleanHandles(newHdls, oldHdls, cb)
    })
    return
  }
  //newRoot == null

  newHdls.push(null)
  this.cleanHandles(newHdls, oldHdls, cb)
}


BpTree.prototype.cleanHandles = function(newHdls, oldHdls, cb){
  assert.equal(newHdls.length, oldHdls.length)

  var i = 0
  async.eachSeries(
    oldHdls
  , function(oldHdl, ecb){
      var newHdl = newHdls[i]
      i += 1

      // if no oldHdl or oldHdl == newHdl don't release
      if (oldHdl == null || oldHdl.cmp(newHdl) == 0) {
        ecb()
        return
      }
      self.storage.release(oldHdl, ecb)
    }
  , function(err, res){
      if (err) { cb(err); return }



      cb(null) //sucessful .put()
    }) //async.eachSeries
}

BpTree.prototype.doMerge = function(node, path, cb){

  //how can you merge something at root?!?
  assert.ok(path.length > 0)
  assert.ok(node !== this.root)

  //find sibling to merge with

  var parent = path[path.length-1] //non-destructive use of path
    , lNode, rNode
    , sibHdl
    , pKey

  //is the current node the left-most child in parent?
  for (var idx=0; idx<parent.children.length; idx += 1)
    if ( node.hdl.cmp(parent.children[idx]) == 0 ) break

  assert(idx != parent.children.length, "node.hdl not found in parent.children")


  if ( idx == parent.children.length-1 ) { //right-most child
    sibHdl = parent.children[idx-1] //node to the left
    rNode = node
  }
  else {
    //this should be the common case; idx := [ 0, parent.children.length )
    //sibling is the node to the right
    sibHdl = parent.children[idx+1]
    lNode = node
  }

  self.load(sibHdl, function(err, sibNode){
    if (err) { cb(err); return }

    if (lNode) {
      //lNode == node
      rNode = sibNode
      pKey = parent.keys[idx] //lNode == node == parent.children[idx]
    }
    else {
      //rNode == node
      lNode = sibNode
      pKey = parent.keys[idx-1] // lNode == sibNode == parenth.children[idx-1]
    }

    lNode.merge(rNode)

    cb(null, lNode, pKey, rNode)
  })
}


/**
 * Load a node
 *
 * @param {Handle} hdl
 * @param {Function} cb cb(err, node) node might be Branch xor Leaf type
 */
BpTree.prototype.load = function(hdl, cb){
  var self = this

  this.storage.load(hdl, function(err, n){
    if (err) { cb(err); return }

    assert( u.isPlainObject(n), "BpTree#load: n is not a plain object")

    for (var i=0; i<n.children.length; i++)
      assert.ok(typeof n.children[i] != 'undefined')

    var node
    if ( n.type == 'Leaf') {
      node = new Leaf(n.order, n.keys, n.children, self.keyCmp)
    }
    else if ( n.type == 'Branch') {
      assert.equal(
        n.keys.length+1
      , n.children.length
      , format("n.keys.length+1 != n.children.length; hdl=%j; n = %j", hdl, n))

      var Handle = self.storage.constructor.Handle
        , children = n.children.map(function(c){ return Handle.fromJSON(c) })

      assert.equal(n.children.length, children.length)

      node = new Branch(n.order, n.keys, children, self.keyCmp, true)
    }
    else throw new Error("unknown n.type = "+n.type)

    node.hdl = hdl
    cb(err, node)
  })

} //.load()


/**
 * Store a node
 *
 * @param {Node} node
 * @param {Function} cb cb(err, hdl)
 */
BpTree.prototype.store = function(node, cb){
  var self = this

  console.warn("BpTree#store: test this.writing = %j", this.writing)
  if (this.writing) {
    console.warn("node = %s", node)
    cb(new Error("BpTree.prototype.store: store already in progress."))
  }

  this.writing = true
  console.warn("BpTree#store: assignment this.writing = %j", this.writing)

  var n = {
    "type"     : ""
  , "order"    : node.order
  , "keys"     : u.clone(node.keys, true /*deep*/)
  , "children" : []
  }

  if      (node instanceof Leaf  ) {
    assert.equal(node.keys.length, node.children.length) //TMP
    n.type = "Leaf"
    n.children = u.cloneDeep(node.children)
  }
  else if (node instanceof Branch) {
    assert.equal(
      node.keys.length+1, node.children.length
    , format("node.keys.length+1 != node.children.length; node = %s", node) )

    var Handle = this.storage.constructor.Handle
    n.type = "Branch"
    n.children = node.children.map(function(c){ return Handle.toJSON(c) })

    assert.equal(
      n.children.length
    , node.children.length
    , format("n.children.length != node.children.length; %d != %d"
            , n.children.length, node.children.length) )
    assert.equal(
      n.keys.length+1
    , n.children.length
    , format("n.keys.length+1 != n.children.length; n = %j",n) )

  }
  else throw new Error("Unknown node type")

  for (var i=0; i<n.children.length; i++)
    assert.ok(typeof n.children[i] != 'undefined')

  //for a fresh new node (leaf or branch) node.hdl will be null
  this.storage.store(n, node.hdl, function(err, hdl){
    self.writing = false
    console.warn("BpTree#store: stored: assignment self.writing = %j", self.writing)
    if (err) { cb(err); return }

    assert.ok(hdl != null, 'hdl == null')

    node.hdl = hdl //this is done here AUTHORITATIVE!

    cb(null, hdl)
  })

} //.store()


/**
 *
 * @param {Function} cb cb(err)
 */
BpTree.prototype.save = function(cb){
  throw new Error("not implemented")
} //.save()


//