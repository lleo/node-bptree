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
  , OpData = require('./op_data')
  , Node = require('./node')
  , Branch = require('./branch')
  , Leaf = require('./leaf')
  , strOps = require('./str_ops')
  , strCmp = strOps.cmp
  , TrivialStore = require('./trivial_store')
  , Stats = require('stats-api')

//Setup library wide Stats NameSpace & Stats
var bptStats = Stats().createNameSpace("bptree")

bptStats.createStat('tt_put', Stats.TimerNS)
bptStats.createStat('tt_get', Stats.TimerNS)
bptStats.createStat('tt_del', Stats.TimerNS)
bptStats.createStat('tt_store', Stats.TimerNS)
bptStats.createStat('tt_load', Stats.TimerNS)
bptStats.createStat("tt_put ravg", Stats.RunningAverage
                  , { stat: bptStats.get("tt_put") })
bptStats.createStat("tt_store ravg", Stats.RunningAverage
                  , { stat: bptStats.get("tt_store") })
bptStats.createStat("tt_load ravg", Stats.RunningAverage
                  , { stat: bptStats.get("tt_load") })
bptStats.createHistogram("hog tt_put", "tt_put", Stats.semiLogNS)
bptStats.createHistogram("hog tt_get", "tt_get", Stats.semiLogNS)
bptStats.createHistogram("hog tt_del", "tt_del", Stats.semiLogNS)
bptStats.createHistogram("hog tt_store", "tt_store", Stats.semiLogNS)
bptStats.createHistogram("hog tt_load", "tt_load", Stats.semiLogNS)

bptStats.createStat('load_cnt', Stats.Count, {stat: bptStats.get('tt_load')})
bptStats.createStat('store_cnt', Stats.Count, {stat: bptStats.get('tt_store')})

BpTree.STATS = bptStats

var DEFAULT_OPTIONS = {
  cow   : false
, order : 3
}

/**
 * B+Tree data struture
 *
 * @param {number} order
 * @param {Function} keyType
 */
exports = module.exports = BpTree
function BpTree(keyCmp, storage, opts) {
  // Fill in default arguments in reverse order
  //

  // Options
  opts = u.defaults(opts||{}, DEFAULT_OPTIONS)
  this.cow = opts.cow ? true : false

  if (u.has(opts, 'order')) {
    this.leafOrder = opts.order
    this.branchOrder = opts.order
  }
  else {
    assert.ok( u.has(opts, 'leafOrder') )
    assert.ok( u.has(opts, 'branchOrder') )
    this.leafOrder = opts.leafOrder
    this.branchOrder = opts.branchOrder
  }

  assert(typeof this.leafOrder === 'number', "this.leafOrder must be an number")
  assert(this.leafOrder%1===0, "this.leafOrder must be an integer")
  assert(this.leafOrder > 1, "this.leafOrder must be greater than 1")

  assert(typeof this.branchOrder === 'number', "this.branchOrder must be an number")
  assert(this.branchOrder%1===0, "this.branchOrder must be an integer")
  assert(this.branchOrder > 1, "this.branchOrder must be greater than 1")

//  assert.ok( this.leafOrder >= this.branchOrder )

  // Storage
  this.storage = typeof storage == 'undefined' ? new TrivialStore() : storage
  assert.ok(u.isObject(this.storage))

  // Key Compare function
  this.keyCmp = keyCmp == null || keyCmp == 'string' ? strCmp : keyCmp
  assert.ok(typeof this.keyCmp == 'function'
           , "BpTree#constructore: keyCmp must be a function: keyCmp="+keyCmp)

  this.root = null
  this.writing = false
} //constructor

BpTree.Node = Node
BpTree.Leaf = Leaf
BpTree.Branch = Branch

/**
 * Get the root Handle from the storage
 *
 * @param {Function} cb cb(err)
 */
BpTree.prototype.loadRoot = function(cb){
  var self = this
  this.storage.loadRootHandle(function(err, hdl){
    if (err) { cb(err); return }

    if (hdl !== null) {
      self.load(hdl, null, function(err, node){
        if (err) { cb(err); return }
        self.root = node
        cb(null)
      })
      return
    }

    self.root = null
    cb(null)
  })
}

/**
 * Insert a Key-Value pair in the B+Tree
 *
 * @param {Key} key
 * @param {object} value
 * @param {Function} cb cb(err)
 */
BpTree.prototype.put = function(key, value, cb){
  var self = this
    , doneNS = bptStats.get('tt_put').start()


  assert(!u.isUndefined(key)  , "BpTree#put: key of undefined is not allowed")
  assert(!u.isUndefined(value), "BpTree#put: value of undefined is not allowed")

  var opData = new OpData("put")

  if (this.root === null) {
    var newRoot = new Leaf(this.leafOrder, [key], [value], this.keyCmp)
    newRoot.parent = null

    this.storeNode(newRoot, opData, function(err){
      if (err) { cb(err); return }
      doneNS()
      cb(null)
    })
    return
  }

  this.findLeaf(key, opData, function(err, leaf){
    if (err) { cb(err); return }

    self.insertIntoLeaf(leaf, key, value, opData, function(err){
      if (err) { cb(err); return }
      doneNS()
      cb(null)
    })
  })

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
    , doneNS = bptStats.get('tt_get').start()

  var opData = new OpData("get")

  this.findLeaf(key, opData, function(err, leaf, path){
    if (err) { cb(err); return }
    doneNS()

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

  var doneNS = bptStats.get('tt_del').start()

  var opData = new OpData("del")

  this.findLeaf(key, opData, function(err, leaf){
    var idx, data
    if (err) { cb(err); return }

    data = leaf.get(key)
    if ( u.isUndefined(data) ) {
      doneNS()
      cb(null, null) //null means not found
      return
    }

    self.removeFromLeaf(leaf, key, opData, function(err, deadHdls){
      if (err) { cb(err); return }
      doneNS()
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
          return function(pcb){ self.load(hdl, node, pcb) }
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
      res.push( visit(node, false /*isLeaf*/, depth) )
      async.series(
        node.children.map(function(hdl, i, nc){
          return function(next){
            self.load(hdl, node, function(err, child){
              if (err) { next(err); return }
              traverseNodeInOrder(child, depth+1, next)
            })
          }
        })
      , nodeDone) //async.series hdl
    }
    else if (node instanceof Leaf){
      res.push( visit(node, true /*isLeaf*/, depth) )
      nodeDone(null, node.hdl)
    }
    else throw new Error("WTF!")
  }

  if (this.root === null) {
    done(null, [])
    return
  }
  traverseNodeInOrder(this.root, 0, function(err, res){
//    console.log("nodeDone: err=%s; res=[%s]", err, res)
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
 * Find the depth of the tree by following the left most key
 *
 * @param {Function} cb(err, depth)
 */
BpTree.prototype.findDepth = function(cb){
  var self = this
    , depth = 0

  if (this.root == null) {
    setImmediate(function(){ cb(null, depth) })
    return
  }

  var opData = new OpData("findDepth")

  this._findDepth(this.root, depth+1, opData, cb)
}

BpTree.prototype._findDepth = function(node, depth, opData, cb){
  var self = this

  if (node instanceof Leaf) {
    setImmediate(function(){ cb(null, depth) })
    return
  }

  assert.ok(node instanceof Branch)

  this.load(node.children[0], node, function(err, node_){
    if (err) { cb(err); return }
    opData.loaded(node.children[0])
    self._findDepth(node_, depth+1, opData, cb)
  })
}


/**
 * Search BpTree for a leaf node which would have a given key
 *
 * @param {Key} key
 * @param {OpData} opData
 * @param {Function} cb cb(err, leaf)
 */
BpTree.prototype.findLeaf = function(key, opData, cb){
  var self = this

  if (this.root === null) {
    cb(new Error("BpTree is empty"))
    return
  }

  function find(err, node) {
    if (err) { cb(err); return }

    if (node instanceof Branch) {
      for (var i=0; i<node.keys.length; i++)
        if ( self.keyCmp(key, node.keys[i]) < 0 ) break

      self.load(node.children[i], node, find)
      return
    }

    assert.ok( node instanceof Leaf
             , format("BpTree#findLeaf: node !instanceof Leaf; node=", node))

    cb(null, node)
  }

  find(null, this.root)
} //.findLeaf()


/**
 * Insert a Key and child into a Node of this BpTree.
 *
 * @param {Node} node inner Node to insert key and Node|Leaf into.
 * @param {Key} key
 * @param {Node} child
 * @param {OpData} opData
 * @param {Function} cb cb(err)
 */
BpTree.prototype.insertIntoLeaf = function(leaf, key, child, opData, cb) {
  assert(leaf instanceof Leaf, "WTF! this is supposed to be leaf instanceof Leaf")

  var self = this

  leaf.add(key, child)
  if ( !leaf.toBig() ) { //no split is necessary, just store
    this.storeNode(leaf, opData, cb)
    return
  }
  //node.toBig() needs node.split()

  var lLeaf = leaf
    , pair  = leaf.split()
    , pKey  = pair[0]
    , rLeaf = pair[1]

  this.storeSplitNode(lLeaf, pKey, rLeaf, opData, cb)
} //.insertIntoLeaf()


/**
 * Remove a child from a Leaf by Key
 * and propengate changes up the tree if needed.
 *
 * @param {Leaf} leaf
 * @param {Key} key
 * @param {OpData} op
 * @param {Function} cb cb(err)
 *
 */
BpTree.prototype.removeFromLeaf = function(leaf, key, opData, cb) {
  this.removeFromNode(leaf, key, null, opData, cb)
}


/**
 * Remove a entry from a Node by key
 * and propengate changes up the tree if needed.
 *
 * @param {Node} node node being modified
 * @parem {Key} key key of the column to be removed
 * @param {Node} rplNode the replacement node for i+1
 * @param {OpData} opData
 * @param {Function} cb cb(err)
 *
 */
BpTree.prototype.removeFromNode = function(node, key, rplNode, opData, cb){
  assert(node instanceof Node, "WTF! this is supposed to be a Node")

  var self = this

  node.remove(key)

  if (this.root === node) {
    assert.ok( node.parent === null
             , "BpTree#removedFromNode: node.parent !== null" )

    if (node.size() > 0) {
      this.storeNode(node, opData, cb)
    }
    else {
      if (rplNode === null) {
        this.replaceRoot(rplNode, opData, cb)
        return
      }

      assert.ok(node.children.length == 1)

      opData.deadHdl(node.hdl)

      //rplNode becomes root indicated by rplNode.parent == null
      rplNode.parent = null
      this.storeNode(rplNode, opData, cb)
    }

    return
  }

  if ( !node.toSmall() ) {
    this.storeNode(node, opData, cb)
    return
  }
  //node.toSmall needs .mergeNode

  this.mergeNode(node, opData, function(err, mergedNode, pKey, deadNode){
    var parent = mergedNode.parent

    opData.deadHdl(deadNode.hdl)

    if ( !mergedNode.toBig() ) {

      var mergedNodeOldHdl = mergedNode.hdl

      self.store(mergedNode, function(err){
        if (err) { cb(err); return }

        opData.stored(mergedNodeOldHdl, mergedNode.hdl)

        self.removeFromNode(parent, pKey, mergedNode, opData, cb)
      })
      return
    }
    //mergedNode.toBig needs a .split

    var pair = mergedNode.split()
      , npKey = pair[0]
      , rNode = pair[1]

    var rmHdl = parent.remove(pKey)
      , rNodeOldHdl = rNode.hdl

    //rmHdl == deadNode.hdl; so it is already in dealHdls[]

    self.store(rNode, function(err){
      if (err) { cb(err); return }

      opData.stored(rNodeOldHdl, rNode.hdl)

      parent.add(npKey, mergedNode.hdl, rNode.hdl)

      self.storeNode( mergedNode, opData, cb)
    }) //.store rNode
  }) //.mergeNode
} //.removeFromNode


/**
 *
 * @param {Node} node
 * @param {OpData} opData
 * @param {Function} cb cb(err, mergedBranch, pKey, deadBranch)
 */
BpTree.prototype.mergeNode = function(node, opData, cb){
  var self = this

  //how can you merge something at root?!?
  assert.ok( node.parent !== null )
  assert.ok(node !== this.root)

  //find sibling to merge with

  var parent = node.parent
    , lNode, rNode
    , sibHdl
    , pKey

  //is the current node the left-most child in parent?
  for (var idx=0; idx<parent.children.length; idx += 1)
    if ( node.hdl.equals(parent.children[idx]) ) break

  assert(idx != parent.children.length, "node.hdl not found in parent.children")

  if ( idx == parent.children.length-1 ) { //right-most child
    sibHdl = parent.children[idx-1] //node to the left
    rNode = node
    //lNode == undefined
  }
  else {
    //this should be the common case; idx := [ 0, parent.children.length )
    //sibling is the node to the right
    sibHdl = parent.children[idx+1]
    lNode = node
    //rNode == undefined
  }

  assert(sibHdl, format("sibHdl == %s; idx=%d; parent.children.length=%d"
                       , sibHdl, idx, parent.children.length) )

  this.load(sibHdl, parent, function(err, sibNode){
    if (err) { cb(err); return }

    opData.loaded(sibHdl)

    if (lNode) { //if lNode is defined; pKeyIdx = idx,  rNodeIdx=idx+1
      //lNode == node
      rNode = sibNode
      pKey = parent.keys[idx] //lNode == node == parent.children[idx]
    }
    else { //lNode is undefined; pKeyIdx = idx-1, rNodeIdx = idx
      //rNode == node
      lNode = sibNode
      pKey = parent.keys[idx-1] // lNode == sibNode == parent.children[idx-1]
    }

    self.merge(lNode, rNode, opData, function(err){
      if (err) { cb(err); return }

      //(err, mergedBranch, pKey, deadBranch)
      cb(null, lNode, pKey, rNode)
    }) //merge
  })
} //.mergeNode


/**
 * Abstract merge operation acrose Leaf and Branch
 *
 * @param {Node} lNode
 * @param {Node} rNode
 * @param {OpData} opData
 * @param {Function} cb
 */
BpTree.prototype.merge = function(lNode, rNode, opData, cb){
  assert.equal(lNode.constructor, rNode.constructor)

  if (lNode instanceof Leaf) {
    lNode.merge(rNode)
    cb(null, lNode, rNode)
    return
  }
  assert(lNode instanceof Branch)
  this.findLeftMostKey(rNode, opData, function(err, lmKey){
    if (err) { cb(err); return }

    lNode.merge(rNode, lmKey)

    cb(null, lNode, rNode)
  })
} //.merge


/**
 * Traverse a node recusively to find the left-most key
 *
 * @param {Node} node
 * @param {OpData} opData
 * @param {Function} cb cb(err, key)
 */
BpTree.prototype.findLeftMostKey = function(node, opData, cb){
  var self = this

  if (node instanceof Leaf) {
    var key = node.keys[0]
    cb(null, key)
    return
  }

  this.load(node.children[0], node, function(err, n){
    if (err) { cb(err); return }

    opData.loaded(node.children[0])

    self.findLeftMostKey(n, opData, cb)
  })
} //.findLeftMostKey


/**
 * Store and node and replace the new handle in parent, then bubble up the
 * changes to the parent.
 *
 * @param {Node} node
 * @param {OpData} opData
 * @param {Function} cb cb(err)
 */
BpTree.prototype.storeNode = function(node, opData, cb){
  var self = this

  var oldHdl = node.hdl

  this.store(node, function(err){
    if (err) { cb(err); return }

    opData.stored(oldHdl, node.hdl)

    if (node.parent === null) {
      self.replaceRoot(node, opData, cb)
      return
    }

    var parent = node.parent

    if (oldHdl && !oldHdl.equals(node.hdl))
      parent.updateHdl(oldHdl, node.hdl)

    if (parent.dirty) {
      if (oldHdl) parent.updateHdl(oldHdl, node.hdl)

      self.storeNode(parent, opData, cb)
      return
    }

    self.cleanHandles(opData, cb)
    return
  })
} //.storeNode()


/**
 * Store a split node (lNode, pKey, rNode) and bubble up the changes
 *
 * @param {Node} lNode
 * @param {Key} pKey
 * @param {Node} rNode
 * @param {OpData} opData
 * @param {Function} cb cb(err)
 */
BpTree.prototype.storeSplitNode = function(lNode, pKey, rNode, opData, cb){

  var self = this

  assert.ok( lNode.parent === rNode.parent, "lNode.parent !== rNode.parent")

  var lHdl = lNode.hdl
    , rHdl = rNode.hdl

  async.series(
    [ function(scb){
        self.store(lNode, function(err){
          if (err) { cb(err); return }
          opData.stored(lHdl, lNode.hdl)
          scb(null, lNode.hdl)
        })
      }
    , function(scb){
        self.store(rNode, function(err){
          if (err) { cb(err); return }
          opData.stored(rHdl, rNode.hdl)
          scb(null, rNode.hdl)
        })
      }
    ]
  , function(err, res){
      var parent = lNode.parent

      if ( parent === null ) {
        var newRoot = new Branch( self.branchOrder
                        , [pKey], [lNode.hdl, rNode.hdl], self.keyCmp)
          , newRootOldHdl = newRoot.hdl

        newRoot.parent = null

        self.store(newRoot, function(err){
          if (err) { cb(err); return }
          opData.stored(newRootOldHdl, newRoot.hdl)
          self.replaceRoot(newRoot, opData, cb)
        })
        return
      }

      parent.add(pKey, lNode.hdl, rNode.hdl)

      if ( !parent.toBig() ) {
        self.storeNode(parent, opData, cb)
        return
      }
      //parent.toBig needs split

      var pair = parent.split()
        , nKey  = pair[0]
        , nNode = pair[1]

      //                  lNode   pKey  rNode
      self.storeSplitNode(parent, nKey, nNode, opData, cb)
    })

}


/**
 *
 * @param {Node} newRoot
 * @param {OpData} opData
 * @param {Function} cb cb(err)
 */
BpTree.prototype.replaceRoot = function(newRoot, opData, cb){
  var self = this
    , oldRoot = this.root

//  console.warn("BpTre#replaceRoot: newRoot=%s; oldRoot=%s;", newRoot, oldRoot)

  this.root = newRoot

  if (oldRoot && newRoot == null)
    opData.deadHdl(oldRoot.hdl)

  self.cleanHandles(opData, function(err){
    if (err) { cb(err); return }

    var rootHdl = newRoot ? newRoot.hdl : null

    self.storage.storeRootHandle(rootHdl, cb)
  })
}


/**
 * Clean up handles no longer used by the tree.
 *
 * @param {OpData} opData
 * @param {Function} cb cb(err)
 */
BpTree.prototype.cleanHandles = function(opData, cb){
  var self = this
    , deadHdls

  deadHdls = opData.deadHdls()

  async.eachSeries(
    deadHdls
  , function(deadHdl, ecb){
      assert.ok(deadHdl)
      self.storage.release(deadHdl, ecb)
    }
  , function(err, res){
      if (err) { cb(err); return }
//      self.storage.flush(cb)
      cb(null)
    }) //async.eachSeries
}


/**
 * Create a Plain-ole-JSON object to represet a given node.
 *
 * @param {Node} node a Leaf or Branch object
 * @return {Object} a Plain-ole-JSON object
 */
BpTree.prototype.nodeToJson = function(node){
  var self = this
    , n

  if (node instanceof Leaf) {
    n = { "type"     : "Leaf"
        , "order"    : node.order
        , "keys"     : node.keys
        , "children" : node.children
        }
  }
  else if (node instanceof Branch) {
    n = { "type"     : "Branch"
        , "order"    : node.order
        , "keys"     : node.keys
        , "children" : node.children.map(function(hdl){
                         return self.storage.handleToJson(hdl)
                       })
        }
  }
  else throw new Error("WTF! node !instanceof Leaf || Branch")

  return n
}


/**
 * Create a node from a Plain-ole-JSON object created to represet this node.
 *
 * @param {Object} n Plain-old-JSON object
 * @return {Node} a Leaf or Branch object
 */
BpTree.prototype.nodeFromJson = function(n){
  var self = this
    , node

  if (n.type == "Leaf") {
    node = new Leaf(n.order, n.keys, n.children, this.keyCmp)
  }
  else if (n.type == "Branch") {
    var children = n.children.map(function(c){
                     return self.storage.handleFromJson(c)
                   })
    node = new Branch(n.order, n.keys, children, this.keyCmp)
  }
  else throw new Error('WTF! n.type != "Leaf" || "Branch"')

  return node
}


/**
 * Get a handle to represent a given Node. This is a policy
 * decision. Currently that policy is:
 *   1. if this.cow is true, allocate a new Handle
 *   2. if the given node does not have a handle, allocate a new Handle
 *   3. if the given node.hdl is not big enough to store the handle,
 *      allocate a new Handle
 *   4. re-use given node.hdl
 *
 * @param {Node} node
 * @return {Handle}
 */
BpTree.prototype.getHandle = function(node){
  var hdl, n

  n = this.nodeToJson(node)

  if (this.cow || node.hdl == null || !this.storage.isBigEnough(node.hdl, n)) {
    hdl = this.storage.reserve( n )
    assert(hdl, "getHandle: Failed to reserve() handle for node")
  }
  else
    hdl = node.hdl

  return hdl
}


/**
 * Load a node
 *
 * @param {Handle} hdl
 * @param {Function} cb cb(err, node) node might be Branch xor Leaf type
 */
BpTree.prototype.load = function(hdl, parent, cb){
  assert.ok( (parent instanceof Node) || parent === null
           , "parent must be defined or null")

  var self = this

  var loadedNS = bptStats.get('tt_load').start()

  this.storage.load(hdl, function(err, n){
    if (err) { cb(err); return }

    var node = self.nodeFromJson(n)

    node.hdl = hdl
    node.parent = parent

    loadedNS()

    cb(err, node)
  })

} //.load()


/**
 * Store a node. Updates node.hdl if it changes.
 *
 * @param {Node} node
 * @param {Function} cb cb(err)
 */
BpTree.prototype.store = function(node, cb){
  var self = this

  if (this.writing) {
    console.error("*** this.writing == true; node = %s", node)
    cb(new Error("BpTree.prototype.store: store already in progress."))
  }

  this.writing = true

  var storedNS = bptStats.get('tt_store').start()

  var n = this.nodeToJson(node)
    , hdl = this.getHandle(node)

  //for a fresh new node (leaf or branch) node.hdl will be null
  this.storage.store(n, hdl, function(err, newHdl){
    self.writing = false

    if (err) { cb(err); return }

    //if (node.hdl.cmp(hdl) != 0) //node.hdl != hdl
    node.hdl = newHdl //this is done here AUTHORITATIVE!

    node.dirty = false //NOTE: only matters for Branch nodes

    storedNS()

    cb(null)
  })

} //.store


/**
 *
 * @param {Function} cb cb(err)
 */
BpTree.prototype.save = function(cb){
  throw new Error("not implemented")
} //.save()


//
