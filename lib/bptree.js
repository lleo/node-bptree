/**
 * @fileOverview Top level B+Tree class with BlockFile block store
 * @author LLeo
 * @version 0.0.0
 */

var assert = require('assert')
  , async = require('async')
  , u = require('lodash')
  , Dequeue = require('dequeue')
  , Handle = require('block-file').Handle
  , Node = require('./node')
  , Branch = require('./branch')
  , Leaf = require('./leaf')

/**
 * B+Tree data struture
 *
 * @param {number} order
 * @param {function} keyType
 */
exports = module.exports = BpTree
function BpTree(order /*, keyType, idType*/) {
  assert(typeof order === 'number', "order must be an number")
  assert(order%1===0, "order must be an integer")
  assert(order > 0, "order must be positive")
//  assert.equal(typeof keyType, 'function', "keyType must be a function")

//  this.keyType = keyType
//  this.idType = idType
  this.order = order
  this.root = null
  this.writing = false
  this.cache = new Cache()
} //constructor

BpTree.Node = Node
BpTree.Leaf = Leaf
BpTree.Branch = Branch
BpTree.Handle = Handle

/**
 * Cache constructor
 */
function Cache(max){
  this.max = max || 1000
  this.nodeQ = new Dequeue()
  this.nodes = []
}

Cache.prototype.size = function(){
  return this.nodeQ.length
}

Cache.prototype.put = function(hdl, node){
  this.nodeQ.push(hdl)
  this.nodes[hdl] = node

  var victimHdl
  if (this.size() > this.max) {
    victimHdl = this.nodeQ.shift()
    delete this.nodes[victimHdl]
  }
  return this
}

Cache.prototype.get = function(hdl){
  return this.nodes[hdl]
}


/**
 * Insert a Key-Value pair in the B+Tree
 *
 * @param {Key} key
 * @param {object} value
 * @param {function} cb cb(err, [...ids])
 */
BpTree.prototype.put = function(key, value, cb){
  var self = this
  if (this.root === null) { //empty tree
    //create leaf node
    this.root = new Leaf(this.order, [key], [value])
    this.store(this.root, cb)
    return
  }

  this.findLeaf(key, function(err, leaf, path){
    if (err) { cb(err); return }
    self.insert(leaf, path, key, value, cb)
  })
} //.put()


/**
 * Retrieve data for a given Key w/o deleting it.
 *
 * @param {Key} key
 * @param {function} cb cb(err, data)
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
 * @param {function} cb cb(err, data)
 * @returns {object} data value assosiated with key.
 */
BpTree.prototype.del = function(key, cb){
  var self = this
  this.findLeaf(key, function(err, leaf, path){
    if (err) { cb(err); return }
    //console.error("***BpTree.del: findLeaf cb: key = %s", key)
    //console.error("***BpTree.del: findLeaf cb: leaf = %s", leaf)
    self.remove(leaf, path, key, cb)
  })
} //.del()


/**
 * Visit each node in-order traversal.
 *
 * @param {function} visit visit(node, isLeaf, depth)
 * @param {function} [done] done(err, res)
 */
BpTree.prototype.traverseInOrder = function(visit, done){
  var self = this
    , res = []

  function traverseNodeInOrder(node, depth, nodeDone){
    //console.error("***BpTree::traverseInOrder: traversNodeInOrder: node=%s", node)
    if (node instanceof Branch) {
      res.push( visit(node, false, depth) )
      async.parallel(
        node.children.map(function(id){
          return function(pcb){ self.load(id, pcb) }
        })
      , function(err, childs){
          if (err) { done(err); return }
          async.parallel(
            childs.map(function(n){
              return function(pcb){ traverseNodeInOrder(n, depth+1, pcb) }
            })
          , function(err) {
              if (err) { nodeDone(err); return }
              nodeDone(null)
            }) //async.parallel traverse each node
        }) //async.parallel id
    }
    else if (node instanceof Leaf){
      res.push( visit(node, true, depth) )
      nodeDone(null)
    }
  }

  traverseNodeInOrder(this.root, 0, function(err){
    var hasDone = typeof done === 'function'
    if (err) {
      if (hasDone) { done(err); return }
      else throw new Error(err)
    }
    hasDone && done(null, u.flatten(res))
  })
} //.traverseInOrder()



/**
 * Visit each key/data pair in order.
 *
 * @param {function} visit visit(key, data)
 * @param {function} [done] done(err, res)
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
 * @param {function} cb cb(err, leaf, path) path isa Array of Branch objects
 */
BpTree.prototype.findLeaf = function(key, cb){
  if (this.root === null) { cb(null, null); return }

  var self = this
    , path = []

  function find(err, node) {
    if (err) { cb(err); return }
    if (node instanceof Branch) {
      //node isa Branch so node.children contains IDs
      path.push(node)
      for (var i=0; i<node.keys.length; i++)
        if ( key.cmp(node.keys[i]) < 0 ) break
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
 * @param {function} cb cb(err, [...ids])
 */
BpTree.prototype.insert = function(node, path_, key, child, cb) {
  var self = this
    , path = u.clone(path_)

  node.add(key, child)
  if ( !node.toBig() ) {
    this.storePath(node, path, function(err, nodeId, pathIds){
      if (err) { cb(err); return }
      cb(null, u.flatten([nodeId, pathIds]))
    })
    return
  }
  //node.toBig() needs node.split()

  var pair = node.split()
    , pKey = pair[0] //pKey parent key
    , sibl = pair[1] //nNode next sibling node to "node"

  var oNodeId, nNodeId

  async.series(
    [ function(scb){
        oNodeId = node.id
        nNodeId = self.store(node, scb)
      }
    , function(scb){
        self.store(sibl, scb) //first time sibl.id gets set
      }
    , function(scb){
        //if (node === self.root)
        if (path.length == 0) {
          self.root = new Branch(self.order, [pKey], [node.id, sibl.id])
          path.unshift(self.root)
          self.store(self.root, scb)
        }
        else {
          var parent = path.pop()
          self.insert(parent, path, pKey, sibl, scb)
        }
      }
    ]
  , function(err, res){
      if (err) { cb(err); return }
      //FIXME: mark old ids free ?
      cb(null, u.flatten(res) )
    }
  )
} //.insert()


/**
 * Remove a child from a node and propengate changes up the tree if needed.
 *
 * @param {Node} parent
 * @param {Array(Branch)} path
 * @param {Key} key
 * @param {function} cb cb(err, ?)
 *
 */
BpTree.prototype.remove = function(node, path_, key, cb) {
  var self = this
    , path = u.clone(path_)
    , pair, idx, data
    , reverse, parent

  //console.error("***BpTree.remove: node = %s", node)
  //console.error("***BpTree.remove: path = %s", path)
  //console.error("***BpTree.remove: key = %s", key)

  pair = node.remove(key)
  assert(!u.isUndefined(pair), "node.remove(key) failed")
  idx  = pair[0]
  data = pair[1]

  //console.error("***BpTree.remove: node.remove(key) => Pair(idx,data) = [%d, %s]", idx, data)

  if (node === this.root) {
    //console.error("***BpTree.remove: node == this.root")
    assert.strictEqual(path.length, 0)
    if (node instanceof Branch) {
      //console.error("***BpTree.remove: this.root is a Branch")
      if (node.size() > 1) {
        //console.error("***BpTree.remove: this.root still has plenty; store it")
        this.storePath(node, path, cb)
      }
      else {
        //console.error("***BpTree.remove: this.root node is empty; promote last child")
        //promote the one remaining child to root
        this.load(this.root.children[0], function(err, node){
          if (err) { cb(err); return }

          self.root = node
          cb(null)
        })
      }
    }
    else if (node instanceof Leaf) {
      //console.error("***BpTree.remove: this.root is a Leaf")
      if (node.size() > 0) {
        //console.error("***BpTree.remove: this.root has plenty left store it")
        this.storePath(node, path, cb)
      }
      else {
        //console.error("***BpTree.remove: this.root node is empty; set to null")
        this.root = null
        cb(null)
      }
    }
    else throw new Error("WTF!! this.root not a Leaf or Branch")
    return
  }
  if ( !node.toSmall() ) {
    //console.error("***BpTree.remove: !node.toSmall: node = %s", node)
    this.storePath(node, path, cb)
    return
  }
  //node.toSmall() gotta merge/split/store rinse&repeat

  //console.error("***BpTree.remove: AFTER node.remove(key): node.toSmall: node = %s", node)

  reverse = false
  parent = path[path.length-1]

  //console.error("***BpTree.remove: node.id = %s", node.id)
  //console.error("***BpTree.remove: parent = %s", parent)

  //who am i; which child(idx) of parent is node.id
  var nodeIdx, siblIdx //node and sibl id indexes in parent
  for (nodeIdx=0; nodeIdx < parent.children.length; nodeIdx++)
    if ( node.id.equals(parent.children[nodeIdx]) ) break

  assert(nodeIdx != parent.children.length, "did not find node.id in parent.children")

  if (nodeIdx == parent.children.length-1) {
    //if node.id is the last child of parent
    // pick the previous sibling of node and reverse lnode and rnode for merge.
    // reverse cuz we always merge right
    //this is ok cuz parent.children.length >= 2 under all valid values of order
    reverse = true
    siblIdx = nodeIdx - 1
  }
  else {
    //else pick the next(+1) sibling of node; we merge right
    siblIdx = nodeIdx + 1
  }
  self.load(parent.children[siblIdx], loaded)

  //remember this is hoisted up to the beginning of the function scope
  // its here for ease of reading
  function loaded(err, sibl){
    var lnode, rnode
      , pKey, pair, nNode

    if (reverse) { lnode = sibl; rnode = node }
    else         { lnode = node; rnode = sibl }

    //console.error("***BpTree.removed: loaded cb: node = %s", node)
    //console.error("***BpTree.removed: loaded cb: sibl = %s", sibl)
    //console.error("***BpTree.removed: loaded cb: reverse = %s", reverse)
    //console.error("***BpTree.removed: loaded cb: lnode = %s", lnode)
    //console.error("***BpTree.removed: loaded cb: rnode = %s", rnode)
    ////console.error("***BpTree.removed: loaded cb: path = %s", path)

    lnode.merge(rnode)

    parent = path.pop()
    pKey = rnode.keys[0]
    //parent.remove(pKey)
    //console.error("***BpTree.removed: loaded cb: parent = %s", parent)
    //console.error("***BpTree.removed: loaded cb: pKey = %s", pKey)

    async.series(
      [ function(scb){
          var pair, nKey, nNode
          if (lnode.toBig()) {
            pair  = lnode.split()
            nKey  = pair[0]
            nNode = pair[1]
            parent.add(nKey, nNode)
            self.store(nNode, scb)
            return
          }
          scb(null)
        }
      , function(scb){
          self.store(lnode, scb)
        }
      , function(scb){
          self.remove(parent, path, pKey, scb)
        }
      ]
    , function(err, res){
        if (err) { cb(err); return }
        cb(null, u.flatten(res))
      }
    )
  } //loaded()
} //.remove()


/**
 * Load a node
 *
 * @param {Handle} hdl
 * @param {function} cb cb(err, node)
 */
var STORE = {}
  , curSegNum = 0
  , curBlkNum = 0

function nextHandle() {
  if (curBlkNum >= Handle.MAX_BLOCKNUM) {
    curBlkNum = 0
    curSegNum += 1
  }
  if (curSegNum > Handle.MAX_SEGNUM)
    throw new Error("ran out of Handle namespace")

  return new Handle(curSegNum, curBlkNum++, 0)
}

BpTree.prototype.load = function(hdl, cb){
  var self = this
    , node

  node = this.cache.get(hdl)
  if (node) {
    process.nextTick(function(){ cb(null, node) })
    return
  }

  function loaded(err, node) {
    //if it is an Inner node add it to the cache
    if (node instanceof Node) self.cache.put(hdl, node)
    cb(err, node)
  }

  setTimeout(function(){
    var node = STORE[hdl]
      , err=null
    if (!node) {
      err = new Error("hdl not it STORE")
      err.code = "ENOEXIST"
    }
    loaded(err, node)
  }, 100)
} //.load()


/**
 * Store a node
 *
 * @param {Node} node
 * @param {function} cb cb(err, id, node)
 */
BpTree.prototype.store = function(node, cb){
  var self = this
    , hdl = nextHandle()

  //console.error("***BpTree.store: id = %s", hdl)
  //console.error("***BpTree.store: node = %s", node)

  if (this.writing) {
    throw new Error("store already in progress.")
  }

  function stored(err, id) {
    if (err) { cb(err); return }
    node.id = id
    self.writing = false
    cb(null, id)
  }

  this.writing = true

  if (node instanceof Branch) self.cache.put(hdl, node)

  setTimeout(function(){
    STORE[hdl] = node
    stored(null, hdl)
  }, 100)

  return hdl
} //.store()


/**
 * Store a node and optional sibling. along with the path. (COW semantics)
 *
 * @param {Node} node
 * @param {Node|null} sib
 * @param {Array(Node)} path path is root..parent
 * @param {function} cb cb(err, nodeId, pathIds)
 */
BpTree.prototype.storePath = function(node, path_, cb){
  var self = this
    , path = u.clone(path_)

  //path[0] == root
  if (path.length > 0) assert.strictEqual(this.root, path[0])

  path.push(node)
  path.reverse() //path now goes upto root

  //path.map(function(n,i){console.error("***BpTree.storePath: path[%d] = %s", i, n)})

  var oldIds = []

  async.series(
    path.map(function(n, i, a){ //n == node; i == index; a == path
      return function(scb){
        var oldId = n.id
          , newId = self.store(n, scb)
        oldIds.push(oldId)
        if (i+1 < a.length) //parent exists
          a[i+1].setChild(oldId, newId) //a[i+1] == parent
      }
    })
  , function(err, pathIds){
      if (err) { cb(err); return }

      pathIds.reverse()
      oldIds.reverse()

      //self.rootId = pathIds[0]
      var nodeId = pathIds.pop()

      cb(null, nodeId, pathIds, oldIds)
    })

} //.storePath()


/**
 *
 * @param {function} cb cb(err)
 */
BpTree.prototype.save = function(cb){
  throw new Error("not implemented")
} //.save()


//