#!/usr/bin/env node

var assert = require('assert')
  , util = require('util')
  , format = util.format
  , u = require('lodash')
  , BpTree = require('..')
  , Node = BpTree.Node
  , Leaf = BpTree.Leaf
  , Branch = BpTree.Branch
  , Handle = BpTree.Handle
  , Key = BpTree.Key
  , async = require('async')
  , log = console.log
  , node
  , randOps = require('../lib/rand_ops')
  , rand = randOps.rand
  , randInt = randOps.randInt
  , randomize = randOps.randomize
  , strOps = require('../lib/str_ops')
  , strCmp = strOps.cmp
  , incStr = strOps.inc
  , multStr = strOps.repeat
  , TrivialStore = require('../lib/trivial_store')
  , MemStore = require('../lib/mem_store')
  , cmdLine = require('commander')

var keyOrder = function(s){
  if (s == "rev" || s == "reverse")
    return "reverse"
  if (s == "rand" || s == "random" || s == "randomize")
    return "randomize"
  return "inorder"
}

var parseStore = function(s){
  if (s == 'triv' || s == 'trivial')
    return 'TrivialStore'
  if (s == 'mem' || s == 'memory')
    return 'MemStore'
  return 'TrivialStore'
}

cmdLine
.version('0.0.1')
.option('-o|--order <n>', 'order of the BpTree', parseInt, 3)
.option('-n|--number <n>', 'Number of Key/Val pairs', parseInt, 10)
.option('-d|--deletion [key-order]', "Order of deletion: inorder, (rev)erse, (rand)omize", keyOrder, 'inorder')
.option('-i|--insertion [key-order]', "Order of insertion: inorder, (rev)erse, (rand)omize", keyOrder, 'inorder')
.option('-D|--io-delay <ms>', "Delay each delete (milliseconds)", parseInt, -1)
.option('--no-display-delete', "Don't display the tree after each delete")
.option('-S|--use-store <store>', "IO Store to use '(mem)ory', '(triv)ial'", parseStore, 'TrivialStore')
.parse(process.argv)

console.log("order             = %d", cmdLine.order)
console.log("number            = %d", cmdLine.number)
console.log("insertion         = %s", cmdLine.insertion)
console.log("deletion          = %s", cmdLine.deletion)
console.log("io-delay          = %d", cmdLine.ioDelay)
console.log("display-delete    = %j", cmdLine.displayDelete)
console.log("use-store         = %s", cmdLine.useStore)
//process.exit(0)

var displayNode = (
  function(){
    var cnt=0
    return function(node, isLeaf, depth){
      var indent = depth>0 ? multStr("  ", depth) : ""
      console.log("%s%s", indent, node)
      return cnt++
    }
  }
)()

var order = cmdLine.order
  , numEnts = cmdLine.number
  , insOrder = cmdLine.insertion
  , delOrder = cmdLine.deletion
  , ioDelay = cmdLine.ioDelay
  , displayDelete = cmdLine.displayDelete
  , useStore = cmdLine.useStore
  , store
  , tree
  , tKey = ""
  , keys = []
  , kv = {}
  , delKeys, insKeys

if (useStore == 'MemStore') {
  store = new MemStore(ioDelay)
}
if (useStore == 'TrivialStore') {
  store = new TrivialStore(ioDelay)
}

tree = new BpTree(order, strOps.cmp, store)

for (var ki = 0; ki<numEnts; ki++){
  keys.push( tKey = incStr(tKey) )
  kv[tKey] = ki
}


insKeys = u.clone(keys)
if (insOrder == "randomize")
  randomize(insKeys)
else if (insOrder == "reverse")
  insKeys.reverse()

delKeys = u.clone(keys)
if (delOrder == "randomize")
  randomize(delKeys)
else if (delOrder == "reverse")
  delKeys.reverse()

var buildTree =  function(scb){
      console.log("Build Tree: %d tree.put() calls", keys.length)
      console.log("----------")
      async.eachSeries(
        insKeys
      , function(k, ecb){
          process.stdout.write(format("tree.put( %s, %d )", k, kv[k]))
          tree.put(k, kv[k], function(err){
            if (err) {
              process.stdout.write(" failed.\n")
              console.error(err)
              ecb(err)
              return
            }
            process.stdout.write(" success\n")
            ecb()
          })
        }
      , function(err){
          if (err) { scb(err); return }
          console.log("")
          console.log("Initial Tree")
          console.log("------------")
          tree.traverseInOrder(displayNode, scb)
        })
    }
  , delTree = function(scb){
      console.log("")
      console.log("Modify Tree")
      console.log("-----------")

      async.eachSeries(
        delKeys
      , function(k, ecb){
          process.stdout.write(format("tree.del(%s)", k))

          tree.del(k, function(err){
            if (err) {
              process.stdout.write(" failed.\n")
              console.error(err)
              ecb(err)
              return
            }
            process.stdout.write(" success.\n")
            if (displayDelete)
              tree.traverseInOrder(displayNode, function(err, res){
                console.log("")
                ecb(err, res)
              })
            else
              ecb()
          })
        }
      , function(err){
          if (err) { scb(err); return }
          scb()
        })
    }
  , lastSteps = [ function(scb){
                    console.log("")
                    console.log("FINAL TREE")
                    console.log("----------")
                    tree.traverseInOrder(displayNode, function(err, res){
                      if (err) console.log("err = %j", err)
                      else     console.log("res = %j", u.flatten(res))
                      scb(null)
                    })
                  }
                , function(scb){
                    console.log("")
                    console.log("FOREACH(key, val)")
                    console.log("-----------------")
                    tree.forEach(function(k,v){
                      console.log("k = %s; v = %s;", k, v)
                    }, scb)
                  }
              ]
  , steps = u.flatten([ buildTree
                      , delTree
                      , lastSteps
                      ])

async.series(
  steps
, function(err, res){
    if (err) throw err

    //console.log("res = %j", u.flatten(res)) //results of
    //console.log("")

    if (tree.storage instanceof TrivialStore) {
      var cruft = Object.keys(tree.storage.things)
      console.log("cruft = %j", cruft)
      console.log("cruft.length = %d", cruft.length)
      console.log("tree.storage.nextId = %d", tree.storage.nextId)
    }
    if (tree.storage instanceof MemStore) {
      var cruft = Object.keys(tree.storage.buffers)
      console.log("cruft = %j", cruft)
      console.log("cruft.length = %d", cruft.length)
      console.log("tree.storage.nextId = %d", tree.storage.nextId)
    }

    console.log("Node.CNT = %d", Node.CNT)
    console.log("THE END ")
  }
)

//THE END