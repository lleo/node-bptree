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

cmdLine
.version('0.0.1')
.option('-n|--number <n>', 'Number of Key/Val pairs', parseInt, 10)
.option('-d|--deletion [key-order]', "Order of deletion: inorder, (rev)erse, (rand)omize", keyOrder, 'inorder')
.option('-i|--insertion [key-order]', "Order of insertion: inorder, (rev)erse, (rand)omize", keyOrder, 'inorder')
.option('-D|--delay-delete <ms>', "Delay each delete (milliseconds)", -1, parseInt)
.option('--no-display-delete', "Don't display the tree after each delete")
//.option('--display-delete', "Don't display the tree after each delete")
.parse(process.argv)

console.log("number            = %d", cmdLine.number)
console.log("insertion         = %s", cmdLine.insertion)
console.log("deletion          = %s", cmdLine.deletion)
console.log("display-delete    = %j", cmdLine.displayDelete)
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

var order = 3
  //, st = new MemStore()
  //, st = new TrivialStore()
  //, tree = new BpTree(order, strCmp, st)
  , tree = new BpTree(order)
  , _key = ""
  , keys = []
  , kv = {}
  , dKeys, iKeys

for (var ki = 0; ki<cmdLine.number; ki++){
  keys.push( _key = incStr(_key) )
  kv[_key] = ki
}


iKeys = u.clone(keys)
if (cmdLine.insertion == "randomize")
  randomize(iKeys)
else if (cmdLine.insertion == "reverse")
  iKeys.reverse()

dKeys = u.clone(keys)
if (cmdLine.deletion == "randomize")
  randomize(dKeys)
else if (cmdLine.deletion == "reverse")
  dKeys.reverse()

var buildTree =  function(scb){
      console.log("Build Tree: %d tree.put() calls", keys.length)
      console.log("----------")
      async.eachSeries(
        iKeys
      , function(k, ecb){
          console.log("tree.put( %s, %d )", k, kv[k])
          tree.put(k, kv[k], ecb)
          //tree.put(iKey, i, function(err, ids){
          //  if (err) { next(err); return }
          //  tree.traverseInOrder(displayNode, next)
          //  //next(null, ids)
          //})
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
        dKeys
      , function(k, ecb){
          console.log("tree.del(%s)", k)

          if (cmdLine.delayDelete < 0) {
            tree.del(k, function(err){
              if (err) { ecb(err); return }
              if (cmdLine.displayDelete)
                tree.traverseInOrder(displayNode, function(err, res){
                  console.log("")
                  ecb(err, res)
                })
              else
                ecb()
            })
          }
          else {
            setTimeout(function(){
              tree.del(k, function(err){
                if (err) { ecb(err); return }
                if (cmdLine.displayDelete)
                  tree.traverseInOrder(displayNode, function(err, res){
                    console.log("")
                    ecb(err, res)
                  })
                else
                  ecb()
              })
            }, cmdLine.delayDelete)
          }
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
    var cruft = Object.keys(tree.storage.things)
    console.log("cruft = %j", cruft)
    console.log("cruft.length = %d", cruft.length)
    console.log("tree.storage.nextId = %d", tree.storage.nextId)

    console.log("Node.CNT = %d", Node.CNT)
    console.log("THE END ")
  }
)

//THE END