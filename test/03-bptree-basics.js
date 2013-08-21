/* global describe it */

var assert = require('assert')
  , BpTree = require('..')
  , Leaf = BpTree.Leaf
  , TrivialStore = require('../lib/trivial_store')
  , strCmp = require('../lib/str_cmp')
  , async = require('async')

describe("BpTree", function(){
  var tree, order=3
    , keys = []
    , data = []
    , cur = 0
    , key = cur.toString()
    , val = cur
    , trivialStore = new TrivialStore()

  describe("Constructor", function(){
    tree = new BpTree(order, trivialStore, strCmp)

    it("should construct an object", function(){
      assert.ok(tree instanceof BpTree)
    })

  })

  describe("put/get first data", function(){

    it("should put first key/data", function(next){
      keys.push(key)
      data.push(val)
      tree.put(key, val, function(err){
        if (err) throw err
        next()
      })
    })

    it("should get first key/data", function(next){
      tree.get(keys[0], function(err, d){
        if (err) throw err
        assert.equal(d, data[0])
        next()
      })
    })

    it("use .findLeaf to find the leaf and path", function(next){
      tree.findLeaf('0', function(err, leaf, path){
        if (err) { next(err); return }
        if ( path.length != 0 )
          next(new Error("path not zero"))
        else if ( !(leaf instanceof Leaf) )
          next(new Error("leaf not a Leaf"))
        else
          next()
      })
    })

    it("put the next two k/v pairs; splitting the root leaf", function(next){
      async.series(
        [ function(scb){
            cur += 1 //cur is now = 1
            key = cur.toString()
            val = cur

            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 2
            key = cur.toString()
            val = cur

            tree.put(key, val, scb)
          }
        ]
      , function(err, res) {
          if (err) { next(err); return }

          //use the last key/val pair
          tree.findLeaf(key, function(err, leaf, path){
            if (path.length != 1)
              next(new Error("path is not 1"))
            else
              next()
          })
        })
    })

    it("put a four more k/v pairs, resulting in another b-tree level", function(next){
      async.series(
        [ function(scb){
            cur += 1 //cur is now = 3
            key = cur.toString()
            val = cur

            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 4
            key = cur.toString()
            val = cur

            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 5
            key = cur.toString()
            val = cur

            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 6
            key = cur.toString()
            val = cur

            tree.put(key, val, scb)
          }
        ]
      , function(err, res) {
          if (err) next(err)

          //use the last key/val pair
          tree.findLeaf(key, function(err, leaf, path){
            if (path.length != 2)
              next(new Error("path is not 2"))
            else
              next()
          })
        })//async.series
    })//it
  })
})

//the end