/* global describe it */

var assert = require('assert')
  , BpTree = require('..')
  , Leaf = BpTree.Leaf
  , u = require('lodash')
  , async = require('async')
  , format = require('util').format

describe("BpTree", function(){
  var tree, order=3
    , keys = []
    , data = []
    , cur = 0
    , key = cur.toString()
    , val = cur

  describe("Constructor", function(){
    //defaults to strCmp & TrivialStore & order=3
    tree = new BpTree()

    it("should construct an object", function(){
      assert.ok(tree instanceof BpTree)
    })

  })

  describe("put/get first data", function(){

    it("should put first key/data", function(next){
      keys.push(key)
      data.push(val)
      tree.put(key, val, next)
    })

    it("should get first key/data", function(next){
      tree.get(keys[0], function(err, d){
        if (err) throw err
        assert.equal(d, data[0])
        next()
      })
    })

    it("use .findDepth to confirm the depth is 1", function(next){
      tree.findDepth(function(err, depth){
        if (err) { next(err); return }
        if (depth != 1)
          next(new Error("depth is not 1; depth="+depth))
        else next()
      })
    })

    it("put the next two k/v pairs; splitting the root leaf", function(next){
      async.series(
        [ function(scb){
            cur += 1 //cur is now = 1
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 2
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        ]
      , function(err, res) {
          if (err) { next(err); return }

          tree.findDepth(function(err, depth){
            if (err) { next(err); return }
            if (depth != 2)
              next(new Error("depth is not 2; depth="+depth))
            else next()
          })
        })
    })

    it("put a four more k/v pairs, resulting in another b-tree level", function(next){
      async.series(
        [ function(scb){
            cur += 1 //cur is now = 3
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 4
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 5
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 6
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        ]
      , function(err, res) {
          if (err) next(err)

          tree.findDepth(function(err, depth){
            if (err) { next(err); return }
            if (depth != 3)
              next(new Error("depth is not 3; depth="+depth))
            else next()
          })
        })//async.series
    })//it
  })//describe put/get first data

  describe("delete all nodes", function(){
    it("should delete all the keys", function(next){
      async.mapSeries(
        keys
        , function(key, mcb){
           tree.del(key, mcb)
         }
      , function(err, res){
          if (err) { next(err); return }
          assert.ok( u.isEqual(data, res) )
          next()
        })
    })

    it("the internal stoage should be empty", function(){
      assert.ok(Object.keys(tree.storage.things).length == 0)
    })
  })
})//BpTree

//the end