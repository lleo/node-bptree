/* global describe it */

var assert = require('assert')
  , BlockFileStore = require('../lib/block_file_store')
  , Handle = BlockFileStore.Handle
  , u = require('lodash')
  , block_file_fn = "test.bf"
  , fs = require('fs')

describe("BlockFileStore", function(){
  var store
    , bfs

  describe("BlockFileStore open", function(){
    it("should open a BlockFile fn ="+block_file_fn, function(done){
      BlockFileStore.open(block_file_fn, function(err, bfs){
        if (err) { done(err); return }
        store = bfs
        done()
      })
    })

  })

  describe("BlockFileStore.Handle", function(){
    var testHdl

    it("new Handle(0, 0, 0); should construct an object", function(){
      var segNum = 0
        , blkNum = 0
        , spanNum = 0

      testHdl = new Handle(segNum, blkNum, spanNum /*, default props */)
      assert.ok( testHdl instanceof Handle )
    })


    it("cur.equals(identical) should return true", function(){
      assert.ok( testHdl.equals( new Handle( testHdl.segNum
                                           , testHdl.blkNum
                                           , testHdl.spanNum
                                           /*, default props */) ) )
    })

    it("cur.equals(!identical) should return false", function(){
      assert.ok( !testHdl.equals( new Handle( testHdl.segNum
                                            , testHdl.blkNum
                                            , testHdl.spanNum+1
                                            /*, default props */) ) )
    })

    it("cur.toString() returns a string", function(){
      assert.ok( typeof testHdl.toString() === 'string' )
    })

  }) //BlockFileStore.Handle

  var leafHdl
    , branchHdl
    , l = {
      type: "Leaf"
    , order: 3
    , keys : [ "one" ]
    , children: [ 1 ]
    }
    , b = {
      type: "Branch"
    , order: 3
    , keys : [ "three" ]
    , children: [ {segNum : 0, blkNum : 3, spanNum : 0}
                , {segNum : 0, blkNum : 7, spanNum : 0} ]
    }

  describe(".store() method", function(){
    it("should store a Leaf and callback with a valid Handle", function(done){
      store.store(l, function(err, hdl){
        if (err) { done(err); return }
        leafHdl = hdl
        assert(leafHdl instanceof Handle, "leafHdl !instanceof Handle")
        done()
      })
    })
  })

  describe(".load() method", function(){
    it("should load the previous leaf Handle", function(done){
      store.load(leafHdl, function(err, o) {
        if (err) { done(err); return }
        assert.deepEqual(l, o)
        done()
      })
    })
  })

  describe(".store() method", function(){
    it("should store a Leaf and callback with a valid Handle", function(done){
      store.store(b, function(err, hdl){
        if (err) { done(err); return }
        branchHdl = hdl
        assert(branchHdl instanceof Handle, "branchHdl !instanceof Handle")
        done()
      })
    })
  })

  describe(".load() method", function(){
    it("should load the previous branch Handle", function(done){
      store.load(branchHdl, function(err, o) {
        if (err) { done(err); return }
        assert.deepEqual(b, o)
        done()
      })
    })
  })

  describe("BlockFileStore close and unlink fn="+block_file_fn, function(){
    it("should close without incident", function(done){
      store.close(done)
    })

    it("delete (unlink) the block file", function(done){
      fs.unlink(block_file_fn, done)
    })
  })
}) //BlockFileStore

//describe("", function(){
// it("", function(){
//
// })
//})
//the end