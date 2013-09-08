/* global describe it */

var assert = require('assert')
  , MemStore = require('../lib/mem_store')
  , Handle = MemStore.Handle
  , u = require('lodash')

describe("MemStore nodelay", function(){
  var store

  describe("MemStore Constructor delay=-1", function(){
    it("should construct an object", function(){
      store = new MemStore(1)
      assert.ok( store instanceof MemStore )
    })
  })

  describe("MemStore.Handle", function(){
    it("new Handle(0); should construct an object", function(){
      var hdl = new Handle(0)
      assert.ok( hdl instanceof Handle )
    })

    var testHdl = new Handle(1)

    it("cur.equals(cur) should return true", function(){
      assert.ok( testHdl.equals( new Handle(1) ) )
    })
  }) //MemStore.Handle

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
    , children: [ {"id":0} , {"id":4} ]
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

}) //MemStore

//describe("", function(){
// it("", function(){
//
// })
//})
//the end