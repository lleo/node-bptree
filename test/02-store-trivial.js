/* global describe it */

var assert = require('assert')
  , TrivialStore = require('../lib/trivial_store')
  , Handle = TrivialStore.Handle

describe("TrivalStore", function(){
  var store

  describe("TrivalStore Constructor", function(){
    it("should construct an object", function(){
      store = new TrivialStore()
      assert.ok( store instanceof TrivialStore )
    })
  })

  describe("TrivailStore.Handle", function(){
    describe("Constructor", function(){
      it("should construct an object", function(){
        var hdl = new Handle(0)
        assert.ok( hdl instanceof Handle )
      })
    })

    var testHdl = new Handle(1)

    it("cur.cmp(next) should return -1", function(){
      assert.equal( testHdl.cmp( new Handle(2) ), -1 )
    })

    it("cur.cmp(cur) should return 0", function(){
      assert.equal( testHdl.cmp( new Handle(1) ), 0 )
    })

    it("cur.cmp(prev) should return 1", function(){
      assert.equal( testHdl.cmp( new Handle(0) ), 1 )
    })

  }) //TrivialStore.Handle

  var hdl
    , testObj = {
      type: "Leaf"
    , order: 3
    , keys: ["a", "b"]
    , children: [1, 2]
    }

  describe(".store() method", function(){
    it("should store the testObj and callback with a valid Handle", function(done){
      store.store(testObj, function(err, hdl_){
        if (err) { done(err); return }
        hdl = hdl_
        assert.ok(hdl instanceof Handle)
        done()
      })
    })
  })

  describe(".load() method", function(){
    it("should load the previous Handle with json object equiv to testObj", function(done){
      store.load(hdl, function(err, res) {
        if (err) { done(err); return }
        assert.deepEqual(testObj, res)
        done()
      })
    })
  })

}) //TrivialStore

//describe("", function(){
// it("", function(){
//
// })
//})
//the end