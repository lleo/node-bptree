/* global describe it */

var assert = require('assert')
  , TrivialStore = require('../lib/trivial_store')
  , Handle = TrivialStore.Handle
  , u = require('lodash')

describe("TrivalStore", function(){
  var store
    , testHdl = new Handle(1)
    , testJson = { id: 1 }


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

    it("cur.equals(cur) should return true", function(){
      assert.ok( testHdl.equals( new Handle(1) ) )
    })

    it("cur.toString() returns a string", function(){
      assert.ok( typeof testHdl.toString() === 'string' )
    })

  }) //TrivialStore.Handle

  describe(".handleToJson() method", function(){
    it("should convert a Handle to an JSON Object", function(){
      assert.ok( u.isEqual( store.handleToJson(testHdl), testJson ) )
    })
  })

  describe(".handleFromJson() method", function(){
    it("should convert a JSON object to a Handle", function(){
      assert.ok( testHdl.equals( store.handleFromJson(testJson) ) )
    })
  })

  var hdl
    , testLeafJson = { type     : "Leaf"
                     , order    : 3
                     , keys     : ["a", "b"]
                     , children : [1, 2]
                     }
    , testBranchJson = { type     : "Branch"
                       , order    : 3
                       , keys     : ["c"]
                       , children : [{id:1}, {id:4}]
                       }

  describe(".store() method", function(){
    it("should store the testLeafJson and callback with a valid Handle", function(done){
      store.store(testLeafJson, function(err, hdl_){
        if (err) { done(err); return }
        hdl = hdl_
        assert.ok(hdl instanceof Handle)
        done()
      })
    })
  })

  describe(".load() method", function(){
    it("should load the previous Handle with json object equiv to testLeafJson", function(done){
      store.load(hdl, function(err, res) {
        if (err) { done(err); return }
        assert.deepEqual(testLeafJson, res)
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