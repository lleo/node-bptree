/* global describe it */

var assert = require('assert')
  , BpTree = require('..')
  , Key = BpTree.Key


describe("BpTree", function(){
  var tree, order=3
    , keys = []
    , data = []
    , cur = 0

  describe("Constructor", function(){
    tree = new BpTree(order)

    it("should construct an object", function(){
      assert.ok(tree instanceof BpTree)
    })

  })

  describe("put/get first data", function(){

    it("should put first key/data", function(done){
      var key = new Key.Utf8Str(cur.toString())
      keys.push(key)
      data.push(cur)
      tree.put(key, cur, function(err){
        if (err) throw err
        done()
      })
    })

    it("shout get first key/data", function(done){
      tree.get(keys[0], function(err, d){
        if (err) throw err
        assert.equal(d, data[0])
        done()
      })

    })
  })
})