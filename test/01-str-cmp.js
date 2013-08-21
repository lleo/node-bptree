/* global describe it */

var assert = require('assert')
  , strCmp = require('../lib/str_cmp')

describe("strCmp - string compare funciton", function(){
  describe("comparing via string length first!", function(){
    it("\"aa\" > \"b\" => 1" , function(){
      assert.equal( strCmp("aa", "b"), 1 )
    })

    it("\"b\" < \"aa\" => -1" , function(){
      assert.equal( strCmp("b", "aa"), -1 )
    })
  })

  describe("comparing by string contents second", function(){
    it("\"aa\" < \"ab\" => -1", function(){
      assert.equal( strCmp("aa", "ab"), -1 )
    })

    it("\"ab\" > \"aa\" => 1", function(){
      assert.equal( strCmp("ab", "aa"), 1 )
    })

    it("\"aa\" == \"aa\" => 0", function(){
      assert.equal( strCmp("aa", "aa"), 0 )
    })
  })
})

//the end