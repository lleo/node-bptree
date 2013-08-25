
"use strict";

exports.rand = rand
function rand(n) { return Math.random() * n } // [0,n)

exports.randInt = randInt
function randInt(n) { return Math.floor( rand(n) ) } //[0,n)

exports.randomize = randomize
function randomize(arr) {
  var i, ri, idx = []
  for (i=0; i<arr.length; i++) idx.push(i)

  function swap(a, b){
    var t = arr[a]
    arr[a] = arr[b]
    arr[b] = t
  }

  for(i=0; i<arr.length; i++) {
    ri = idx[randInt(idx.length)]
    idx.splice(ri,1)
    swap(i, ri)
  }
}
