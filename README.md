
BpTree.keyType == Utf8Str | UInt32BE
BpTree.order   == number
BpTree.rootId  == Handle
BpTree.cache   == { Handle.value :  Node | Leaf

Node.id       == Handle
Node.parentId == Handle
Node.order    == number //BpTree.order
Node.keys     == []     //length <= Node.order
Node.chld     == []     //length == keys.length+1

Leaf.id       == Handle
Leaf.parentId == Handle
Leaf.order    == number //BpTree.order
Leaf.keys     == []     //length <= Leaf.order
Leaf.data     == []     //length == keys.length

Rules:
1) Split Leaf|Node when .keys.length > order
2) 

BpTree.order = 3
BpTree.root  = new Leaf()
--------------------------
tree.add(kA, vA)
L0:[ kA ]
   [ vA ]

tree.add(kB, vB)
L0:[ kA, kB ]
   [ vA, vB ]

tree.add(kC, vC)
L0:[ kA, kB, kC ]
   [ vA, vB, vC ]

tree.add(kD, vD)
L0:[ kA, kB, kC, kD ]
   [ vA, vB, vC, vD ]
L1 = L0.split()
tree.root = new Node([kC], [L0, L1]) //aka N0
N0:[ kC ]
   [ L0, L1 ]
L0:[ kA, kB ] L1:[ kC, kD ]
   [ vA, vB ]    [ vC, vD ]

tree.add(kE, vE)
N0:[ kC ]
   [ L0, L1 ]
L0:[ kA, kB ] L1:[ kC, kD, kE ]
   [ vA, vB ]    [ vC, vD, vE ]

tree.add(kF, vF)
N0:[ kC ]
   [ L0, L1 ]
L0:[ kA, kB ] L1:[ kC, kD, kE, kF ]
   [ vA, vB ]    [ vC, vD, vE, vF ]
L1.split()
N0:[ kC ]
   [ L0, L1 ]
L0:[ kA, kB ] L1:[ kC, kD ] L2:[ kE, kF ] 
   [ vA, vB ]    [ vC, vD ]    [ vE, vF ]
N0.add(kE, L2)
N0:[ kC, kE ]
   [ L0, L1, L2 ]
L0:[ kA, kB ] L1:[ kC, kD ] L2:[ kE, kF ] 
   [ vA, vB ]    [ vC, vD ]    [ vE, vF ]

tree.add(kG, vG)
N0:[ kC, kE ]
   [ L0, L1, L2 ]
L0:[ kA, kB ] L1:[ kC, kD ] L2:[ kE, kF, kG ] 
   [ vA, vB ]    [ vC, vD ]    [ vE, vF, kG ]

tree.add(kH, vH)
N0:[ kC, kE ]
   [ L0, L1, L2 ]
L0:[ kA, kB ] L1:[ kC, kD ] L2:[ kE, kF, kG, kH ] 
   [ vA, vB ]    [ vC, vD ]    [ vE, vF, vG, vH ]

tree.root = N0
N0:[ kC, kE, kG ]
   [ L0, L1, L2, L3 ]
L0:[ kA, kB ] L1:[ kC, kD ] L2:[ kE, kF ] L3:[ kG, kH, kI ]
   [ vA, vB ]    [ vC, vD ]    [ vE, vF ]    [ vG, vH, vI ]
tree.add(kJ, vJ)
N0:[ kC, kE, kG, kI ]
   [ L0, L1, L2, L3, L4 ]
L0:[ kA, kB ] L1:[ kC, kD ] L2:[ kE, kF ] L3:[ kG, kH ] L4:[ kI, kJ ]
   [ vA, vB ]    [ vC, vD ]    [ vE, vF ]    [ vG, vH ]	   [ vI, vJ ]
N1 = N0.split() //cuz N0.keys.length==4 > order==3
tree.root = new Node(N0, N1) //aka N3
N3:[ kG ]
   [ N0, N1 ]
N0:[ kC, kE ]     N1:[ kI ]
   [ L0, L1, L2 ]    [ L3, L4 ]
L0:[ kA, kB ] L1:[ kC, kD ] L2:[ kE, kF ] L3:[ kG, kH ] L4:[ kI, kJ ]
   [ vA, vB ]    [ vC, vD ]    [ vE, vF ]    [ vG, vH ]	   [ vI, vJ ]
...
N3:

N0:[ kC, kE ]     N1:[ kI kK ]      N2:[ kO kQ ]
   [ L0, L1, L2 ]    [ L3, L4, L5 ]    [ L6 L7 L8 ]
L0:AB L1:CD L2:EF L3:GH L4:IJ L5:KL L6:MN L7:OP L8: QR

Scenario#1
Init:
N:[ kB, kC, kD ]
  [ L1, L2, L3, L4]
N.add(kA, L0) //let kA = L0.keys[0]
we know kB > L1.keys[last] and kA > L0.keys[last] and kB = L2.keys[0]
if kA < KB then
  if kA < L1.keys[0] then
    let k = L1.keys[0]
    N.keys.unshift(k)
    n.children.unshift(L0)

input := RQPONMLKJIHGFEDCBA
tree.add(kR, vR)
tree.add(kQ, vQ)
tree.add(kP, vP)
root := L0
L0: [ kP, kQ, kR ]
    [ vP, vQ, vR ]

tree.add(kO, vO)
L0:[ kO, kP, kQ, kR ]
   [ vO, vP, vQ, vR ]
L1 = L0.split()
L0:[ kO, kP ] L1:[ kQ, kR ]
   [ vO, vP ]    [ vQ, vR ]
tree.root = new Node([kQ], [L0, L1]) //aka N0
N0:[ kQ ]
   [ L0, L1 ]
L0:[ kO, kP ] L1:[ kQ, kR ]
   [ vO, vP ]    [ vQ, vR ]

tree.add(kN, vN)
tree.add(kM, vM)
N0:[ kQ ]
   [ L0, L1 ]
L0:[ kM, kN, kO, kP ] L1:[ kQ, kR ]
   [ vM, vN, vO, vP ]    [ vQ, vR ]
L2 = L0.split()
N0:[ kQ ]
   [ L0, L1 ]
L0:[ kM, kN ] L2:[ kO, kP ] L1:[ kQ, kR ]
   [ vM, vN ] 	 [ vO, vP ]    [ vQ, vR ]
N0.add(kO, L2)
N0:[ kO, kQ ]
   [ L0, L2, L1 ]
L0:[ kM, kN ] L2:[ kO, kP ] L1:[ kQ, kR ]
   [ vM, vN ] 	 [ vO, vP ]    [ vQ, vR ]

//