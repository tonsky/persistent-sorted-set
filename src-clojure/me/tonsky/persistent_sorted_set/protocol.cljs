(ns me.tonsky.persistent-sorted-set.protocol)

(defprotocol IStorage
  (restore [this address])
  (accessed [this address])
  (store [this node address])
  (delete [this unused-addresses]))
