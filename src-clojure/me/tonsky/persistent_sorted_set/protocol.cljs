(ns me.tonsky.persistent-sorted-set.protocol)

(defprotocol IStorage
  (restore [address])
  (accessed [address])
  (store [node]))
