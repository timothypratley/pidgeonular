(ns pidgeonular.client
  (:use pidgeonular.messaging pidgeonular.logging))

(def world (ref {}))

(defn dissoc-in
  [m [k & ks]]
  (if ks
    (dissoc m k (dissoc-in (get m k) ks))
    (dissoc m k)))

(defmulti state-client-protocol :id)
(defmethod state-client-protocol :default
  [message connection]
  (log :info "CLIENT Bad message received: " message " from "
       (get-connection-name connection))
  :bye)
(defmethod state-client-protocol :replace
  [message connection]
  (dosync (alter world assoc-in (:keys message) (:value message)))
  nil)
(defmethod state-client-protocol :remove
  [message connection]
  (dosync (alter world dissoc-in (:keys message)))
  nil)
(defmethod state-client-protocol :update
  [message connection]
  (dosync
    (doseq [k (keys (:value message))]
      (alter world assoc-in (conj (:keys message) k) ((:value message) k))))
  nil)


(def c (ref nil))

(dosync ref-set c (connect "localhost" 8888 state-client-protocol))
(send-message @c {:id :login
                  :name "foo"})
(send-message @c {:id :replace
                  :keys [:region]
                  :value "foo"})
