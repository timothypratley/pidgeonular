(ns pidgeonular.messaging
  (:use pidgeonular.logging))

(declare new-client add-client read-messages write-messages)

(defn listen
  "Sets up a message server. The server listens on port for clients.
  When a client connects it interprets messages which are put onto a
  processing queue.
  protocol is a user supplied function which will be passed a message.
  protocol takes a message which is a map containing a key :message_id
  to dispatch processing upon.
  protocol can return :bye to end the connection"
  [port protocol]
  (let [listener (new java.net.ServerSocket port)
        running (ref true)]
    (log :info "Listening on " listener)
    (future (while true (new-client (.accept listener) protocol)))
    listener))

(defn send-message
  "Send a message to a client.
  A message is a map containing a :message-id and :words"
  [client message]
  (.put (:out client)
        (apply str (:message-id message) (:words message))))

(defn connect
  "Connect to a message server."
  [host port protocol]
  (let [s (new java.net.Socket host port)]
    (log :info "Connecting to " s)
    (.connect s)
    (new-client s)))

; Christophe Grand
;http://groups.google.com.au/group/clojure/browse_thread/thread/9f77163a9f29fe79
(defmacro while-let
  "Makes it easy to continue processing an expression as long as it is true"
  [binding & forms]
   `(loop []
      (when-let ~binding
        ~@forms
        (recur))))

(def clients (ref #{}))

(defn- new-client
  "Setup and add a new client connection"
  [socket protocol]
  (log :info "New connection " socket)
  (let [in (new java.util.concurrent.LinkedBlockingQueue)
       out (new java.util.concurrent.LinkedBlockingQueue)
       client {:in in :out out :socket socket}]
    (future (read-text-messages socket in))
    (future (write-text-messages socket out))
    (future (process-messages client protocol))
    (add-client client)))

(defn- add-client
  [client]
  (dosync (commute clients conj client)))

(defn- remove-client
  [client]
  (dosync (commute clients disj client)))

(defn- process-messages
  "Deal with messages that are ready for processing"
  [client protocol]
  (loop []
    (let [r (protocol (.take (:in client)))]
      (if r (send-message client r))
      (if (not= r :bye) (recur))))
  (log :info "Closing client " client)
  (remove-client client))

(defn- read-text-messages
  "Put incomming messages onto a queue to be processed"
  [socket queue]
  (log :info "Reader started for " socket)
  (let [input (new java.io.DataInputStream (.getInputStream socket))]
    (while-let [msg (.readLine input)]
      (log :info "Read " socket msg)
      (let [words (.split msg " ")]
        (.put queue {:message_id (first words) :words (rest words)}))))
  (.put queue nil)
  (log :info "Reader closed for " socket))

(defn- write-text-messages
  "Write outgoing messages from a queue"
  [socket queue]
  (log :info "Writer started for " socket)
  (let [output (new java.io.PrintStream (.getOutputStream socket))]
    (while-let [msg (.take queue)]
      (.println output (apply str (:message_id msg) (:words msg)))))
  (log :info "Writer closed for " socket))
