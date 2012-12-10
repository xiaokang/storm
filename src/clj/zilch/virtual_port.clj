(ns zilch.virtual-port
  (:use [clojure.contrib.def :only [defnk]])
  (:use [backtype.storm util log])
  (:require [zilch [mq :as mq]])
  (:import [java.nio ByteBuffer])
  (:import [java.util.concurrent Semaphore]))

(mq/zeromq-imports)

(defn mk-packet [virtual-port ^bytes message]
  (let [bb (ByteBuffer/allocate (+ 2 (count message)))]
    (.putShort bb (short virtual-port))
    (.put bb message)
    (.array bb)
    ))

(defn parse-packet [^bytes packet]
  (let [bb (ByteBuffer/wrap packet)
        port (.getShort bb)
        msg (byte-array (- (count packet) 2))]
    (.get bb msg)
    [port msg]
    ))

(defn virtual-url [port]
  (str "inproc://" port)) ;;xiaokang localhost in-process url

(defn- get-virtual-socket! [context mapping-atom port] ;;xiaokang mapping port -> local port connected PUSH socket
  (when-not (contains? @mapping-atom port)
    (log-message "Connecting to virtual port " port)
    (swap! mapping-atom
           assoc
           port
           (-> context (mq/socket mq/push) (mq/connect (virtual-url port)))
           ))
  (@mapping-atom port))

(defn close-virtual-sockets! [mapping-atom]
  (doseq [[_ virtual-socket] @mapping-atom]
    (.close virtual-socket))
  (reset! mapping-atom {}))

(defn virtual-send
  ([^ZMQ$Socket socket virtual-port ^bytes message flags] ;; send packet(local port + msg) to remote socket
     (mq/send socket (mk-packet virtual-port message) flags))
  ([^ZMQ$Socket socket virtual-port ^bytes message]
     (virtual-send socket virtual-port message ZMQ/NOBLOCK)))

(defnk launch-virtual-port!
  [context url :daemon true
               :kill-fn (fn [t] (System/exit 1))
               :priority Thread/NORM_PRIORITY
               :valid-ports nil] ;;xiaokang bind to url, parse received packet to [local-port msg] and send msg to in-process local-port
  (let [valid-ports (set (map short valid-ports))
        vthread (async-loop
                  (fn [^ZMQ$Socket socket virtual-mapping]
                        (let [[port msg] (parse-packet (mq/recv socket))] ;;xiaokang auto block here
                          (if (= port -1)
                            (do
                              (log-message "Virtual port " url " received shutdown notice")
                              (close-virtual-sockets! virtual-mapping)
                              (.close socket)
                              nil )
                            (if (or (nil? valid-ports) (contains? valid-ports port))
                              (let [^ZMQ$Socket virtual-socket (get-virtual-socket! context virtual-mapping port)]
                                ;; TODO: probably need to handle multi-part messages here or something
                                (mq/send virtual-socket msg)
                                0 ;;xiaokang no need to sleep since auto block on mq/recv
                                )
                              (log-message "Received invalid message directed at port " port ". Dropping...")
                              ))))
                  :args-fn (fn [] [(-> context (mq/socket mq/pull) (mq/bind url)) (atom {})]) ;;xiaokang bind to url to recive packet from other hosts
                  :daemon daemon
                  :kill-fn kill-fn
                  :priority priority)]
    (fn []
      (let [kill-socket (-> context (mq/socket mq/push) (mq/connect url))]
        (log-message "Shutting down virtual port at url: " url)
        (virtual-send kill-socket
                      -1
                      (mq/barr 1))
        (.close kill-socket)
        (log-message "Waiting for virtual port at url " url " to die")
        (.join vthread)
        (log-message "Shutdown virtual port at url: " url)
        ))))

(defn virtual-bind
  [^ZMQ$Socket socket virtual-port] ;; xiaokang: bind to local in-process port
  (mq/bind socket (virtual-url virtual-port))
  )

(defn virtual-connect
  [^ZMQ$Socket socket virtual-port] ;; xiaokang: connect to local in-process port
  (mq/connect socket (virtual-url virtual-port))
  )
