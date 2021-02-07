(ns talk.api-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [chan go go-loop thread >! <! >!! <!! alt!]]
            [hato.websocket :as hws]
            [talk.api :as talk]
            [taoensso.timbre :as log]
            [talk.ws :as ws])
  (:import (talk.ws Text Binary)))

(defonce server (atom nil))
(defonce client (atom nil))
(def port 8124)

(defn with-server [f]
  (reset! server (talk/server! port))
  (f)
  ((:close @server)))

(defn with-client [f]
  (reset! client (talk/client! (str "ws://localhost:" port "/ws")))
  (f)
  (hws/close! (@client :ws)))

(use-fixtures :once with-server with-client)

(defprotocol Echo
  (echo [this]))

(extend-protocol Echo
  Text (echo [{:keys [channel data]}] {:ch channel :text data})
  Binary (echo [{:keys [channel data]}] {:ch channel :data data}))

(defn round-trip
  "Send message from client to server and back again."
  [msg client server]
  (log/info "about to roundtrip" (count msg) "characters")
  (<!! (go (if (>! (client :out) msg)
             (<! (go-loop [{:keys [data] :as msg} (<! (server :in))]
                   (log/info "Server received" (str msg))
                   ; TODO probably should test connection notices too
                   (if data ; clumsy way of restricting types
                     (do
                       (log/info "Trying to send" (str msg))
                       (do (>! (server :out) (echo msg))
                           (alt! (async/timeout 1000) (do (log/info "timeout") ::timeout)
                                 (client :in) ([v] v))))
                       ; drop connection/disconnection notices
                       ; clearer with `go-loop` and `if` than xformed chan, `pipe` etc
                     (recur (<! (server :in))))))
             (log/warn "already closed")))))

(deftest messages
  (let [{:keys [clients port path close evict] :as server} @server
        client @client
        client-id (-> @clients keys first)
        short-message "hello"
        ; Can't actually get very near (* 1024 1024); presumably protocol overhead.
        ; Hangs IDE when trying, annoyingly. TODO debug
        long-message (apply str (repeatedly (* 512 1024) #(char (rand-int 255))))]
    (is (contains? @clients client-id)) ; hard to imagine this failing, just for symmetry
    (is (= short-message (round-trip short-message client server)))
    (is (= long-message (round-trip long-message client server)))
    (is (nil? (-> @clients keys first evict deref)))
    (is (not (contains? @clients client-id)))))
    ; TODO check for disconnection msg?

#_ (@client :ws)
#_ (hws/close! (@client :ws))
#_ ((:close @server)) ; when tests crash

; TODO test
; - small & large file put/post/patch multipart form data (and urlencoded?)
; - get
; - binary ws