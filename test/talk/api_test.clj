(ns talk.api-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [chan go go-loop thread >! <! >!! <!! alt!]]
            [hato.websocket :as hws]
            [talk.api :as talk]
            [clojure.tools.logging :as log]))

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


(defn round-trip
  "Send message from client to server and back again."
  [msg client server]
  (log/info "about to roundtrip" (count msg) "characters")
  (<!! (go (if (>! (client :out) msg)
             (<! (go-loop [{:keys [ch text]} (<! (server :in))]
                   ; TODO probably should test connection notices too
                   (if text
                     (do (>! (server :out) {:ch ch :text text})
                         (alt! (async/timeout 1000) ::timeout
                               (client :in) ([v] v)))
                     ; drop connection/disconnection notices
                     ; clearer with `go-loop` and `if` than xformed chan, `pipe` etc
                     (recur (<! (server :in))))))
             (log/warn "already closed")))))
; FIXME *** locks up whole ide! tests pass except error on long-message
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
#_ ((:close @server))