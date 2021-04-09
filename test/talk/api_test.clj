(ns talk.api-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [chan go go-loop thread >! <! >!! <!! alt!]]
            [hato.websocket :as hws]
            [hato.client :as hc]
            [talk.api :as talk]
            [taoensso.timbre :as log]
            [talk.ws :as ws])
  (:import
    (talk.http Connection Request Attribute File Trail)
    (talk.ws Text Binary)))

(defonce test-server (atom nil))
(defonce test-clients (atom {:http nil :ws nil}))
(def port 8124)

(defn with-server [f]
  (reset! test-server (talk/server! port))
  (f)
  ((:close @test-server)))

(defn with-clients [f]
  (swap! test-clients assoc
    :http (hc/build-http-client {})
    :ws (talk/client! (str "ws://localhost:" port "/ws")))
  (f)
  (hws/close! (-> @test-clients :ws :ws))
  (swap! test-clients dissoc :http :ws))

(use-fixtures :once with-server with-clients)

(defprotocol Echo
  (echo [this]))

(extend-protocol Echo
  Connection (echo [_])
  Request (echo [this] {:status 200 :headers {:content-encoding "text/plain"}
                        :content (str this) :channel (:channel this)})
  Attribute (echo [_])
  File (echo [_])
  Trail (echo [_])
  Text (echo [this] this)
  Binary (echo [this] this))

(defn round-trip
  "Send message from client to server and back again."
  [msg client server]
  (log/info "about to roundtrip" (count msg) "characters")
  (<!! (go (if (>! (client :out) msg)
             (<! (go-loop [msg (<! (server :in))]
                   (case msg
                     ::timeout ::timeout
                     (do
                       (log/info "Server received" (str msg))
                       (if-let [res (try (echo msg)
                                         (catch IllegalArgumentException e
                                           (log/info "No echo defined" e)))]
                         (do
                           (log/info "Trying to send")
                           (do (>! (server :out) res)
                               (alt! (async/timeout 1000)
                                     (do (log/info "client receive timeout") ::timeout)
                                     (client :in) ([v] v))))
                               ; NB websocket doesn't automatically get reply if too long etc
                         (recur (alt! (async/timeout 1000)
                                  (do (log/info "server receive timeout") ::timeout)
                                  (server :in) ([v] v))))))))
             (log/warn "already closed")))))

(deftest websocket
  (let [{:keys [clients port path close evict] :as server} @test-server
        client (:ws @test-clients)
        client-id (-> @clients keys first)
        short-message "hello"
        ; Can't actually get anywhere near (* 1024 1024); presumably protocol overhead.
        ; Hangs IDE when trying, annoyingly. TODO debug
        long-message (apply str (repeatedly (* 512 1024) #(char (rand-int 255))))]
    (is (contains? @clients client-id)) ; hard to imagine this failing, just for symmetry
    (is (= short-message (round-trip short-message client server)))
    (is (= long-message (round-trip long-message client server)))
    ; Does IDE project window hang while trying to display certain (!) messages?
    (is (nil? (-> @clients keys first evict deref)))
    (is (not (contains? @clients client-id)))))
    ; TODO check for disconnection msg?

#_ (-> @test-clients :ws)
#_ (hws/close! (-> @test-clients :ws :ws))
#_ ((:close @test-server)) ; when tests crash

; TODO test
; - small & large file put/post/patch multipart form data (and urlencoded?)
; - successive such requests on kept-alive channel
; - get
; - binary ws

(defn echo-server [{:keys [in out]}]
  (go-loop [{:keys [channel] :as msg} (<! in)]
    (if-let [res (some-> msg echo)]
      (when-not (>! out res)
        (log/error "failed to write" res "because port closed")))
    (when msg
      (recur (<! in)))))

; FIXME could probably roll ws test into this echo server?
(deftest http
  (let [{:keys [clients port path close evict] :as server} @test-server
        c (:http @test-clients)
        echo-chan (echo-server server)]
    (is (= 200 (:status (hc/get (str "http://localhost:" port "/") #_{:http-client c}))))))