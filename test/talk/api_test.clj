(ns talk.api-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as async :refer [chan go go-loop thread >! <! >!! <!! alt! alt!!]]
            [hato.websocket :as hws]
            [hato.client :as hc]
            [talk.util :refer [ess]]
            [talk.api :as talk]
            [taoensso.timbre :as log])
  (:import
    (talk.http Connection Request Attribute File Trail)
    (talk.ws Text Binary)))

(defonce test-server (atom nil))
(defonce test-clients (atom {:http nil :ws nil}))
(def port 8124)

(defn with-server [f]
  (reset! test-server (talk/server! port {:ws-path "/ws"}))
  (f)
  ((:close @test-server)))

(defn with-clients [f]
  (swap! test-clients assoc
    :http (hc/build-http-client {})
    :ws (talk/client! (str "ws://localhost:" port "/ws"))) ; opens a new http client first
  (f)
  (hws/close! (-> @test-clients :ws :ws))
  (swap! test-clients dissoc :http :ws))

(use-fixtures :once with-server with-clients)

(defprotocol Echo
  (echo [this]))

(extend-protocol Echo
  Connection (echo [_])
  Request (echo [this] (log/debug "Responding to" (ess this))
            {:status 200 :headers {:content-encoding "text/plain"}
             :content (str this) :channel (:channel this)})
  Attribute (echo [_])
  File (echo [_])
  Trail (echo [_])
  Text (echo [this] (log/debug "Echoing text") this)
  Binary (echo [this] (log/debug "Echoing binary") this))

; TODO test
; - small & large file put/post/patch multipart form data (and urlencoded?)
; - successive such requests on kept-alive channel

(defn echo-application [{:keys [in out] :as server}]
  (go-loop [msg (<! in)]
    (if-let [res (some-> msg echo)]
      (when-not (>! out res)
        (log/error "failed to write" res "because port closed")))
    (when msg ; will be nil if `in` is closed
      (recur (<! in)))))

(deftest echo-test
  (let [{:keys [clients port path close evict] :as server} @test-server
        {:keys [http ws]} @test-clients ; opening ws client should actually connect to server
        ws-client-id (-> @clients keys first)
        _ (echo-application server)
        ;read!! #(alt!! (ws :in) ([v] v) (async/timeout 100) nil) ; hangs whole IDE when trying to view error report?!
        read!! #(<!! (ws :in)) ; just hangs REPL mid-test (still closable)
        short-text "hello"
        ; FIXME
        ; Can't actually get anywhere near (* 1024 1024); presumably protocol overhead.
        ; Interestingly binary over max-frame-size throws CorruptedWebSocketFrameException
        ; but oversized text doesn't?!
        ; Oversized text *message* throws TooLongFrameException...
        ; Something to do with client behaviour?
        long-text (apply str (repeatedly (* 512 1024) #(char (rand-int 255))))
        binary (byte-array (repeatedly (* 64 1024) #(rand-int 255)))]
    (is (contains? @clients ws-client-id)
      "Clients registry contains websocket client channel.")
    (testing "http"
      (is (= 200 (:status (hc/get (str "http://localhost:" port "/") {:http-client http})))
        "HTTP GET returns status 200.")
      (is (= 200 (:status (hc/get (str "http://localhost:" port "/hello" {:http-client http}))))
        ; FIXME doesn't seem to be on same channel
         "Second request from same client works (should be reusing channel but this isn't tested)."))
    (testing "ws"
      (is (= short-text (when (async/put! (ws :out) short-text) (read!!)))
        "Short text WS roundtrip works.")
      (is (= long-text (when (async/put! (ws :out) long-text) (read!!)))
        "Long text WS roundtrip works.")
      (is (= (seq binary) (seq (when (async/put! (ws :out) binary) (read!!))))
        "Binary WS roundtrip works."))
    (testing "clients registry"
      (is (nil? (-> ws-client-id evict deref))
        "(Evicting websocket client)")
      (is (not (contains? @clients ws-client-id))
        "Client registry no longer contains websocket client channel."))))