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
#_ ((:close @test-server)) ; if breaks while testing

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
  Connection (echo [this] #_(log/debug (ess this)))
  Request (echo [{:keys [channel method] :as this}]
            (case method
              (:post :put :patch)
              {:status 102 :channel channel} ; i.e. approve upload
              {:status 200 :headers {:content-type "text/plain; charset=utf-8"}
               :content (str this) :channel channel}))
  Attribute (echo [this] (log/debug "Received" (str this)))
  File (echo [this] (log/debug "Received" (str this)))
  Trail (echo [{:keys [channel] :as this}] (log/debug "Received" (str this))
          {:status 200 :channel channel :headers {:content-type "text/plain; charset=utf-8"}})
  Text (echo [this] (log/debug "Echoing" (str this)) this)
  Binary (echo [this] (log/debug "Echoing" (str this)) this))

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
        [ws-id ws-ch] (first clients)
        _ (echo-application server)
        ; hangs whole IDE when trying to view error report?!
        #_#_read!! #(alt!! (ws :in) ([v] v) (async/timeout 100) nil)
        read!! #(<!! (ws :in)) ; just hangs REPL mid-test (still closable)
        short-text "hello"
        ; FIXME
        ; Can't actually get anywhere near (* 1024 1024); presumably protocol overhead.
        ; Interestingly binary over max-frame-size throws CorruptedWebSocketFrameException
        ; but oversized text doesn't?!
        long-text (apply str (repeatedly (* 512 1024) #(char (rand-int 255))))
        binary (byte-array (repeatedly (* 64 1024) #(rand-int 255)))]
    (is (contains? clients ws-id)
      "Clients registry contains websocket client channel.")
    (testing "http"
      (is (= 200 (:status (hc/get (str "http://localhost:" port "/") {:http-client http})))
        "HTTP GET returns status 200.")
      (is (= 200 (:status (hc/get (str "http://localhost:" port "/hello") {:http-client http})))
        "Second request from same client works (should be reusing channel but this isn't tested).")
      (is (= 200 (:status (hc/post (str "http://localhost:" port "/post-form-urlencoded")
                            {:http-client http :throw-exceptions? false
                             :form-params {:field1 "val1" :field2 "val2"}})))
        "Simple form request works."))
    (testing "angry http"
      (is (= 413 (:status (hc/post (str "http://localhost:" port "/post-massive-form-urlencoded")
                            {:http-client http :throw-exceptions? false
                             :form-params {:bigfield1 long-text :bigfield2 binary}})))
        "Simple form request with two big fields is too big!")
      ; FIXME not writing attrib to disk!
      ; Think need multipart to do that? Or misleading content-length?
      (is (= 200 (:status (hc/post (str "http://localhost:" port "/post-big-form-urlencoded")
                            {:http-client http :throw-exceptions? false
                             :form-params {:bigfield binary}})))
        "Simple form request with one big field works.")
      (is (= 200 (:status (hc/post (str "http://localhost:" port "/post-multipart")
                            {:http-client http :throw-exceptions? false
                             :multipart [{:name "multipart1" :content "boring text"}
                                         {:name "multipart2" :content binary
                                          :content-type :octet-stream}]})))
        "Multipart form incl binary attribute works.")
      (is (= 200 (:status (hc/post (str "http://localhost:" port "/post-json")
                             {:http-client http :throw-exceptions? false
                              :content-type :json
                              :form-params {:json1 12345 :json2 ["ugh" "blah"]}})))
        "Simple post with json works.")
      (is (= 200 (:status (hc/post (str "http://localhost:" port "/post-transit")
                            {:http-client http :throw-exceptions? false
                             :content-type :transit+json
                             :form-params {:transit1 12345 :transit2 ["ugh" "blah"]}})))
        "Simple post with transit works.")
      (is (= 200 (:status (hc/post (str "http://localhost:" port "/post-binary")
                            {:http-client http :throw-exceptions? false
                             :content-type :octet-stream
                             :body binary})))
        "Simple post with binary works.")
      (is (= 200 (:status (hc/put (str "http://localhost:" port "/put-binary")
                            {:http-client http :throw-exceptions? false
                             :content-type :octet-stream
                             :body binary})))
        "Put with binary works.")
      (is (= 200 (:status (hc/patch (str "http://localhost:" port "/patch-binary")
                            {:http-client http :throw-exceptions? false
                             :content-type :octet-stream
                             :body binary})))
        "Patch with binary works."))
      ; TODO test CORS
    (testing "ws"
      (is (= short-text (when (async/put! (ws :out) short-text) (read!!)))
        "Short text WS roundtrip works.")
      (is (= long-text (when (async/put! (ws :out) long-text) (read!!)))
        "Long text WS roundtrip works.")
      ; FIXME Java 11 WS client doesn't fragment large outgoing binary messages
      ; Strangely large text works.
      (is (= (seq binary) (seq (when (async/put! (ws :out) binary) (read!!))))
        "Binary WS roundtrip works."))
    (testing "clients registry"
      (is (nil? (-> ws-id evict deref))
        "(Evicting websocket client)")
      (is (not (contains? clients ws-id))
        "Client registry no longer contains websocket client channel."))))