(ns user
  (:require [bidi.bidi :as bidi]
            [clojure.core.async :as async
             :refer [chan go go-loop thread >! <! >!! <!! alt! timeout]]
            [taoensso.timbre :as log]
            [clojure.pprint :refer [pprint]])
  (:import (java.util TimeZone)))

(defn pprint-middleware
  "Middleware after https://github.com/ptaoussanis/timbre/issues/184#issuecomment-397421329"
  [data]
  (binding [clojure.pprint/*print-right-margin* 100
            clojure.pprint/*print-miser-width* 50]
    (update data :vargs
      (partial mapv #(if (string? %) % (with-out-str (pprint %)))))))

(defn configure
  "Add middleware and config logging with sane defaults."
  [log-level]
  (log/merge-config! {:middleware [pprint-middleware]
                      :min-level [[#{"io.netty.*"} :info]
                                  [#{"*"} log-level]]
                      :timestamp-opts {:timezone (TimeZone/getDefault)}}))

(set! *warn-on-reflection* true)
(configure :debug)
(add-tap pprint)

#_(do
    (require '[talk.api :as talk]) ; delay netty noise until after logger configured
    (import ; bad for reloadability
      (talk.server Connection)
      (talk.http Request Attribute File Trail)
      (talk.ws Text Binary)))
    ; not working?
    ;(defmethod clojure.pprint/simple-dispatch Connection [_] prn)
    ;(defmethod clojure.pprint/simple-dispatch Request [_] prn)
    ;(defmethod clojure.pprint/simple-dispatch Attribute [_] prn)
    ;(defmethod clojure.pprint/simple-dispatch File [_] prn)
    ;(defmethod clojure.pprint/simple-dispatch Text [_] prn)
    ;(defmethod clojure.pprint/simple-dispatch Binary [_] prn))

(defn inspect [{:keys [in out]}]
  (go-loop [{:keys [channel value] :as msg} (<! in)]
    (tap> msg)
    (>! out {:ch channel :status 200 :content value})
    (when msg (recur (<! in)))))

(defprotocol Responder
  (response [this id]))
#_(extend-protocol Responder
    Connection
    (response [_ _])
    Trail
    (response [_ _])
    Attribute
    (response [this id] {:status 200 :content (:value this) :channel id})
    Request
    (response [this id] {:status 200 :headers {:content-encoding "text/plain"}
                         :content (str this) :channel id})
    Text
    (response [this _] this)
    Binary
    (response [this _] this))

#_(defn echo
    "Attempt to echo incoming."
    ; TODO exit-ch https://stackoverflow.com/a/53559455/780743
    [{:keys [in out]}]
    (go-loop [{:keys [channel] :as msg} (<! in)]
      (log/debug "APP RECEIVED" msg)
      (if-let [res (try #_(log/info "Trying to respond to" msg "with" (response msg channel))
                        (response msg channel)
                        (catch IllegalArgumentException e
                          (log/error "Forgot about" msg e)))]
        (do (log/debug "APP SENT" res)
            (when-not (>! out res)
              (log/error "failed to write")))
        (log/debug "APP DROPPED" msg))
      (when msg
        #_(log/debug "RECUR")
        (recur (<! in)))))
; TODO check toString is working properly

#_ ((:close s))
; Don't forget websocket defaults to /ws path!
#_ (def s (talk/server! 8125 {:max-content-length (* 1 1024 1024) #_(* 5 1024 1024 1024)}))
#_ (def echo-chan (echo s))
#_ (inspect s)

; Server application can internally publish `in` using topic extracted from @clients :type via <ChannelId>
; e.g. yielding {:ch <ChannelId> :method :get ...} for http
; or {:ch <ChannelId> :text "..."} for ws
; Server application can send `out`
; e.g. {:ch <ChannelId> :status 200 :headers ...} for http
; or {:ch <ChannelId> :text "..."} for ws
; Server internally publishes `out` using :ch topic.

; Can test file upload POST and PUT with (respectively):
; Will only be :file if over threshold (default 16KB)
; FIXME does seem fairly slow for huge files (adjust chunk size?)
; % curl http://localhost:8125 -v --form "fileupload=@file.pdf;filename=hmm.pdf"
; $ curl http://localhost:8125 -v -T file.pdf

#_ (def routes ["/" {"index.html" :index
                     "articles/" {"index.html" :article-index
                                  [:id "/article.html"] :article}}])
#_ (bidi/path-for routes :article :id 123)
;=> "/articles/123/article.html"
#_ (bidi/match-route routes "/articles/123/article.html")
;=> {:handler :article, :route-params {:id "123"}}
; Don't really want to support Ring or bidi.ring...
; https://github.com/juxt/bidi/blob/master/test/bidi/ring_test.clj
; Just steal ideas...