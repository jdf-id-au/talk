(ns user
  (:require [clojure.core.async :as async
             :refer [chan go go-loop thread >! <! >!! <!! alt! timeout]]
            [taoensso.timbre :as log]
            [clojure.pprint :refer [pprint]])
  (:import (java.util TimeZone)))

; Logging config

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

; REPL interactive demo

#_ (require '[talk.api :as talk])
#_ (require '[talk.ws :refer [->Text]])
#_ (def server (talk/server! 8080
                 {:ws-path "/ws"
                  :handler-timeout (* 10 1000)}))

; Use ws client to connect and send message
#_ (<!! (:in server)) ; should be http connection
#_ (<!! (:in server)) ; should be ws upgrade
#_ (<!! (:in server)) ; should be message you sent
#_ (>!! (:out server)  (->Text (:channel *1) "hello there"))
; Should appear in ws client

; Use http client to connect
#_ (<!! (:in server)) ; should be http connection
#_ (<!! (:in server)) ; should be http request
; Do this within timeout period!
#_ (>!! (:out server) {:channel (:channel *1)
                       :status 200
                       :headers {:content-type "text/plain"}
                       :content "hello"})

#_ ((:close server))

; Check behaviour of out-pub if one topic sub blocking

(defn pub-test
  "Check how one topic subscription can block others."
  [bufsize]
  (let [out (chan)
        out-pub (if (zero? bufsize)
                  (async/pub out first)
                  (async/pub out first (fn [topic] (async/buffer bufsize))))
        blocking-sub (chan)
        blocked-sub (chan)]
    (async/sub out-pub :blocking blocking-sub)
    (async/sub out-pub :blocked blocked-sub)
    ; no sub so doesn't block pub:
    (doseq [n (range 5)] (async/put! out [:ignored n]))
    ; blocking-sub blocks blocked-sub until consumed or buffered:
    (doseq [n (range 5)] (async/put! out [:blocking n]))
    (doseq [n (range 5)] (async/put! out [:blocked n]))
    ; don't consume last three put!s on blocking-sub:
    (dotimes [n 2] (async/take! blocking-sub tap>))
    ; close!ing blocking-sub here unblocks blocked-sub
    ;(async/close! blocking-sub)
    (dotimes [n 5] (async/take! blocked-sub tap>))
    blocking-sub))

#_ (pub-test 0) ; blocks because 5 pub - 0 buffer - 1 on inner chan - 2 take > 0
#_ (pub-test 1) ; blocks because 5 - 1 - 1 - 2 > 0
#_ (pub-test 2) ; doesn't block because 5 - 2 - 1 - 2 = 0 left