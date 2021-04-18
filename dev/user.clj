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