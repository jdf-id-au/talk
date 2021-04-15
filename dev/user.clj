(ns user
  (:require [clojure.core.async :as async
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

; Can test file upload POST and PUT with (respectively, after setting up echo server):
; Will only be :file? if over threshold (default 16KB)
; FIXME does seem fairly slow for huge files (adjust chunk size?)
; % curl http://localhost:8125/path -v --form "fileupload=@file.pdf;filename=hmm.pdf"
; $ curl http://localhost:8125/path -v -T file.pdf