(ns user
  (:require [bidi.bidi :as bidi]
            [clojure.core.async :as async :refer [chan go go-loop thread >! <! >!! <!! alt! timeout]]
            [taoensso.timbre :as log]
            [clojure.pprint :refer [pprint]])
  (:import (java.util TimeZone)
           (talk.http Connection Attribute File Request)
           (talk.ws Text Binary)))

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

(defmethod clojure.pprint/simple-dispatch Connection [_] prn)
(defmethod clojure.pprint/simple-dispatch Request [_] prn)
(defmethod clojure.pprint/simple-dispatch Attribute [_] prn)
(defmethod clojure.pprint/simple-dispatch File [_] prn)
(defmethod clojure.pprint/simple-dispatch Text [_] prn)
(defmethod clojure.pprint/simple-dispatch Binary [_] prn)

#_ (require '[talk.api :as talk])

#_ ((:close s))
#_ (def s (talk/server! 8125 {:max-content-length (* 1 1024 1024) #_(* 5 1024 1024 1024)}))
#_ (def inspect
     (go-loop [{:keys [ch value] :as msg} (<! (s :in))]
       (tap> msg)
       (>! (s :out) {:ch ch :status 200 :content value})
       (when msg (recur (<! (s :in))))))
#_ (def echo
     (go-loop [{:keys [ch connected text method data] :as msg} (<! (s :in))]
       (log/info "successfully <! from server in" msg)
       (cond
         text
         (when-not (>! (s :out) {:ch ch :text (str "heard you: " text)})
           (log/error "failed to write to ws server out"))

         (some->> data (map :file) seq)
         (when-not (>! (s :out) {:ch ch :status 200 :content (->> data (map :file) first)})
           (log/error "failed to write file response to http server out"))

         method
         (when-not (>! (s :out) {:ch ch :status 200
                                 :headers {"Content-Encoding" "text/plain"}
                                 :content (str "message containing " data ": " msg)})
           (log/error "failed to write to http server out"))

         connected
         (log/info "connection" ch connected))
       (when msg
         (recur (<! (s :in))))))

; Status:
; *** need to get upload-handler to look at Attr... file rather than a buffer?? (or up the chain)


; Server application can internally publish `in` using topic extracted from @clients :type via <ChannelId>
; e.g. yielding {:ch <ChannelId> :method :get ...} for http
; or {:ch <ChannelId> :text "..."} for ws
; Server application can send `out`
; e.g. {:ch <ChannelId> :status 200 :headers ...} for http
; or {:ch <ChannelId> :text "..."} for ws
; Server internally publishes `out` using :ch topic.

; Can test file upload POST and PUT with (respectively):
; Will only be :file if over threshold (default 16KB)
; FIXME does seem fairly slow for huge files (what determines chunk size?)
; % curl http://localhost:8125 -v --form "fileupload=@file.pdf;filename=hmm.pdf"
; $ curl http://localhost:8125 -v -T file.pdf
; TODO:
; Routing entirely within application (bidi I guess)
; HTTP basics - some in application; could plagiarise bits of Ring
; spec all messages
; vigorous benchmarking and stress testing

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

; Do set up static file serving for convenience? Maybe just individual files?