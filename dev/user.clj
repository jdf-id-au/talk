(ns user
  (:require [talk.api :as talk]
            [bidi.bidi :as bidi]
            [clojure.core.async :as async :refer [chan go go-loop thread >! <! >!! <!! alt! timeout]]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pprint]))

#_ ((:close s))
#_ (def s (talk/server! 8125 {:max-content-length (* 5 1024 1024 1024)}))
#_ (def inspect
     (go-loop [{:keys [ch connected text method data] :as msg} (<! (s :in))]
       (tap> ["received" msg])
       (when-let [[{:keys [file]}] data]
         (when file (tap> ["file length" (.length file)])))
       (>! (s :out) {:ch ch :status 200 :content (some-> data first :file)})
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
; http requests with body < threshold (therefore going to memory) seem fine
; larger ones (therefore going to file) seem to come in but not get stored properly (or deleted too soon?)
; *** need to get upload-handler to look at Attr... file rather than a buffer?? (or up the chain)

(add-tap pprint/pprint)

; Server application can internally publish `in` using topic extracted from @clients :type via <ChannelId>
; e.g. yielding {:ch <ChannelId> :method :get ...} for http
; or {:ch <ChannelId> :text "..."} for ws
; Server application can send `out`
; e.g. {:ch <ChannelId> :status 200 :headers ...} for http
; or {:ch <ChannelId> :text "..."} for ws
; Server internally publishes `out` using :ch topic.

; Can test file upload POST and PUT with (respectively):
; Will only be :file if over threshold (default 16KB)
; % curl -o POST.pdf --form "fileupload=@file.pdf;filename=hmm.pdf" http://localhost:8125
; $ curl -o PUT.pdf -v -T file.pdf http://localhost:8125
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