(ns user
  (:require [talk.api :as talk]
            [clojure.core.async :as async :refer [chan go go-loop thread >! <! >!! <!! alt! timeout]]
            [clojure.tools.logging :as log]))

#_ (def s (talk/server! 8125))
#_ ((:close s))

#_ (def echo (go-loop [{:keys [ch connected text method] :as msg} (<! (s :in))]
               (log/info "successfully <! from server in" msg)
               (cond
                 text
                 (when-not (>! (s :out) {:ch ch :text (str "heard you: " text)})
                   (log/error "failed to write to ws server out"))
                 method
                 (when-not (>! (s :out) {:ch ch :status 200
                                         :headers {"Content-Encoding" "text/plain"}
                                         :content (str msg)})
                   (log/error "failed to write to http server out"))
                 connected
                 (log/info "connection" ch connected))
               (when msg
                 (recur (<! (s :in))))))

; Server application can internally publish `in` using topic extracted from @clients :type via <ChannelId>
; e.g. yielding {:ch <ChannelId> :method :GET ...} for http
; or {:ch <ChannelId> :text "..."} for ws
; Server application can send `out`
; e.g. {:ch <ChannelId> :status 200 :headers ...} for http
; or {:ch <ChannelId> :text "..."} for ws
; Server internally publishes `out` using :ch topic.

; TODO:
; Routing entirely within application (bidi I guess)
; HTTP basics - some in application?
; spec all messages
; vigorous benchmarking and stress testing