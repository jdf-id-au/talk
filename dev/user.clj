(ns user
  (:require [talk.api :as talk]
            [clojure.core.async :as async :refer [chan go go-loop thread >! <! >!! <!! alt! timeout]]
            [clojure.tools.logging :as log]))

#_ (def s (talk/server! 8125))
#_ ((:close s))

#_ (def echo (go-loop [{:keys [ch text]} (<! (s :in))]
               (when-not (boolean? text)
                 (>! (s :out) {:ch ch :text (str "heard you: " text)}))
               (recur (<! (s :in)))))


; API sketch
#_ (def s (talk/server! 8125))

; Preparing for pub/sub system:

; Server application can internally publish `in` using topic extracted from @clients :type via <ChannelId>
; e.g. yielding {:ch <ChannelId> :method :GET ...} for http
; or {:ch <ChannelId> :text "..."} for ws
; Server application can send `out`
; e.g. {:ch <ChannelId> :status 200 :headers ...} for http
; or {:ch <ChannelId> :text "..."} for ws
; Server internally publishes `out` using :ch topic.

; ws incoming
{:ch "ChannelId" :text "text"}
; ws outgoing
{:ch "ChannelId" :text "text"}
; http request
{:ch "ChannelId"
 :method :GET
 :path "/path"
 :query {"p1" ""}
 :protocol "HTTP/1.1"
 :headers {:h1 ""}
 :cookies {"c1" ""}}
; http response
{:ch "ChannelId"
 :status 200
 :headers [[:h1 ""]]
 :cookies {"c1" ""}
 :content "content"}

; Routing entirely within application (bidi I guess)