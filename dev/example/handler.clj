(ns example.handler
  (:require [talk.api :as talk]
            [bidi.bidi :as bidi]
            [clojure.core.async :as async :refer [chan go go-loop thread >! <! >!! <!! alt! timeout]]))

(def routes ["/" [["ws" :web-socket]
                  ["" :index]
                  []]])
; TODO