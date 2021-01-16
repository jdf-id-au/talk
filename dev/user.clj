(ns user
  (:require [talk.api :as talk]
            [clojure.core.async :as async :refer [chan go go-loop thread >! <! >!! <!! alt! timeout]]
            [clojure.tools.logging :as log]))

#_ (def s (talk/server! 8125))
#_ ((:close s))

#_ (def echo (go-loop [[id msg] (<! (s :in))]
               (when-not (boolean? msg)
                 (>! (s :out) [id (str "heard you: " msg)]))
               (recur (<! (s :in)))))