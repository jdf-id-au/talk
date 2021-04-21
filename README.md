# jdf/talk

**Netty** HTTP + Websockets &mdash; **Clojure** core.async + spec

Just enough of each.

Exerts backpressure on both incoming and outgoing messages.

Ougoing messages are validated to spec; application/caller may choose to use provided specs for incoming messages. Routing and most HTTP protocol semantics are left to the application/caller; try [jdf/foundation](https://github.com/jdf-id-au/foundation/tree/ws-only) for this.

Streams uploads to disk above threshold size.

(Could use as sketch to update [ring-adapter-netty](https://github.com/shenfeng/async-ring-adapter/blob/master/src/ring/adapter/netty.clj).)

## Usage

```clojure
(require '[clojure.core.async :refer [<!! >!!])
(require '[talk.api :as talk])
(require '[talk.ws :refer [->Text]])

(def server (talk/server! 8080 {:ws-path "/ws" :handler-timeout (* 10 1000)}))

; Use websocket client to visit ws://localhost:8080/ws
; Send a message

(<!! (:in server))
;=> talk.http.Connection record representing http connection
(<!! (:in server))
;=> talk.http.Connection record representing ws upgrade
(<!! (:in server))
;=> talk.ws.Text record representing message
(>!! (:out server) (->Text (:channel *1) "hello"))
; Message appears in ws client

; Use browser to visit http://localhost:8080/hello
; It will time out unless you're quick with the next steps!

(<!! (:in server))
;=> talk.http.Connection record representing http connection
(<!! (:in server))
;=> talk.http.Request record representing request

; Exercise for reader to set up proper async reply
; This one needs to be sent within timeout period!
(>!! (:out server)
  {:channel (:channel *1)
   :status 200
   :headers {:content-type "text/plain"}
   :content "hello"
  })

((:close server))
```