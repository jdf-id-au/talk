# jdf/talk

**Netty** HTTP + Websockets &mdash; **Clojure** core.async + spec

Just enough of each.

Somewhat [Ring-like](https://github.com/ring-clojure/ring), but not Ring-compatible.

Exerts backpressure on both incoming and outgoing messages.

Specs incoming and outgoing messages, but leaves some HTTP protocol semantics to the application.

(Could use as sketch to update [ring-adapter-netty](https://github.com/shenfeng/async-ring-adapter/blob/master/src/ring/adapter/netty.clj).)