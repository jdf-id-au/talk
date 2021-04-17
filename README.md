# jdf/talk

**Netty** HTTP + Websockets &mdash; **Clojure** core.async + spec

Just enough of each.

Somewhat [Ring-like](https://github.com/ring-clojure/ring), but not Ring-compatible.

Exerts backpressure on both incoming and outgoing messages.

Ougoing messages are validated to spec; application/caller may choose to use provided specs for incoming messages. Routing and most HTTP protocol semantics are left to the application/caller; try jdf/foundation for this.

(Could use as sketch to update [ring-adapter-netty](https://github.com/shenfeng/async-ring-adapter/blob/master/src/ring/adapter/netty.clj).)