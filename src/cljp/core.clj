(ns cljp.core
  (:require [clojure.tools.logging :as log])
  (:import
   [io.netty.buffer ByteBuf]
   [io.netty.util ReferenceCountUtil]
   [io.netty.channel ChannelHandler ChannelHandlerContext ChannelInboundHandlerAdapter]
   [io.netty.bootstrap ServerBootstrap Bootstrap]
   [io.netty.channel ChannelFuture]
   [io.netty.channel ChannelInitializer]
   [io.netty.channel ChannelOption]
   [io.netty.channel EventLoopGroup]
   [io.netty.channel.nio NioEventLoopGroup]
   [io.netty.channel.socket SocketChannel]
   [io.netty.channel.socket.nio NioSocketChannel NioServerSocketChannel]
   [java.nio.charset Charset]
   [io.netty.handler.codec ByteToMessageDecoder]
   [io.netty.buffer Unpooled])
  (:require [cljp.messages :as messages]))

(defn deep-merge [a b]
  (loop [[[k v :as i] & ks] b
         acc a]
    (if (nil? i)
      acc
      (let [av (get a k)]
        (if (= v av)
          (recur ks acc)
          (recur ks (if (and (map? v) (map? av))
                      (assoc acc k (deep-merge av v))
                      (assoc acc k v))))))))

(defn client [state]
  (proxy [ByteToMessageDecoder] []
    (decode [^ChannelHandlerContext ctx
             ^Object msg
             ^Object out]
      (.markReaderIndex msg)
      (when (> (.readableBytes msg) 5)
        (let [tp (.readByte msg)
              len (.readInt msg)
              data-len (- len 4)]
          (cond
            (or (< data-len 0)
                (< (.readableBytes msg) data-len))
            (.resetReaderIndex msg)

            (>= (.readableBytes msg) data-len)
            (let [m   (.readSlice msg data-len)
                  p   (or (get messages/parsers tp) messages/parse-default)
                  res (if (and (= tp 0x52) (= 12 len))
                        (messages/parse-auth m ctx out @state)
                        (p m ctx out @state))]
              #_(println "MSG:" (char tp) " len:" len)
              (when (map? res)
                (:params (swap! state #(deep-merge % res)))))))))

    (channelActive [^ChannelHandlerContext ctx]
      (log/info "Connected")
      (.write ctx (messages/startup-message))
      (.flush ctx))

    (channelInactive [^ChannelHandlerContext ctx]
      (log/info "Disconnected"))

    (exceptionCaught
      [^ChannelHandlerContext ctx ^Throwable cause]
      (.printStackTrace cause)
      (.close ctx))))

(defn connect [group state]
  (let [b (Bootstrap.)
        cl (client state)
        connected (promise)
        opts @state
        chan (-> b
                 (.group group)
                 (.channel NioSocketChannel)
                 (.option ChannelOption/SO_KEEPALIVE true)
                 (.handler
                  (proxy [ChannelInitializer] []
                    (initChannel [^SocketChannel ch]
                      (swap! state assoc
                             :channel ch
                             :connected connected)
                      (-> ch
                          (.pipeline)
                          (.addLast (into-array ChannelHandler [cl]))))))
                 (.connect (:host opts) (:port opts))
                 (.sync))]
    (swap! state assoc
           :closeFeature (-> chan (.channel) (.closeFuture)))
    connected))



(defn query [conn sql & [opts]]
  (let [chan (:channel @conn)
        result (promise)]
    (swap! conn update
           :query
           (fn [old]
             (deep-merge
              (deep-merge (or old {}) (or opts {}))
              {:sql sql
               :rows (atom (transient []))
               :result result})))
    (.write chan (messages/query sql))
    (.flush chan)
    result))


(defn pool []
  (NioEventLoopGroup.))

(defn shutdown [pool]
  (.shutdownGracefully pool))

(comment
  (def group (NioEventLoopGroup.))

  (def cl-1 (atom {:host "localhost"
                   :port 5555
                   :user "postgres"
                   :password "pass"
                   :query {:keywordize? true}}))

  (do
    (.shutdownGracefully group)
    (def group (NioEventLoopGroup.))
    (def x (connect group cl-1)))

  (def res (query cl-1 "select * from information_schema.columns, information_schema.tables limit 100"))

  (count (:rows @res))

  @(query cl-1 "select '{\"a\":1}'::jsonb as x, 2 as y")

  @(query cl-1 "select 1 || 2")
  @(query cl-1 "select ups")
  @(query cl-1 "select 'ups', 'dups'")

  @(query cl-1 "select 12.00::numeric")

  @(query cl-1 "select '2017-01-02'::timestamptz")


  (count (:rows r))

  (time (def r @(query cl-1 "select x from generate_series(1000) x")))

  (count r)

  @(query cl-1 "select NULL as test")


  cl-1

  )

