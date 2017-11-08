(ns cljp.netty
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
   [io.netty.buffer Unpooled])
  (:require [cljp.messages :as messages]))



(defn parse-auth [msg]
  (println "Auth"  (.readInt msg))
  (let [salt (byte-array 4)]
    (.readBytes msg salt 0 (.readableBytes msg))
    salt))

(defn parse-message [ctx msg]
  (let [tp (char (.readByte msg))
        len (.readInt msg)]
    
    (cond (= tp \R)
          (cond
            (= 12 len) (let [salt (parse-auth msg)]
                         (println "Salt" (String. salt))
                         (.write ctx (messages/md5-login {:password "pass" :user "postgres"} salt))
                         (.flush ctx))

            :else (println "MSG:::"
                           "[type:" tp "] "
                           "[length:" len "]"
                           (.toString (cast ByteBuf msg)
                                      (Charset/forName "utf-8"))))
          :else (println "MSG:::"
                         "[type:" tp "] "
                         "[length:" len "]"
                         (.toString (cast ByteBuf msg)
                                    (Charset/forName "utf-8"))))))

(defn client []
  (proxy [ChannelInboundHandlerAdapter] []

    (channelRead
      [^ChannelHandlerContext ctx ^Object msg]
      (try
        (#'parse-message ctx msg)
        (finally
          (ReferenceCountUtil/release msg))))

    (channelActive [^ChannelHandlerContext ctx]
      (println "Active")
      (.write ctx (messages/startup-message))
      (.flush ctx))

    (channelInactive [^ChannelHandlerContext ctx]
      (println "InActive"))

    (exceptionCaught
      [^ChannelHandlerContext ctx ^Throwable cause]
      (.printStackTrace cause)
      (.close ctx))))

(defn connect []
  (let [host "localhost"
        port (int 5555)
        group (NioEventLoopGroup.)]
    (try
      (let [b (Bootstrap.)
            cl (client)]
        (-> b
            (.group group)
            (.channel NioSocketChannel)
            (.option ChannelOption/SO_KEEPALIVE true)
            (.handler
             (proxy [ChannelInitializer] []
               (initChannel [^SocketChannel ch]
                 (-> ch
                     (.pipeline)
                     (.addLast
                      (into-array ChannelHandler [cl]))))))
            (.connect host port)
            (.sync)
            (.channel)
            (.closeFuture)
            (.sync)))
      (catch Exception e
        (println e))

      (finally
        (.shutdownGracefully group)))
    group))

(defonce cl (atom nil))

@cl

(defn restart []
  (let [t (Thread. connect)]
    (reset! cl t)
    (.start t)
    (.isAlive t)
    t))

(comment

  (restart)

  (.interrupt @cl)

  cl


  )


