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
   [io.netty.buffer Unpooled]))


(defn startup-message []
  (let [buf (Unpooled/buffer)]
    (.writeInt buf (int 0))
    (.writeShort buf (short 3))
    (.writeShort buf (short 0))

    (.writeBytes buf (.getBytes "database"))
    (.writeByte buf (byte 0))
    (.writeBytes buf (.getBytes "postgres"))
    (.writeByte buf (byte 0))

    (.writeBytes buf (.getBytes "user"))
    (.writeByte buf (byte 0))
    (.writeBytes buf (.getBytes "postgres"))
    (.writeByte buf (byte 0))

    (.writeByte buf (byte 0))

    (let [idx (.writerIndex buf)]
      (.markWriterIndex buf)
      (.writerIndex  buf 0)
      (.writeInt  buf idx)
      (.resetWriterIndex buf)
      buf)))

#_(spit "test/messages/startup" (.toString
                 (startup-message)
                 (Charset/forName "utf-8")))

(defn parse-auth [msg]
  (println "len"  (.readInt msg))
  (let [salt (byte-array 4)]
    (.readBytes msg 4)
    (println "salt" salt)))

(defn parse-message [msg]
  (let [tp (char (.readByte msg))
        len (.readInt msg)]
    
    (cond (= tp \R)
          (cond
            (= 12 len) (parse-auth msg)
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
        (#'parse-message msg)
        
        (finally
          (ReferenceCountUtil/release msg))))

    (channelActive [^ChannelHandlerContext ctx]
      (println "Active")
      (.write ctx (startup-message))
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


