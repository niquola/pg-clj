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
   [io.netty.handler.codec ByteToMessageDecoder]
   

   [io.netty.buffer Unpooled])
  (:require [cljp.messages :as messages]))

(def utf (Charset/forName "utf-8"))

(defn parse-auth [msg]
  (println "Auth"  (.readInt msg))
  (let [salt (byte-array 4)]
    (.readBytes msg salt 0 (.readableBytes msg))
    salt))

(def MAX-SIZE (* 1000 1000 100))

(defn buf-to-str [b]
  (.toString b utf))

(defn parse-data-row [b]
  (let [cols (.readShort b)]
    ;; (println "Num columns:" cols)
    (loop [x cols]
      (when (> x 0)
        (let [len (.readInt b)
              col (.readSlice b len)]
          (println "Col " x " len:" len " buf: " (buf-to-str col)))
        (recur (dec x))))))

(defn parse-default [b]
  (println "MSG: " (buf-to-str b)))

(defn parse-row-desc [b]
  (loop [x (.readShort b)]
    (when (> x 0)
      (let [idx (.bytesBefore b (byte 0)) 
            col-name (buf-to-str (.readSlice b idx))]
        (.readByte b)
        (println "col desc: "
                 {:col-name  col-name
                  :col-num (.readShort b)
                  :col-type (.readInt b)
                  :type-size (.readShort b)
                  :type-mod (.readInt b)
                  :field-format (.readShort b)}))
      (recur (dec x)))))

(def parsers
  {(byte \D) parse-data-row
   (byte \T) parse-row-desc})

(defn parse-message [ctx msg out]
  (.markReaderIndex msg)
  (let [tp (.readByte msg)
        len (.readInt msg)
        data-len (- len 4)]
    (cond
      (or (< data-len 0) (< (.readableBytes msg) data-len)) 
      (.resetReaderIndex msg)

      (>= (.readableBytes msg) data-len) 
      (let [m (.readSlice msg data-len)]
        (println "MSG TYPE: " (char tp) " len:" data-len " bytes left: " (.readableBytes msg))
        ;;(println "  MSG: " (.toString (cast ByteBuf m) utf))
        ;; \R
        (when (and (= tp 0x52) (= 12 len))
          (let [salt (parse-auth m)]
            (println "Salt" (String. salt))
            (.write ctx (messages/md5-login {:password "pass" :user "postgres"} salt))
            (.flush ctx)))
        (.add out 
              (or 
               (when-let [parser (or (get parsers tp) parse-default)]
                 (parser m))
               (.toString (cast ByteBuf m) utf)))

        ))))


(defn client []
  (proxy [ByteToMessageDecoder] []

    (decode
      [^ChannelHandlerContext ctx ^Object msg ^Object out]
      (#'parse-message ctx msg out))

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

(defonce connections (atom nil))

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
                 (reset! connections ch)
                 (-> ch
                     (.pipeline)
                     (.addLast (into-array ChannelHandler [cl]))))))
            (.connect host port)
            (.sync)
            (.channel)
            (.closeFuture)
            (.sync)))
      (catch Exception e (println e))

      (finally (.shutdownGracefully group)))
    group))

(defn query [conn sql]
  "use context to remember columns, where to report, etc"
  (.write conn (messages/query sql))
  (.flush conn))

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


  (def c @connections)
  c

  (query c "select table_catalog, table_schema from information_schema.columns limit 1")

  (query c "select '{\"a\":1}'::jsonb")

  (query c "select 12.00::numeric")
  (query c "select '2017-01-02'::timestamptz")
  (query c "select row_to_json(x.*) from information_schema.tables x limit 10")

  )


