(ns cljp.netty
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

(def utf (Charset/forName "utf-8"))

(defn parse-md5-auth [msg]
  (log/info "Auth"  (.readInt msg))
  (let [salt (byte-array 4)]
    (.readBytes msg salt 0 (.readableBytes msg))
    salt))

(def MAX-SIZE (* 1000 1000 100))

(defn buf-to-str [b]
  (.toString b utf))

(defn read-cstring [b]
  (let [s (->> (.bytesBefore b (byte 0))
               (.readSlice b)
               (buf-to-str))]
    (.readByte b)
    s))

(defn parse-row-desc [b ctx out opts]
  {:query
   {:columns
    (loop [x (.readShort b)
           acc []]
      (if (> x 0)
        (->> {:col-name         (read-cstring b)
              :object-id        (.readInt b)
              :attribute-number (.readShort b)
              :type-object-id   (.readInt b)
              :type-size        (.readShort b)
              :type-mod         (.readInt b)
              :format-code      (.readShort b)}
             (conj acc)
             (recur (dec x)))
        acc))}})

(defn parse-data-row [b ctx out {{col-defs :columns} :query :as opts}]
  (let [cols-num (.readShort b)
        row (loop [x cols-num
                   res {}]
              (if (> x 0)
                (let [len     (.readInt b)
                      col     (when (> len 0) (.readSlice b len))
                      col-idx (- cols-num x)
                      col-def (nth col-defs col-idx nil)]
                  (recur (dec x) (assoc res (:col-name col-def) (when col (buf-to-str col)))))
                res))]
    (println "ROW:" row)
    nil))

(defn parse-default [b ctx out opts]
  (log/info "MSG: " (buf-to-str b)))

(defn parse-auth [m ctx out opts]
  (let [salt (parse-md5-auth m)]
    (.write ctx (messages/md5-login {:password (:password opts) :user (:user opts)} salt))
    (.flush ctx)))

(defn parse-ready-for-query [m ctx out opts]

  ;; Byte1('Z')
  ;; Identifies the message type. ReadyForQuery is sent whenever the backend is ready for a new query cycle.

  ;; Int32(5)
  ;; Length of message contents in bytes, including self.

  ;; Byte1
  ;; Current backend transaction status indicator. Possible values are 'I' if
  ;; idle (not in a transaction block); 'T' if in a transaction block; or 'E' if
  ;; in a failed transaction block (queries will be rejected until block is
  ;; ended).

  (println 
   "Status: "
   {:status (get {\I :idle
                  \T :transaction
                  \E :failed-tx}
                 (char (.readByte m)))})
  nil)

(defn  parse-parameter-status [m ctx out opts]
  (parse-default m ctx out opts))

(defn parse-query [m ctx out opts]
  (let [query (read-cstring m)]
    (println "QUERY: " query))
  nil)

(defn parse-close [m ctx out opts]
  ;; Byte1('C')
  ;; Identifies the message as a Close command.

  ;; Int32
  ;; Length of message contents in bytes, including self.

  ;; Byte1
  ;; 'S' to close a prepared statement; or 'P' to close a portal.

  ;; String
  ;; The name of the prepared statement or portal to close (an empty string selects the unnamed prepared statement or portal).
  (println "Close"
           {;;:type           (char (.readByte m))
            :statement-name (read-cstring m)})
  nil)

(def parsers
  {(byte \D) parse-data-row
   (byte \T) parse-row-desc
   (byte \Z) parse-ready-for-query
   (byte \S) parse-parameter-status
   (byte \Q) parse-query
   (byte \C) parse-close
   })

(defn client [opts]
  (let [state (atom opts)]
    (proxy [ByteToMessageDecoder] []
      (decode [^ChannelHandlerContext ctx
               ^Object msg
               ^Object out]
        (.markReaderIndex msg)
        (let [tp (.readByte msg)
              len (.readInt msg)
              data-len (- len 4)]
          (cond
            (or (< data-len 0) (< (.readableBytes msg) data-len)) 
            (.resetReaderIndex msg)

            (>= (.readableBytes msg) data-len)
            (let [m   (.readSlice msg data-len)
                  p   (or (get parsers tp) parse-default)
                  res (if (and (= tp 0x52) (= 12 len))
                        (parse-auth m ctx out @state)
                        (p m ctx out @state))]
              (when (map? res)
                (println "state changed" res)
                (swap! state merge res))
                (log/info "MSG TYPE: " (char tp) " len:" data-len " bytes left: " (.readableBytes msg))))))

      (channelActive [^ChannelHandlerContext ctx]
        (log/info "Active")
        (.write ctx (messages/startup-message))
        (.flush ctx))

      (channelInactive [^ChannelHandlerContext ctx]
        (log/info "InActive"))

      (exceptionCaught
        [^ChannelHandlerContext ctx ^Throwable cause]
        (.printStackTrace cause)
        (.close ctx)))))

(defn connect [group {host :host port :port :as opts}]
  (let [b (Bootstrap.)
        cl (client opts)
        ;; todo use future to wait till connection
        conn (atom nil)
        chan (-> b
                 (.group group)
                 (.channel NioSocketChannel)
                 (.option ChannelOption/SO_KEEPALIVE true)
                 (.handler
                  (proxy [ChannelInitializer] []
                    (initChannel [^SocketChannel ch]
                      (reset! conn ch)
                      (-> ch
                          (.pipeline)
                          (.addLast (into-array ChannelHandler [cl]))))))
                 (.connect host port)
                 (.sync))]
    {:channel      @conn
     :opts         opts
     :closeFeature (-> chan (.channel) (.closeFuture))}))

(defn query [{conn :channel} sql]
  (.write conn (messages/query sql))
  (.flush conn))

(comment

  (def opts {:host "localhost" :port 5555 :user "postgres" :password "pass"})


  cl-1

  (query cl-1 "select table_catalog, table_schema from information_schema.columns limit 1")

  (query cl-1 "select '{\"a\":1}'::jsonb as x, 2 as y")

  (query cl-1 "select 12.00::numeric")

  (query cl-1 "select '2017-01-02'::timestamptz")
  (query cl-1
         "select x.* from information_schema.tables x limit 10"
         {:on-row #(println "ROW:" %)})
  (query cl-1 "select NULL as test")

  (do
    (.shutdownGracefully group)
    (def group (NioEventLoopGroup.))
    (def cl-1 (connect group opts)))
  )


