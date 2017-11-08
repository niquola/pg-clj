(ns cljp.tmp)

;; public class TimeClient {
;;     public static void main(String[] args) throws Exception {
;;         String host = args[0];
;;         int port = Integer.parseInt(args[1]);
;;         EventLoopGroup workerGroup = new NioEventLoopGroup();
;;         try {
;;             Bootstrap b = new Bootstrap(); // (1)
;;             b.group(workerGroup); // (2)
;;             b.channel(NioSocketChannel.class); // (3)
;;             b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
;;             b.handler(new ChannelInitializer<SocketChannel>() {
;;                 @Override
;;                 public void initChannel(SocketChannel ch) throws Exception {
;;                     ch.pipeline().addLast(new TimeClientHandler());
;;                 }
;;             });
;;             // Start the client.
;;             ChannelFuture f = b.connect(host, port).sync(); // (5)

;;             // Wait until the connection is closed.
;;             f.channel().closeFuture().sync();
;;         } finally {
;;             workerGroup.shutdownGracefully();
;;         }
;;     }
;; }

#_(defn run []
  (let [boss-group (NioEventLoopGroup.)
        workers-group (NioEventLoopGroup.)]
    (try
      (let [b (ServerBootstrap.)]
        (-> b
          (.group boss-group workers-group)
          (.channel NioServerSocketChannel)
          (.childHandler
           (proxy [ChannelInitializer] []
             (initChannel [^SocketChannel ch]
               (-> ch
                   (.pipeline)
                   (.addLast
                    (into-array ChannelHandler [(handler)]))))))
          (.option ChannelOption/SO_BACKLOG (Integer. 128))
          (.childOption ChannelOption/SO_KEEPALIVE true))

        (-> (.sync (.bind  b 7777))
          (.channel)
          (.closeFuture)
          (.sync)))
      (catch Exception e
        (println e))
      (finally
        (.shutdownGracefully boss-group)
        (.shutdownGracefully workers-group)))))

;; (defonce server (atom nil))

;; @server

;; (defn restart []
;;   (let [t (Thread. run)]
;;     (reset! server t)
;;     (.start t)
;;     (.isAlive t)))

;; (comment 
;;   (restart)
;;   (when-let [s @server] (.interrupt s))
;;  )


