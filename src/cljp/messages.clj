(ns cljp.messages
  (:import
   [io.netty.buffer ByteBuf]
   [java.nio.charset Charset]
   [java.security MessageDigest]
   [javax.xml.bind DatatypeConverter]
   [io.netty.buffer Unpooled]))

(defn md5 [x]
  (let [d (MessageDigest/getInstance "MD5")]
    (.update d x)
    (-> d
        .digest
        DatatypeConverter/printHexBinary
        .toLowerCase
        .getBytes)))

(defn header-length [buf & [pos]]
  (let [idx (.writerIndex buf)]
    (.markWriterIndex buf)
    (.writerIndex  buf (or pos 1))
    (.writeInt  buf (- idx (or pos 1)))
    (.resetWriterIndex buf)
    buf))

(defn write-hash-map [buf m]
  (doseq [[k v] m]
    (.writeBytes buf (.getBytes (name k)))
    (.writeByte buf (byte 0))
    (.writeBytes buf (.getBytes v))
    (.writeByte buf (byte 0))))

(defn startup-message []
  (let [buf (Unpooled/buffer)]
    (.writeInt buf (int 0)) ;; space for size
    (.writeShort buf (short 3))
    (.writeShort buf (short 0))

    (write-hash-map buf {:database "postgres" :user "postgres"})

    (.writeByte buf (byte 0))

    (header-length buf 0)))


(defn concat-ba [a b]
  (let [buf (byte-array (+ (count a) (count b)))]
    (System/arraycopy a 0 buf 0 (count a))
    (System/arraycopy b 0 buf (count a) (count b))
    buf))

(defn- password [user password salt]
  (md5 (concat-ba (md5 (concat-ba password user)) salt)))

(defn md5-login [{pass :password user :user} salt]
  (let [buf (Unpooled/buffer)
        secret (password (.getBytes user) (.getBytes pass) salt)]
    (.writeByte buf (byte \p))
    (.writeInt buf (int 0))
    (.writeBytes buf (.getBytes "md5"))
    (.writeBytes buf secret)
    (.writeByte buf (byte 0))
    (header-length buf)
    (println (.toString buf (Charset/forName "utf-8")))
    buf))


(comment
  (password (.getBytes "user") (.getBytes "pass") (.getBytes "salt"))
  (spit "test/messages/startup" (.toString
                                 #_(startup-message)
                                 (md5-login {:password "pass" :user "posgress"} (.getBytes "ab"))
                                 (Charset/forName "utf-8")))

  )

