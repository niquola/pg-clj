(ns cljp.core
  (:import
   [java.util.concurrent Executors]
   [java.net Socket]
   [java.io BufferedOutputStream
    ByteArrayOutputStream
    ObjectOutputStream
    BufferedInputStream
    BufferedReader InputStreamReader]
   [java.nio ByteBuffer]

   [javax.xml.bind DatatypeConverter]
   [java.security MessageDigest]))


(defn md5 [x]
  (let [d (MessageDigest/getInstance "MD5")]
    (.update d x)
    (.getBytes (.toLowerCase (DatatypeConverter/printHexBinary (.digest d))))))

(defn concat-ba [a b]
  (let [buf (byte-array (+ (count a) (count b)))]
    (System/arraycopy a 0 buf 0 (count a))
    (System/arraycopy b 0 buf (count a) (count b))
    buf))

(defn password [user password salt]
  (md5 (concat-ba (md5 (concat-ba password user))
                  salt)))

(String. (password (.getBytes "postgres") (.getBytes "Hoha") (byte-array [1 3 4 5])))



(defn start []
  (declare out)
  (declare soc)
  (declare in)

  (try
    (.close out)
    (.close in)
    (.close soc)
    (catch Exception e
      (println e)))

  (def soc (Socket. "localhost", 5555))

  (def out (BufferedOutputStream. (.getOutputStream soc)))

  (def in (.getInputStream soc))

  (.isClosed soc))

(defn add-int [bas x]
  (.write bas
          (->
           (ByteBuffer/allocate 4)
           (.putInt (int x))
           (.array))))

(def types
  {:startup [nil (short 3) (short 0)]
   :password ['p']})


(defprotocol Pg
  (length [x])
  (write-to [x out]))

(extend-protocol Pg
  Short
  (length [x] 2)

  (write-to [x out]
    (.write out
            (->
             (ByteBuffer/allocate 2)
             (.putShort x)
             (.array)))
    out)

  String
  (length [x]
    (inc (count (.getBytes x))))

  (write-to [x out]
    (.write out (.getBytes x))
    (.write out 0)
    out)

  clojure.lang.Keyword
  (length [x]
    (length (name x)))

  (write-to [x out]
    (write-to (name x) out)
    out)

  clojure.lang.PersistentArrayMap
  (length [x]
    (reduce (fn [acc [k v]]
              (+ acc (length k) (length v)))
            1 x))

  (write-to [x out]
    (doseq [[k v] x]
      (write-to k out)
      (write-to v out))
    (.write out 0)
    out)

  java.lang.Character
  (length [x] 1)
  (write-to [x out]
    (.write out (byte-array [(byte x)]))
    out)


  clojure.lang.MapEntry
  (length [x]
    (reduce (fn [acc [k v]]
              (+ acc (length k) (length v) 1))
            0 x))

  (write-to [x out]
    (doseq [[k v] x]
      (write-to k out)
      (write-to v out))
    (.write out (byte 0))
    out)

  clojure.lang.PersistentVector
  (length [^clojure.lang.PersistentVector x]
    (->> x
         (reduce (fn [acc x] (+ acc (length x))) 0)))

  (write-to [x out]
    (doseq [v x]
      (write-to v out))
    out))

(length "asss")
(length (short 1))
(length :key)
(length {:a "ups"})

(write-to "Hello" (ByteArrayOutputStream.))
(write-to :key (ByteArrayOutputStream.))
(write-to (short 1) (ByteArrayOutputStream.))
(write-to {:a "hello" :b "c"} (ByteArrayOutputStream.))

(length [(short 3) (short 0) {:database "postgres" :user "postgres"}])

(write-to [(short 3) (short 0) {:database "postgres" :user "postgres"}]
       (ByteArrayOutputStream.))

(defn printbytes [x]
  (doseq [s x]
    (if (and (< 31 s) (< s 255))
      (print (char s))
      (print s))))

(defn message [out tp params]
  (let [l (+ 4 (length params))]
    (when tp (write-to tp out))
    (println "msg l" l)
    (add-int out l)
    (write-to params out)
    (.flush out)
    out))

(defn startup [out]
  (message out nil [(short 3)
                (short 0)
                {:user "postgres"
                 :database "postgres"}])
  out)

(defn auth-md5 [out user pass salt]
  (let [msg (str "md5" (String. (password (.getBytes user) (.getBytes pass) salt)))]
    (-> (message (ByteArrayOutputStream.) \p msg) (.toByteArray) (printbytes))
    (message out \p msg)
    out))

(defn query [out sql]
  (-> (message (ByteArrayOutputStream.) \Q sql) (.toByteArray) (printbytes))
  (message out \Q sql)
  out)

(defn read-response [in]
  (when (< 0 (.available in))
    (let [buf (ByteArrayOutputStream.)]
      (while (< 0 (.available in))
        (.write buf (.read in)))
      (let [res (.toByteArray buf)]
        (.close buf)
        res))))

(defn read-int [ba pos]
  (loop [i 0 v (int 0)]
    (if (> i 3) v
        (recur
         (inc i)
         (let [shift (* (- 4 1 i) 8)]
           (+ v (bit-shift-left (aget ba (+ i pos)) shift)))))))

(defn parse-auth-md [ba]
  (let [len (read-int ba 1)]
    {:type :auth-md
     :length len
     :salt (byte-array [(aget ba 9)
                        (aget ba 10)
                        (aget ba 11)
                        (aget ba 12)])}))

(defn parse-message [ba]
  (let [tp (aget ba 0)
        len (read-int ba 1)]
    (println (aget ba 0))
    (cond
      (= tp 82) (cond
                  (= 12 len) (parse-auth-md ba)
                  :else {:type :resp
                         :length len
                         :buffer ba})
      :else {:type "unknown"})))

;; (aget respo 0)
;; (parse-message respo)
;; (-> respo  printbytes)

;; -----------------

;; (do (start)
;;     (startup out))

;;  (do-auth)

;; (spit "auth" (String. resp))
;; (spit "respo" (String. respo))
;; (spit "data" (String. respo2))
; (spit "data" (String. respo2))

(def respo
  (read-response in))

(-> respo printbytes)

(query out "select * from information_schema.tables")
(query out "select 7")
(query out "select '{\"a\": 1}'::jsonb")

(def respo2
  (read-response in))

(-> respo2 printbytes)

(write-to [\p "hello"]
          (ByteArrayOutputStream.) )

(defn do-auth []

  (def resp (read-response in))
  (-> 
   resp
   printbytes)

  (def auth-resp
    (when resp
      (parse-message resp)))

  (mapv byte (:salt auth-resp))

  (auth-md5 out "postgres" "pass" (:salt auth-resp))


  )




(comment
  (write-to
   [\p (String. (password (.getBytes "postgres") (.getBytes "pass") (.getBytes "salt")))]
   (ByteArrayOutputStream.) )

  )




(String. (md5 (.getBytes "hello")))

