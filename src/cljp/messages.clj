(ns cljp.messages
  (:require [clojure.tools.logging :as log])
  (:import
   [io.netty.buffer ByteBuf]
   [io.netty.channel ChannelHandler ChannelHandlerContext ChannelInboundHandlerAdapter]
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

(defn query [sql]
  (let [buf (Unpooled/buffer)]
    (.writeByte buf (byte \Q))
    (.writeInt buf (int 0))
    (.writeBytes buf (.getBytes sql))
    (.writeByte buf (byte 0))
    (header-length buf)
    (println (.toString buf (Charset/forName "utf-8")))
    buf))

(def utf (Charset/forName "utf-8"))

(defn parse-md5-auth [msg]
  (log/info "Auth"  (.readInt msg))
  (let [salt (byte-array 4)]
    (.readBytes msg salt 0 (.readableBytes msg))
    salt))

(def MAX-SIZE (* 1000 1000 100))

(defn buf-to-str [^ByteBuf b]
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

(defn parse-data-row
  [^ByteBuf b
   ^ChannelHandlerContext ctx
   ^Object out
   {{col-defs :columns
     rows :rows} :query
    :as opts}]
  (let [cols-num (.readShort ^ByteBuf b)]
    (loop [x (int cols-num)
           res (transient {})]
      (if (> x 0)
        (let [len     ^int (.readInt ^ByteBuf b)
              col     (when (> len 0) (.readSlice ^ByteBuf b len))
              col-def (nth col-defs (- cols-num x) nil)
              k (clojure.lang.Keyword/intern ^String (:col-name col-def))]
          (recur (dec x)
                 (if col
                   (assoc! res k (.toString ^ByteBuf col  ^Charset utf))
                   res)))
        (swap! rows conj! (persistent! res)))))
  nil)

(defn parse-default [b ctx out opts]
  (log/info "MSG: " (buf-to-str b)))

(defn parse-auth [m ctx out opts]
  (let [salt (parse-md5-auth m)]
    (.write ctx (md5-login {:password (:password opts) :user (:user opts)} salt))
    (.flush ctx)))

(def statuses
  {\I :idle
   \T :transaction
   \E :failed-tx})

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

  (let [status (get statuses (char (.readByte m)))
        connected (:connected opts )]
    (when-not (realized? connected) (deliver connected {:status status}))
    {:status status}))

(defn  parse-parameter-status [m ctx out opts]
  ;; ParameterStatus (B)
  ;; Byte1('S')
  ;; Identifies the message as a run-time parameter status report.

  ;; Int32
  ;; Length of message contents in bytes, including self.

  ;; String
  ;; The name of the run-time parameter being reported.

  ;; String
  ;; The current value of the parameter.
  (let [params (loop [param {}]
                 (if (<= (.readableBytes m) 0)
                   param
                   (let [k (read-cstring m)
                         v (read-cstring m)]
                     (recur (assoc param k v)))))]

    {:params params}))

(defn parse-query [m ctx out opts]
  (let [query (read-cstring m)]
    (println "QUERY: " query))
  nil)

(defn parse-close [m ctx out {{res :result rows :rows} :query :as opts}]
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
  (deliver res {:status :ok
                :rows (persistent! @rows)})
  nil)


(def error-fields
  {
   \S :local-severity
   ;; Severity: the field contents are ERROR, FATAL, or PANIC (in an error
   ;; message), or WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message),
   ;; or a localized translation of one of these. Always present.
   \V :severity
   ;; Severity: the field contents are ERROR, FATAL, or PANIC (in an error
   ;; message), or WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message).
   ;; This is identical to the S field except that the contents are never
   ;; localized. This is present only in messages generated by PostgreSQL
   ;; versions 9.6 and later.

   \C :code
   ;; Code: the SQLSTATE code for the error (see Appendix A). Not localizable.
   ;; Always present.

   \M :message
   ;; Message: the primary human-readable error message. This should be accurate
   ;; but terse (typically one line). Always present.

   \D :details
   ;; Detail: an optional secondary error message carrying more detail about the
   ;; problem. Might run to multiple lines.

   \H :hint
   ;; Hint: an optional suggestion what to do about the problem. This is
   ;; intended to differ from Detail in that it offers advice (potentially
   ;; inappropriate) rather than hard facts. Might run to multiple lines.

   \P :position
   ;; Position: the field value is a decimal ASCII integer, indicating an error
   ;; cursor position as an index into the original query string. The first
   ;; character has index 1, and positions are measured in characters not bytes.

   \p :internal-position
   ;; Internal position: this is defined the same as the P field, but it is used
   ;; when the cursor position refers to an internally generated command rather
   ;; than the one submitted by the client. The q field will always appear when
   ;; this field appears.

   \q :internal-query
   ;; Internal query: the text of a failed internally-generated command. This
   ;; could be, for example, a SQL query issued by a PL/pgSQL function.

   \W :where
   ;; Where: an indication of the context in which the error occurred. Presently
   ;; this includes a call stack traceback of active procedural language
   ;; functions and internally-generated queries. The trace is one entry per
   ;; line, most recent first.

   \s :schema
   ;; Schema name: if the error was associated with a specific database object,
   ;; the name of the schema containing that object, if any.

   \t :table
   ;; Table name: if the error was associated with a specific table, the name of
   ;; the table. (Refer to the schema name field for the name of the table's
   ;; schema.)

   \c :column
   ;; Column name: if the error was associated with a specific table column, the
   ;; name of the column. (Refer to the schema and table name fields to identify
   ;; the table.)

   \d :data-type
   ;; Data type name: if the error was associated with a specific data type, the
   ;; name of the data type. (Refer to the schema name field for the name of the
   ;; data type's schema.)

   \n :constraint
   ;; Constraint name: if the error was associated with a specific constraint,
   ;; the name of the constraint. Refer to fields listed above for the
   ;; associated table or domain. (For this purpose, indexes are treated as
   ;; constraints, even if they weren't created with constraint syntax.)

   \F :file
   ;; File: the file name of the source-code location where the error was
   ;; reported.

   \L :line
   ;; Line: the line number of the source-code location where the error was
   ;; reported.

   \R :routine
   ;; Routine: the name of the source-code routine reporting the error.
   })

(defn parse-error [m ctx out {{res :result rows :rows} :query :as opts}]

  ;; ErrorResponse (B)
  ;; Byte1('E')
  ;; Identifies the message as an error.

  ;; Int32
  ;; Length of message contents in bytes, including self.

  ;; The message body consists of one or more identified fields, followed by a
  ;; zero byte as a terminator. Fields can appear in any order. For each field
  ;; there is the following:

  ;; Byte1
  ;; A code identifying the field type; if zero, this is the message terminator
  ;; and no string follows. The presently defined field types are listed in
  ;; Section 52.8. Since more field types might be added in future, frontends
  ;; should silently ignore fields of unrecognized type.

  ;; String
  ;; The field value.

  (let [error (loop [acc {}]
                (let [code (.readByte m)]
                  (if (= 0 code)
                    acc
                    (recur (assoc acc (get error-fields (char code)) (read-cstring m))))))]
    (println "ERROR:" error)
    (deliver res {:status :error :error error})
    nil))

(def parsers
  {(byte \D) parse-data-row
   (byte \T) parse-row-desc
   (byte \Z) parse-ready-for-query
   (byte \S) parse-parameter-status
   (byte \Q) parse-query
   (byte \E) parse-error
   (byte \C) parse-close})

(comment
  (password (.getBytes "user") (.getBytes "pass") (.getBytes "salt"))
  (spit "test/messages/startup" (.toString
                                 #_(startup-message)
                                 (md5-login {:password "pass" :user "posgress"} (.getBytes "ab"))
                                 (Charset/forName "utf-8")))

  )

