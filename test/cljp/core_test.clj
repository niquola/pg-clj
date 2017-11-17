(ns cljp.core-test
  (:require [clojure.test :refer :all]
            [matcho.core :as matcho]
            [cljp.core :as sut]))

(deftest simple-test

  (def pool (sut/pool))
  (def conn (atom {:host "localhost"
                   :port 5555
                   :user "postgres"
                   :password "pass"
                   :query {:keywordize? true}}))

  @(sut/connect pool conn)


  (matcho/match @(sut/query conn "select 1 as a")
                {:rows [{:a "1"}]
                 :status :ok})

  (matcho/match
   @(sut/query conn "select NULL as test")
   {:rows [{:test nil}]})

  (matcho/match @(sut/query conn "select ups")
                {:status :error})
  
  (matcho/match
   @(sut/query conn "select 12.00::numeric as num")
   {:rows [{:num "12.00"}]
    :status :ok})

  (matcho/match
   @(sut/query conn "select '2017-01-02'::timestamptz  as ts")
   {:rows [{:ts "2017-01-02 00:00:00+00"}]
    :status :ok})

  (time @(sut/query conn "select x.* from information_schema.tables x"))

  (Thread/sleep 100)
  (sut/shutdown pool)
  )

(comment
  (do
    (.shutdownGracefully group)
    (def group (NioEventLoopGroup.))
    )

  (def res (query cl-1 "select * from information_schema.columns, information_schema.tables limit 100"))

  (count (:rows @res))

  @(query cl-1 "select '{\"a\":1}'::jsonb as x, 2 as y")

  @(query cl-1 "select 1 || 2")
  @(query cl-1 "select ups")
  @(query cl-1 "select 'ups', 'dups'")

  @(query cl-1 "select 12.00::numeric")

  @(query cl-1 "select '2017-01-02'::timestamptz")

  (time
   (def r @(query cl-1 "select x.* from information_schema.tables x")))

  (count (:rows r))

  (time (def r @(query cl-1 "select x from generate_series(1000) x")))

  (count r)

  @(query cl-1 "select NULL as test")


  cl-1

  )


