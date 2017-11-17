(defproject cljp "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-beta4"]
                 [cheshire "5.6.3"]
                 [org.clojure/tools.logging "0.3.1"]
                 [io.netty/netty-all "4.1.16.Final"]]
  :plugins [[cider/cider-nrepl "0.15.1"]]
  :aot :all
  :profiles {:dev {:dependencies [[matcho "0.1.0-RC6"]
                                  [org.clojure/java.jdbc "0.6.1"]
                                  [org.postgresql/postgresql "9.4.1211.jre7"]]}})
