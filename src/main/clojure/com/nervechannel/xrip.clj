(ns com.nervechannel.xrip
  (:use [clojure.contrib.seq-utils :only [fill-queue]])
  (:use clojure.contrib.command-line)
  (:gen-class))

; from https://github.com/marktriggs/xml-picker-seq/blob/master/xml_picker_seq.clj

(defn root-element? [#^nu.xom.Element element]
  (instance? nu.xom.Document (.getParent element)))

(defn- extract [#^java.io.Reader rdr record-tag-name extract-fn enqueue]
  (let [empty (nu.xom.Nodes.)
        keep? (atom false)
        factory (proxy [nu.xom.NodeFactory] []

                  (startMakingElement [name ns]
                    (when (= name record-tag-name)
                      (reset! keep? true))
                      (let [#^nu.xom.NodeFactory this this]
                        (proxy-super startMakingElement name ns)))

                  (finishMakingElement [#^nu.xom.Element element]
                    (when (= (.getLocalName element) record-tag-name)
                      (when-let [value (extract-fn element)]
                        (enqueue value))
                      (reset! keep? false))

                    (if (or @keep? (root-element? element))
                      (let [#^nu.xom.NodeFactory this this]
                        (proxy-super finishMakingElement element))
                      empty)))]
    (.build (nu.xom.Builder. factory)
            rdr)))

(defn xml-picker-seq [rdr record-tag-name extract-fn]
  (fill-queue (fn [fill] (extract rdr record-tag-name extract-fn fill))
              {:queue-size 128}))

(defn get-vals [elem field]
  (let [nodes (.query elem field)]
    (map #(.. nodes (get %) getValue) (range (.size nodes)))))

(defn render-field [sub elem field]
  (apply str (interpose sub (get-vals elem field))))

(defn render-elem [fields sub del elem]
  (apply str (interpose del (map (partial render-field sub elem) fields))))

(defn print-elem [fields sub del eol elem]
  (dorun (print (apply str (render-elem fields sub del elem) eol))))

(defn -main [ & args]
  (with-command-line args
    "xrip -- extract values from an XML stream on stdin\nUsage: java -jar xrip.jar [opts] <fieldpath ...>"
    [[rec r "name of a single 'record' element" "doc"]
     [sub s "subdelimiter to output between instances of same field [default \" \"]"]
     [del d "delimiter to output between different fields [default \"\\t\"]"]
     [eol e "delimiter to output at end of line [default \"\\n\"]"]
     [flt f "xpath filter to apply to records TODO"]
     fields]
    (with-open [rdr (java.io.BufferedReader. *in*)]
      (dorun (xml-picker-seq rdr rec
                             (partial print-elem fields
                                      (or sub " ") (or "\t" del) (or "\n" eol)))))))
