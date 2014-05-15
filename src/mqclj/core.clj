(ns mqclj.core)

(import 'org.apache.activemq.spring.ActiveMQConnectionFactory)
(import '(javax.jms Session DeliveryMode))

(defn create-factory [url] (doto (ActiveMQConnectionFactory.) (.setBrokerURL url)))

(defn create-connection [factory]
  (doto (.createConnection factory) (.start)))

(defn create-session [connection]
  (.createSession connection false Session/AUTO_ACKNOWLEDGE))

(defn create-queue [session queue]
  (.createQueue session queue))

(defn create-producer [session destination]
  (doto (.createProducer session destination) (.setDeliveryMode DeliveryMode/NON_PERSISTENT)))

(defn create-text-message [session text]
  (.createTextMessage session text))

(defn send-message [content url queue]
  (let [connection  (create-connection (create-factory url))
        session     (create-session connection)
        destination (create-queue session queue)
        producer    (create-producer session destination)
        message     (create-text-message session content)]
    (.send producer message)
    (.close session)
    (.close connection)))

