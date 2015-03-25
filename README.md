# sisifo01
Java Twitter api library

twitter-data-colletion
======================
Connects to Twitter streaming and REST API and extracts to csv files for the ddbb schema in schemaTwitter.xlsx

For streaming:

- run from eclipse or using mvn exec:java (needs settings.xml)

- uses hbc-twitter4j lib to connect

- gets additional user info in a different thread (very slow due to REST API rate limits)


For REST API

- at the moment can only be run from eclipse

- takes into account rate limits and waits to reconnect

Example csv files at exampleFiles dir


twitter-rest-client
===================
JUnit classes for connecting to Twitter REST API (Application-only authentication)

Pass consumerKey and consumerSecret as JVM arguments:
-DconsumerKey=...
-DconsumerSecret=...


twitter-model
=============
Support classes for reading JSON into Tweeter model


additional:
- twitter-spout is obsolete, just using example from hbc library and writting into twitter's model

TO DO's
=======
- change System.out.println into log4j
- improve error control at com.sisifo.twitter_model.utils.TweetFileWriter
- junit tests are not unitary self-checked tests (mvn clean install -DskipTests=true)
