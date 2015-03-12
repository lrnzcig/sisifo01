# sisifo01
Test twitter api

twitter-spout
=============
Connects to Twitter streaming API and extracts to csv files for the ddbb schema in schemaTwitter.xlsx

Run from eclipse or using mvn exec:java (needs settings.xml)

Uses hbc-twitter4j lib to connect
(Actually copied from hbc use example; to be refactorized to right names for projects and packages)

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

