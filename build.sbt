name := "fulfillment"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  //aws
  "com.amazonaws" % "aws-java-sdk" % "1.6.12",
  //actors
  "com.typesafe.akka" %% "akka-actor" % "2.2.3",
  "org.scala-lang" % "scala-actors" % "2.10.2",
  //sqlite via slick
  "com.typesafe.slick" %% "slick" % "1.0.0",
  "org.slf4j" % "slf4j-nop" % "1.6.4",//slick needs a logger. This one disables logging
  "org.xerial" % "sqlite-jdbc" % "3.7.2",//sqlite driver
  //joda
  "org.joda" % "joda-convert" % "1.5",
  "joda-time" % "joda-time" % "2.3"
)
    