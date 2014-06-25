name := "fulfillment"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.2"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  //aws
  "com.amazonaws" % "aws-java-sdk" % "1.6.12",
  "org.scala-lang" % "scala-actors" % "2.10.2",
  "org.slf4j" % "slf4j-nop" % "1.6.4",
  //joda
  "org.joda" % "joda-convert" % "1.5",
  "joda-time" % "joda-time" % "2.3",
  //json. Using play's implementation because its already known to the team
  "com.typesafe.play" % "play-json_2.10" % "2.2.0",
  "org.specs2" %% "specs2" % "2.3.12" % "test",
  "commons-io" % "commons-io" % "2.4",
  "commons-configuration" % "commons-configuration" % "1.10",
  "com.google.api-ads" % "ads-lib" % "1.28.0",
  "com.google.api-ads" % "adwords-axis" % "1.28.0",
  "javax.activation" % "activation" % "1.1",
  "com.github.scopt" %% "scopt" % "3.2.0"
)

scalacOptions in Test ++= Seq("-Yrangepos")

// Read here for optional dependencies:
// http://etorreborre.github.io/specs2/guide/org.specs2.guide.Runners.html#Dependencies
resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)
