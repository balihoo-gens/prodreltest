import AssemblyKeys._

assemblySettings

name := "fulfillment"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.2"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  //aws
  "com.amazonaws" % "aws-java-sdk" % "1.6.12",
  "org.scala-lang" % "scala-actors" % "2.10.2",
  //jansi is included with ivy2, but deduplicate fails so exclude it from jline explicitly
  ("org.scala-lang" % "jline" % scalaVersion.value).exclude("org.fusesource.jansi", "jansi"),
  "org.slf4j" % "slf4j-nop" % "1.6.4",
  //joda
  "org.joda" % "joda-convert" % "1.5",
  "joda-time" % "joda-time" % "2.3",
  //json. Using play's implementation because its already known to the team
  "com.typesafe.play" % "play-json_2.10" % "2.2.0",
  "org.specs2" %% "specs2" % "2.3.12" % "test",
  "commons-io" % "commons-io" % "2.4",
  "commons-configuration" % "commons-configuration" % "1.10",
  "commons-beanutils" % "commons-beanutils" % "1.9.2",
  "com.google.api-ads" % "ads-lib" % "1.29.0",
  "com.google.api-ads" % "adwords-axis" % "1.29.0",
  "javax.activation" % "activation" % "1.1",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "org.eclipse.jetty" % "jetty-webapp" % "8.1.8.v20121106"
)

scalacOptions in Test ++= Seq("-Yrangepos")

// Read here for optional dependencies:
// http://etorreborre.github.io/specs2/guide/org.specs2.guide.Runners.html#Dependencies
resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)

///Use this to set a specific main class and run that when running the jar with:
///  java -jar target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar
// exportJars := true
// mainClass in (Compile, run) := Some("com.balihoo.fulfillment.main")
// mainClass in (Compile, packageBin) := Some("com.balihoo.fulfillment.main")

///Or, leave that all out and run the desired main like this
// java -cp target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar com.balihoo.fulfillment.workers.sendemailworker -p <propfile> -d <propdir>
