import AssemblyKeys._

assemblySettings

name := "fulfillment"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.2"

// Specify the local repos first for best performance.
resolvers ++= Seq(
  "Internal Snapshot Repository" at "https://archiva.office.balihoo.com/archiva/repository/snapshots",
  "Internal Repository" at "https://archiva.office.balihoo.com/archiva/repository/internal",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases",
  "Keyczar at Google Code" at "http://keyczar.googlecode.com/svn/trunk/java/maven/"
)

// Don't use the Maven Central repository.  Go through the local repo, so everything is cached.
externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false)

libraryDependencies ++= Seq(
  //aws
  "com.google.guava" % "guava" % "18.0",
  "com.amazonaws" % "aws-java-sdk" % "1.6.12",
  "org.scala-lang" % "scala-actors" % "2.10.2",
  //jansi is included with ivy2, but deduplicate fails so exclude it from jline explicitly
  ("org.scala-lang" % "jline" % scalaVersion.value).exclude("org.fusesource.jansi", "jansi"),
  "org.slf4j" % "slf4j-nop" % "1.6.4",
  //joda
  "org.joda" % "joda-convert" % "1.5",
  "joda-time" % "joda-time" % "2.3",
  //json. Using play's implementation because its already known to the team
  "com.typesafe.play" % "play-json_2.10" % "2.4.0-M1",
  "org.specs2" %% "specs2" % "2.3.12" % Test,
  "commons-io" % "commons-io" % "2.4",
  "commons-configuration" % "commons-configuration" % "1.10",
  "commons-beanutils" % "commons-beanutils" % "1.9.2",
  ("com.balihoo.commons" % "commons-client" % "14.19-SNAPSHOT")
    .exclude("com.google.guava", "guava")
    .exclude("com.sun.jersey", "jersey-core"),
  ("com.google.api-ads" % "ads-lib" % "1.35.1").exclude("com.google.guava", "guava-jdk5"), // exclude old guava
  ("com.google.api-ads" % "adwords-axis" % "1.35.1").exclude("com.google.guava", "guava-jdk5"), // exclude old guava
  "javax.activation" % "activation" % "1.1",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "org.eclipse.jetty" % "jetty-webapp" % "8.1.8.v20121106",
  "org.keyczar" % "keyczar" % "0.66",
  "com.stackmob" %% "newman" % "1.3.5",
  ("com.balihoo.socialmedia" % "facebook-client" % "14.19-SNAPSHOT").exclude("ch.qos.logback", "logback-classic"),
  "org.glassfish.jersey.test-framework.providers" % "jersey-test-framework-provider-grizzly2" % "2.12" % Test,
  "com.github.tototoshi" %% "scala-csv" % "1.1.2",
  "org.xerial" % "sqlite-jdbc" % "3.8.7",
  "com.github.fge" % "json-schema-validator" % "2.2.6",
  "com.netaporter" %% "scala-uri" % "0.4.3",
  "com.jsuereth" %% "scala-arm" % "1.3",
  "org.scalaj" %% "scalaj-http" % "1.0.1"
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

resourceGenerators in Compile <+= (resourceManaged, baseDirectory) map
  { (managedBase, base) =>
    val webappBase = base / "src" / "main" / "webapp"
    for {
      (from, to) <- webappBase ** "*" x rebase(webappBase, managedBase /
        "main" / "webapp")
    } yield {
      Sync.copy(from, to)
      to
    }
  }

net.virtualvoid.sbt.graph.Plugin.graphSettings

//exclude some sources to allow testing compilation
// of refactoring changes one file at a time
excludeFilter in unmanagedSources :=
/*
//adapters
  "FTPAdapter.scala" ||
  "SQSAdapter.scala" ||
  "SESAdapter.scala" ||
  "AWSAdapter.scala" ||
  "SWFAdapter.scala" ||
  "DynamoAdapter.scala" ||
  "AdWordsAdapter.scala" ||
//utilities
  "Splogger.scala" ||
  "Getch.scala" ||
//config
  "PropertiesLoader.scala" ||
  "FTPUploadConfig.scala" ||
  "FulfillmentSection.scala" ||
  "CategorizedSections.scala" ||
  "FulfillmentCoordinator.scala" ||
  "SWFIdentifier.scala" ||
//dashboard
  "RestServlet.scala" ||
  "Dashboard.scala" ||
  "CommandComponent.scala" ||
//workers
  "FulfillmentWorker.scala" ||
  "AdWordsAccountCreator.scala" ||
  "AdWordsAccountLookup.scala" ||
  "AdWordsAdGroupProcessor.scala" ||
  "AdWordsCampaignProcessor.scala" ||
  "AdWordsUserInterests.scala" ||
  "AdWordsImageAdProcessor.scala" ||
  "AdWordsTextAdProcessor.scala" ||
  "GeoNamesTimeZoneRetriever.scala" ||
  "Chaos.scala" ||
  "EmailAddressVerifier.scala" ||
  "EmailSender.scala" ||
  "EmailVerifiedAddressLister.scala" ||
  "FtpUploadValidator.scala" ||
  "FtpUploader.scala" ||
  "Noop.scala" ||
  "ZipCodeDemographics.scala" ||
  "ParticipantData.scala" ||
  "EmailWorkers.scala" ||
*/
  ""

fork in Test := true

javaOptions in Test += "-Xmx2048m"

