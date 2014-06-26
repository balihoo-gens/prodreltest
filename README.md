Fulfillment
===========

Overview
--------
The core of fulfillment is the **coordinator**. This program operates as an Amazon SWF decider, managing and scheduling
activity tasks for worker processes to complete.

**Workers** are independent processes that solve small parts of the fulfillment workflow. The FulfillmentWorker base
class takes care of registering activity types and polling the matching activity queue. 

*NOTE:* Activity type+version can only be registered ONCE!

Building
--------
SBT should take care of most of the dependencies for you.  You'll also need a
.\<application\>.properties file in the project root.  Defaults for each application have been provided
as templates.

Deployment
--------
build a fat jar (containing all deps) with:
sbt> assembly
this will put the jar in:
    target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar

Running
--------
The jar contains all that is needed to run the code. Run the desired main using, for example:
    java -cp target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar com.balihoo.fulfillment.workers.sendemailworker
This will run the sendemail worker using the "config/sendemailworker.properties" as the config file
Alternatively, you can use a different properties file, or look for it in a different directory:
    test_worker -p <propfile> -d <propdir>

