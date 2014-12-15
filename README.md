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
SBT should take care of most of the dependencies for you; check build.sbt to see what is used
You will also need a number of properties which are set in the config directory.
Some of the property files include a .private file for settings that do not belong in source control:
  * aws.properties.private
    * ```domain=fauxfillment```
    * ```region=us-west-2```
    * ```workflowName=generic```
    * ```workflowVersion=3```
  * ftp_account.properties.private: Add FTP credentials for each destination using this format.  Each destination is represented using a config key. Replace <configKey> in each line with the config key (e.g. localFtpHost or brandXFtpHost).
    * ```<configKey>FtpHost=<hostname>```
    * ```<configKey>FtpUsername=<username>```
    * ```<configKey>FtpPassword=<password>```


Deployment
--------
Deployment is done by running the deploy script
  * Set the configs (keys, domain, region) to your deployment environment
  * Set two environment variables aws to be used for deployment:
    * AWS_ACCESS_KEY_ID
    * AWS_SECRET_ACCESS_KEY
    * The deployment keys are not used runtime, that uses IAM roles. Keys used for deployment must have permissions to S3, create a CF stack etc.
  * If using a brand new region or domain: Set up the domains in the region:
    * DynamoDB table
    * SWF Region
    * SES: Verify any email addresses you may use (until we go with prod access)
  * From the project root, Run ```$ ./deployment/deploy```:
    * run it with ```--help``` to see the usage. Command line parameters can be used to set the aws region for deployment.
    * It generates a fat jar(containing all deps) using the sbt assembly plugin
    * It packaged up the jar along with all the config files in a dir in ```deployment/deployments```
    * It uploads the dir to Amazon S3
    * It fills in a Cloud Formation Template
    * It creates a Cloud Formation Stack based on the template. This involves:
      * launching an auto scaling group for the decider / workers
      * launch an EC2 instance for the dashboard
      * execute and installation and launch script on the instances

Running
--------
  * When using the deployment script, workers, decider and dashboard all run on EC2 instances automatically.
  * Before being able to run locally, you must have valid credentials in your environment:
    * AWS_ACCESS_KEY_ID
    * AWS_SECRET_ACCESS_KEY
    * These keys need to have the same permissions in AWS as the IAM roles have: access SWF and S3 primarily.
  * To run locally, use ```python launcher.py``` in the project root to run some or all of the classes
    * Usage: ```$python ./launcher [-j <jar name>] [<main name> ...]```
    * class name resolution will try to match the name to the list of defaults. This allows you to abbreviate by leaving a lot of namespace out. Ambiguous names result in the selection of the first match.
    * Example, ```$ ./launch_fulfillment -j myff.jar decider htmlrenderer``` would run just the decider and htmlrenderer from myff.jar
  * Without the launcher, the jar in contains all that is needed to run the code. Run the desired main using, for example:
    * ```java -cp target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar com.balihoo.fulfillment.workers.sendemailworker```
    * This will run the sendemail worker using the "config/sendemailworker.properties" as the config file
    * you can use a different properties file, or look for it in a different directory:
      * ```java -cp <jarname> <classname> [-p <propfile>] [-d <propdir>]```
  * Or you can just run from sbt: run ```sbt run``` from the project root and select the main to run

Development
-----------
Development information can be found on the [wiki](https://github.com/balihoo/fulfillment/wiki/Home)




