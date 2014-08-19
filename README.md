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
1. awskeys.properties.private contains two keys:
  * ```aws.accessKey=<your key>```
  * ```aws.secretKey=<your secret key>```
2. aws.properties.private
  * ```domain=fauxfillment```
  * ```region=us-west-2```
  * ```workflowName=generic```
  * ```workflowVersion=3```
3. ftp_account.properties.private
  Add FTP credentials for each destination using this format.  Each destination is represented using a config key.
  Replace <configKey> in each line with the config key (e.g. localFtpHost or brandXFtpHost).
  * ```<configKey>FtpHost=<hostname>```
  * ```<configKey>FtpUsername=<username>```
  * ```<configKey>FtpPassword=<password>```


Deployment
--------
Deployment involves several steps:
1. Build and package up the app:
  * From the project root, Run ```$ ./deploy/packup.sh```
  * This will result in a tar ball called ```fulfillment-<timestamp>.tar.gz```
  * This app tarball contains the complete fulfillment app and all its dependencies
    * it generated a fat jar(containing all deps) using the sbt assembly plugin
    * It packaged up the jar along with all the config files
    * !! Make sure to set the configs (keys, domain, region) to your deployment environment
2. Upload the app tarball to Amazon S3
  * This ought to be in the region you want to run the app in, matching the region in config
  * Do not overwrite an existing tarball; autoscaling instances may pick those up, so always use a new name (hence the timestamp)
  * it is ok to delete old tarballs from S3 if you are confident they are no longer used
3. Set up the domains in the region:
  * DynamoDB table
  * SWF Region
  * SES: Verify any email addresses you may use (until we go with prod access)
4. Set up the CloudFormation using the template in ```deploy/aws_template_basic.json```
  * If you have not changed the template, and a CloudFormation stack has already been set up in this region, you may just pick it from the list. Otherwise, upload the new json template to S3
  * Fill out the parameters:
    * AWS Keys with access to the location of the fulfillment tarball in S3
    * The fulfillment tarball filename to use
    * The type of instance
    * The min and max number of instances. Should be at least one
    * the SSH access key pair
  * This will instantiate the first instance and run the app on it (takes up to 10min)
    * *THIS NEEDS TESTING*
    * Autoscaling is set up in the template to create a new instance at 90% cpu and remove one at < 70%

Running
--------
1. Use the ```launch_fulfillment``` script in the project root to run the decider and all the workers by default.decider from myff.jar
Usage:
  * ```$ ./launch_fulfillment [<jar name> (<main name> ...)]```
  * for example, ```$ ./launch_fulfillment myff.jar com.balihoo.fulfillment.decider``` would run just the decider from myff.jar
2. Without the launcher, the jar in contains all that is needed to run the code. Run the desired main using, for example:
    java -cp target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar com.balihoo.fulfillment.workers.sendemailworker
  This will run the sendemail worker using the "config/sendemailworker.properties" as the config file
3. Alternatively, you can use a different properties file, or look for it in a different directory:
    test_worker -p <propfile> -d <propdir>

Development
-----------
Development information can be found on the [wiki](https://github.com/balihoo/fulfillment/wiki/Home)




