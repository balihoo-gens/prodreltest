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
.<application>.properties file in the project root.  Defaults for each application have been provided
as templates.

