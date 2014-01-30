Fulfillment
===========
In its current state, the fulfillment project aims to explore the functionality
of Amazon Simple Workflow and how to consume those services with Scala.

SBT should take care of most of the dependencies for you.  You'll also need a
.fulfillment.properties file in the project root.  A default has been provided
as a template.

There are some prototype mains that demonstrate functionality. Run them in this order
1) Register types (only needed when type values change)
2) Start the decider and worker classes polling.
3) Start a new workflow execution.