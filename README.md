# pushapi-java
Using the Coveo Cloud Push API in Java

**CoveoPushAPI.java** - This class implements the fundamental interactions with the Coveo Cloud Push API for indexing custom content, such as changing a source status, uploading files to AWS S3, and adding or deleting documents from a source.

**EXAMPLES**

The following classes demonstrate common use cases with the Push API.  Each one executes a sequence of steps, such as constructing JSON documents and executing various commands with CoveoPushAPI.

**UploadLargeFileTest** - This class demonstrates uploading a single, large file to Coveo Cloud.  The file is placed in AWS S3 before being added to the source.

**DeleteTest** - This class demonstrated deleting a single document from a Coveo Cloud source
