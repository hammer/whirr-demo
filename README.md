Simple Maven project to run a Hadoop cluster using Whirr.

To use, you'll need to have downloaded and installed Whirr, and you'll need an account on AWS.

Next, you can do:

     $ git clone git://github.com/hammer/whirr-demo.git whirr-demo
     $ cd whirr-demo
     $ export MAVEN_OPTS="-Xmx1g -Dwhirr.provider=ec2 -Dwhirr.ssh.keyfile=${AWS_SSH_KEYFILE} -Dwhirr.user=${AWS_ACCESS_KEY_ID} -Dwhirr.key=${AWS_SECRET_ACCESS_KEY} -Dwhirr.runurl.base=http://cloudera-tom.s3.amazonaws.com/"
     $ mvn clean; mvn compile; mvn exec:java -Dexec.mainClass="org.apache.whirr.demo.WhirrHadoop"

At this stage, you should have a cluster running on EC2 and an ssh proxy running locally.

Using Firefox, you can navigate to the cluster in your web browser. Make sure you have FoxyProxy installed,
grab the PAC file linked at https://issues.apache.org/jira/browse/WHIRR-35, and punch in the URL for your EC2 instance.

