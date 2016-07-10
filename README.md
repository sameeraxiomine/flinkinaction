Welcome to Flink in Action 

This source code distribution is a companion to the
Flink in Action book available from Manning Publications.
To purchase an electronic or printed copy of the book,
visit http://www.manning.com/

R E Q U I R E M E N T S
-----------------------
  * JDK 1.7+
  * Maven 3.+ (to run the automated examples)

I N S T A L L A T I O N
-----------------------
You've already unpacked the distribution, since you're reading this 
file. You may now run most of the code using Maven.  We recommend
that you open the code in your favorite editor or IDE to follow how 
the examples work.

R U N N I N G
-------------
We recommend you run the examples either right from the IDE or using 
Maven.

However, you may run the examples without maven with some
unsupported and manual configuration.



R U N N I N G     W I T H    MAVEN
----------------------------------
The code is primarily  the classes in relevant chapter, with  Java main()
programs .  All the JUnit test cases and several of the main() 
programs are easily run using the Maven or throught your IDE.


Several of the included Java main() programs may be launched 
individually.  

SimpleBatchWordCount=>Demonstrates a simple word count using DataSet API.
mvn exec:java -Dexec.mainClass="com.manning.fia.c02.SimpleBatchWordCount" -Dexec.cleanupDaemonThreads=false
 
SimpleStreamingWordCount=>Demonstrates a simple word count using Datastream API.
mvn exec:java -Dexec.mainClass="com.manning.fia.c02.SimpleStreamingWordCount" -Dexec.cleanupDaemonThreads=false


SimpleTableAPIBasedWordCount=>Demonstrates the use of Table.
mvn exec:java -Dexec.mainClass="com.manning.fia.c02.SimpleTableAPIBasedWordCount" -Dexec.cleanupDaemonThreads=false


StreamingWordCount=>Demonstrates a streaming word count example.
mvn exec:java -Dexec.mainClass="com.manning.fia.c02.StreamingWordCount" -Dexec.cleanupDaemonThreads=false

MediaBatchTansformations=>Demonstrates batch operators i.e DataSet API.
mvn exec:java -Dexec.mainClass="com.manning.fia.c03.media.MediaBatchTansformations" -Dexec.cleanupDaemonThreads=false


	

T I P S
-------
  *  Make sure you have mvn in your system  path when running 
  through command prompt

  * Please ensure you are able to build the code using mvn clean install 
    or mvn clean package

 
  * Follow along with the examples using the source code and the
    book itself.  The code was written for the book and makes much
    more sense in context with elaborate explanation as the code is 
    packaged as chapters.
    
    

K N O W N   I S S U E S
-----------------------
  * Performance tests may fail if your system is heavily loaded at
    the time or has insufficient RAM or processor/disk speed
  
  
C O N T A C T    I N F O R M A T I O N
--------------------------------------
Manning provides an Author Online forum accessible from:
	http://www.manning.com/


For general Flink information and documenation, please visit
Flink's website for pointers to FAQ's, articles, the user e-mail 
list, and the informative and ever evolving wiki.  The main flink
website is: http://flink.apache.org