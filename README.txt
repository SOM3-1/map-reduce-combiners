
This project contains three Hadoop MapReduce programs:


WordCountWithCombiner.java
Counts the frequency of each word in a text file using a MapReduce combiner.

AvgByKeyWithCombiner.java
Computes the average value per key using a combiner that aggregates (sum, count).

AvgByKeyWithHashtable.java
Computes the average value per key using in-mapper combining with a HashMap.

word_count.txt
Input file for the WordCount program (plain text).

avg_count.txt
Input file for AvgByKey programs. Format:


From the project root directory:

mvn install 


This creates the JAR file:

target/1002246620.dg-1.0.0-SNAPSHOT.jar

Run Programs: 


WordCount with Combiner:

~/hadoop-3.3.2/bin/hadoop jar \
target/1002246620.dg-1.0.0-SNAPSHOT.jar \
cse6322.hw1.WordCountWithCombiner \
word_count.txt out_wordcount



AvgByKey with Combiner:

~/hadoop-3.3.2/bin/hadoop jar \
target/1002246620.dg-1.0.0-SNAPSHOT.jar \
cse6322.hw1.AvgByKeyWithCombiner \
avg_count.txt out_avg_combiner



AvgByKey with Hashtable:

~/hadoop-3.3.2/bin/hadoop jar \
target/1002246620.dg-1.0.0-SNAPSHOT.jar \
cse6322.hw1.AvgByKeyWithHashtable \
avg_count.txt out_avg_hashtable


Results are stored in:

out_wordcount/part-r-00000
out_avg_combiner/part-r-00000
out_avg_hashtable/part-r-00000

Notes