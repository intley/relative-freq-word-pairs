## Relative Frequency for 100K Wikipedia documents

- In this assignment, I explored a set of 100,000 Wikipedia documents: 100KWikiText.txt, in which each line consists of the plain text extracted from an individual Wikipedia document.

- In order to do this, I configured a Hadoop environment in Pseudo Distributed and Fully Distributed
mode on Apache Hadoop 3.0.0

### Algorithm Steps:

1. Setup a basic Mapreduce workflow by writing the necessary configurations.
2. Create a Mapper class and within the Mapper class, parse the data in such
a way that the mapper forms a key and value paired of words such as (word 1, word 2)
and words (word 1, &)
3. Combiner counts the number of occurences of each word pair prior to sending it
to the reducer.
4. The reducer checks if the key is a single word or a word pair.
If it's a single word, check if the key is equal to the current word.
5. If yes, count the total occurences of this word.
If not, set it as the current word and count the number of occurences.
6. If it's a word pair, count the total number of occurences of the word pair and
then calculate the relative frequencies.
7. Add this word pairs to a Treeset, and retrieve the top 100 word pairs.
8. Print the output to a output file.

### Pseudo-Distributed Mode Steps:
*
```tar xzvf hadoop-3.0.0.tar.gz
```
*
```cd
vi ./hadoop/etc/hadoop/core-site.xml
## adding these lines to the file ##
<configuration>
<property>
<name>fs.defaultFS</name>
<value>hdfs://10.1.37.12:9000</value>
</property>
</configuration>

vi ./hadoop/etc/hadoop/hdfs-site.xml
## adding these lines to the file ##
<configuration>
<property>
<name>dfs.replication</name>
<value>1</value>
</property>
</configuration>
```

* ```hdfs namenode -format
```

* ```## to start hadoop cluster
start-dfs.sh
```

* ```hdfs dfs -put ~/hadoop/etc/hadoop input
```

* ```hadoop jar ~/hadoop/share/hadoop/input output
```

* ```hadoop fs -cat /home/ubuntu/output/output.txt
```

### Fully Distributed Mode Setup:

* Install Hadoop on all 3 instances. (1 namenode and 2 datanodes)

* ```tar xzvf hadoop-3.0.0.tar.gz
```

* Configure the hadoop -env.sh, core-site.xml, yarn-site.xml,
mapred-site.xml

* Configure the hdfs-site.xml file

* ```hdfs namenode -format
start-dfs.sh
```

* Execute the JAR file and view the output.

### References:
1. https://blog.insightdatascience.com/spinning-up-a-free-hadoop-cluster-step-by-step-c406d56bae42
2. https://medium.com/@luck/installing-hadoop-2-7-2-on-ubuntu-16-04-3a34837ad2db
