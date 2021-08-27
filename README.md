# Big-Data-IPL-DATA-analysis
Step 1: Start the spark shell using following command and wait for prompt to appear
          $spark-shell
Step 2: Create RDD from a file in HDFS, type the following on spark-shell and press enter:
         $var linesRDD = sc.textFile("/home/nallani/Desktop/mounika/deadlock.txt")
Step 3: Convert each record into word
         $var wordsRDD = linesRDD.flatMap(_.split(" "))
Step 4: Convert each word into key-value pair
        $var wordsKvRdd = wordsRDD.map((_, 1))
Step 5: Group By key and perform aggregation on each key:
         $var wordCounts = wordsKvRdd.reduceByKey(_ + _ )
Step 6: Save the results into HDFS:
           $wordCounts.saveAsTextFile("/home/nallani/Desktop/mounika/output")
