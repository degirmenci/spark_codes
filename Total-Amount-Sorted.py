from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerId = int(fields[0])
    amount = float(fields[2])
    return (customerId, amount)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalsByCustomerId = rdd.reduceByKey(lambda x, y: (x +y))
hop = totalsByCustomerId.map(lambda (x, y): (y, x)).sortByKey()



results = hop.collect()
for result in results:
    print result