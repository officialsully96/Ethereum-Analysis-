import re
import pyspark

sc = pyspark.SparkContext()

def spark(line):
    try:
        fields = line.split(',')
        if len(fields) == 7:
            str(fields[2])
            if int(fields[3]) == 0:
                return False
        elif len(fields) == 5:
            str(fields[0])
        else:
            return False
        return True
    except:
        return False

contracts = sc.textFile('hdfs://andromeda.student.eecs.qmul.ac.uk/data/ethereum/contracts')
filtered_lines_contracts = contracts.filter(spark)
address = filtered_lines_contracts.map(lambda l:(l.split(",")[0],1))

transactions = sc.textFile('hdfs://andromeda.student.eecs.qmul.ac.uk/data/ethereum/transactions')
filtered_transactions = transactions.filter(spark)
transaction_value = filtered_transactions.map(lambda l: (l.split(",")[2],float(l.split(",")[3])))
output_transactions = transaction_value.join(address)
transaction_value_reduced = output_transactions.reduceByKey(lambda (a,b), (c,d): (float(a) + float(c), b+d))

top_10 = transaction_value_reduced.takeOrdered(10, key = lambda x: -x[1][0])

for i in topten:
    print(i)