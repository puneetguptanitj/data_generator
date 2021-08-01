import psycopg2
import time

con = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="127.0.0.1", port="5432")

cur = con.cursor()
batch_size = 50
#Query to get entire snapshot of resources
start  = time.time()
cur.execute('''select changes.arn, changes.resource, changes.event_time, changes.record_type from changes 
INNER JOIN (SELECT changes.arn, max(event_time) as max_time
			FROM changes GROUP by arn ) as most_recent
ON changes.arn = most_recent.arn and changes.event_time = most_recent.max_time Where changes.record_type != 'DELETE' limit ''' +  str(batch_size) + ''';''')
end = time.time()

print ("Time to get all resources " + str(batch_size) + " records at a time = " + str(end-start))

#Query to get timline of one resource
start  = time.time()
cur.execute('''select changes.arn, changes.resource, changes.event_time, changes.record_type from changes 
INNER JOIN (SELECT changes.arn, max(event_time) as max_time
			FROM changes GROUP by arn ) as most_recent
ON changes.arn = most_recent.arn and changes.event_time = most_recent.max_time Where changes.record_type != 'DELETE' and changes.arn LIKE 'arn0' order by event_time DESC limit ''' +  str(batch_size) + ''';''')
end = time.time()

print ("Time to get resource timeline for one arn " + str(batch_size) + " records at a time = " + str(end-start))
