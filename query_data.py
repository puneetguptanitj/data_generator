import psycopg2
import time
import statistics

con = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="192.168.68.111", port="5432")
cur = con.cursor()
iterations=50
batch_size = 50

def get_median_time_for_query(query, iterations):
	query_times=[]
	for _ in range(iterations):
		#Query to get entire snapshot of resources
		start  = time.time()
		cur.execute(query)
		end = time.time()
	query_times.append(end-start)
	return statistics.median(query_times)

time_to_fetch_all_arns = get_median_time_for_query('''select changes.arn, changes.resource, changes.event_time, changes.record_type from changes 
					INNER JOIN (SELECT changes.arn, max(event_time) as max_time
					FROM changes GROUP by arn ) as most_recent
					ON changes.arn = most_recent.arn and changes.event_time = most_recent.max_time Where changes.record_type != 'DELETE' limit ''' +  str(batch_size) + ''';''', iterations)

print ("Median time to get all resources " + str(batch_size) + " records at a time = " + str(time_to_fetch_all_arns))

time_to_fetch_one_arn = get_median_time_for_query('''select changes.arn, changes.resource, changes.event_time, changes.record_type from changes 
						INNER JOIN (SELECT changes.arn, max(event_time) as max_time
						FROM changes GROUP by arn ) as most_recent
						ON changes.arn = most_recent.arn and changes.event_time = most_recent.max_time Where changes.record_type != 'DELETE' and changes.arn LIKE 'arn0' order by event_time DESC limit ''' +  str(batch_size) + ''';''', iterations)

print ("Median time to get resource timeline for one arn " + str(batch_size) + " records at a time = " + str(time_to_fetch_one_arn))
