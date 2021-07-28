import psycopg2
from datetime import datetime
from time import sleep

con = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="127.0.0.1", port="5432")

cur = con.cursor()

#Query to get entire snapshot of resources

cur.execute('''select changes.arn, changes.resource, changes.event_time, changes.record_type from changes 
INNER JOIN (SELECT changes.arn, max(event_time) as max_time
			FROM changes GROUP by arn ) as most_recent
ON changes.arn = most_recent.arn and changes.event_time = most_recent.max_time Where changes.record_type != 'DELETE';''')


#Query to get timline of one resource

#cur.execute('''select changes.arn, changes.resource, changes.event_time, changes.record_type from changes 
#INNER JOIN (SELECT changes.arn, max(event_time) as max_time
#			FROM changes GROUP by arn ) as most_recent
#ON changes.arn = most_recent.arn and changes.event_time = most_recent.max_time Where changes.record_type != 'DELETE' and changes.arn LIKE 'arn0';''')