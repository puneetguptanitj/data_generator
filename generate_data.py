import psycopg2
from datetime import datetime
from time import sleep

con = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="127.0.0.1", port="5432")
cur = con.cursor()

def insert_update_records_for_arn(arn, num):
    total_inserted = 0
    batch_size = min(100 , num)
    while(total_inserted < num):
        insert_command = """INSERT into changes VALUES """
        for i in range(batch_size):
            time_of_record = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            insert_command = insert_command  + """('""" + arn + """' , '{"Resources":{"MyEC2Instance":{"Type":"AWS::EC2::Instance","Properties":{"ImageId":"ami-0ff8a91507f77f867"}}}}', """
            insert_command = insert_command  + """'""" + str(time_of_record) + """'::timestamp, 'UPDATE'),"""
        final_insert_cmd = insert_command[0:-1] + ";"
        total_inserted += batch_size
        print("added " + str(total_inserted) + " udpate records for arn " + arn)
        cur.execute(final_insert_cmd)
        con.commit()

def setup():
    # clean up 
    cur.execute('''DROP TABLE IF EXISTS changes''')
    cur.execute('''DROP TYPE IF EXISTS OPERATION''')
    con.commit()
    # insert CREATE records
    cur.execute('''CREATE TYPE  OPERATION AS ENUM ('CREATE', 'UPDATE', 'DELETE');''')
    cur.execute('''CREATE TABLE  changes (
        ARN TEXT,
        RESOURCE JSON NOT NULL,
        EVENT_TIME TIMESTAMP NOT NULL,
        RECORD_TYPE OPERATION NOT NULL
    );''')
    con.commit()

total_num_of_resources = 200000

def generate_create_records():
    batch_size = 100
    number_of_batches = int(total_num_of_resources/batch_size)
    for j in range(number_of_batches):
        # insert in batches of 50
        insert_command = """INSERT into changes VALUES """
        for i in range(batch_size):
            arn = "arn" + str(j*batch_size + i)
            time_of_record = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            insert_command = insert_command  + """('""" + arn + """' , '{"Resources":{"MyEC2Instance":{"Type":"AWS::EC2::Instance","Properties":{"ImageId":"ami-0ff8a91507f77f867"}}}}', """
            insert_command = insert_command  + """'""" + str(time_of_record) + """'::timestamp, 'CREATE'),"""
        final_insert_cmd = insert_command[0:-1] + ";"
        cur.execute(final_insert_cmd)
        con.commit()
    print("inserted " + str(i) + ", "+ str(j) )

def generate_update_records():
    for i in range(total_num_of_resources):
        arn = "arn" + str(i)
        if i < 200:
            insert_update_records_for_arn(arn, 43200)
        elif i < 32000:
            insert_update_records_for_arn(arn, 720)
        elif i < 132000:
            insert_update_records_for_arn(arn, 30)
        elif i < 192000:
            insert_update_records_for_arn(arn, 12)
        elif i < 200000:
            insert_update_records_for_arn(arn, 4)


setup
generate_create_records
generate_update_records
con.close()

