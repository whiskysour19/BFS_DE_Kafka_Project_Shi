"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""


import json
import psycopg2
from confluent_kafka import Consumer, KafkaError
from employee import Employee
from producer import employee_topic_name

class cdcConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        """
        Standard Kafka consumer loop: poll messages and process them.
        timeout=1.0 prevents blocking indefinitely when no messages available.
        """
        try:
            self.subscribe(topics)
            while self.keep_runnning:
                msg = self.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Consumer error: {msg.error()}")
                        break
                processing_func(msg)
        finally:
            self.close()

def update_dst(msg):
    """
    Apply CDC changes to destination database based on action type.
    Replicates INSERT/UPDATE/DELETE operations from source to target.
    """
    e = Employee(**(json.loads(msg.value())))
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            port='5433',  # Destination database port (different from source)
            password="postgres")
        conn.autocommit = True
        cur = conn.cursor()
        
        # Ensure target table exists (idempotent)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                emp_id INT PRIMARY KEY,
                emp_FN VARCHAR(50),
                emp_LN VARCHAR(50),
                emp_dob DATE,
                emp_city VARCHAR(50)
            )
        """)
        
        # Apply changes based on action type from CDC record
        # This implements the core replication logic
        if e.action == 'insert':
            cur.execute("""
                INSERT INTO employees (emp_id, emp_FN, emp_LN, emp_dob, emp_city)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (emp_id) DO NOTHING
            """, (e.emp_id, e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city))
        elif e.action == 'update':
            cur.execute("""
                UPDATE employees
                SET emp_FN = %s, emp_LN = %s, emp_dob = %s, emp_city = %s
                WHERE emp_id = %s
            """, (e.emp_FN, e.emp_LN, e.emp_dob, e.emp_city, e.emp_id))
        elif e.action == 'delete':
            cur.execute("""
                DELETE FROM employees WHERE emp_id = %s
            """, (e.emp_id,))
        
        print(f"Applied {e.action} for employee {e.emp_id}")
        cur.close()
        conn.close()
    except Exception as err:
        print(f"Error updating destination: {err}")

if __name__ == '__main__':
    consumer = cdcConsumer(group_id='cdc_consumer_group')
    consumer.consume([employee_topic_name], update_dst)