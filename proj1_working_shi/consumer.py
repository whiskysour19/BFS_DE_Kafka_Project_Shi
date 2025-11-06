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
import random
import string
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from producer import employee_topic_name #you do not want to hard copy it

class SalaryConsumer(Consumer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host: str = "localhost", port: str = "29092", group_id: str = ''):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        
        #self.consumer = Consumer(self.conf)
        self.keep_runnning = True
        self.group_id = group_id

    def consume(self, topics, processing_func):
        # Main consumer loop - polls messages and processes them
        try:
            self.subscribe(topics)
            while self.keep_runnning:
                # Poll with 1 second timeout to balance responsiveness and CPU usage
                msg = self.poll(timeout=1.0)

                if msg is None:
                    # No message available, continue polling
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Reached end of partition - informational, not an error
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # Valid message received - process it
                    print(f'Processing message: {msg.value()}')
                    processing_func(msg)
        finally:
            # Close down consumer to commit final offsets.
            self.close()

#or can put all functions in a separte file and import as a module
class ConsumingMethods:
    @staticmethod
    def add_salary(msg):
        # Deserialize JSON message to Employee object
        e = Employee(**(json.loads(msg.value())))
        try:
            # Connect to PostgreSQL database
            conn = psycopg2.connect(
                #use localhost if not run in Docker
                host="0.0.0.0",
                database="postgres",
                user="postgres",
                port = '5432',
                password="postgres")
            conn.autocommit = True  # Auto-commit for simplicity
            cur = conn.cursor()
            
            # Create table if not exists - idempotent operation
            # Use department as PRIMARY KEY to ensure uniqueness
            cur.execute("""
                CREATE TABLE IF NOT EXISTS department_employee_salary (
                    department VARCHAR(50) PRIMARY KEY,
                    total_salary BIGINT DEFAULT 0
                )
            """)
            
            # Upsert pattern: Insert new dept or add salary to existing dept
            # ON CONFLICT handles concurrent writes and aggregates salary per department
            # This approach maintains running totals without needing to pre-aggregate
            cur.execute(f"""
                INSERT INTO department_employee_salary (department, total_salary) 
                VALUES ('{e.emp_dept}', {int(float(e.emp_salary))}) 
                ON CONFLICT(department) 
                DO UPDATE SET total_salary = department_employee_salary.total_salary + {int(float(e.emp_salary))}
            """)
            
            print(f"Added {e.emp_salary} to department {e.emp_dept}")
            cur.close()
        except Exception as err:
            # Log errors but continue processing - ensures one bad message doesn't stop consumer
            print(f"Error processing message: {err}")

if __name__ == '__main__':
    # Use specific group_id to enable consumer group management and offset tracking
    consumer = SalaryConsumer(group_id="employee_consumer_salary")
    # Start consuming from the specified topic and process with add_salary function
    consumer.consume([employee_topic_name], ConsumingMethods.add_salary)