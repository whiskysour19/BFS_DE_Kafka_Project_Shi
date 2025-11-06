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

import time
from confluent_kafka import Producer
from employee import Employee
from confluent_kafka.serialization import StringSerializer
import psycopg2

employee_topic_name = "bf_employee_cdc"

class cdcProducer(Producer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
        self.running = True
        # Track last processed action_id to avoid reprocessing records
        # Using in-memory variable for simplicity (resets on restart)
        self.last_processed_id = 0
        self._init_database()
    
    def _init_database(self):
        """
        Initialize database tables and triggers automatically on startup.
        Uses IF NOT EXISTS for idempotency - safe to run multiple times.
        This eliminates the need for separate SQL initialization scripts.
        """
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port='5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            
            # Source table: stores employee data
            cur.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    emp_id INT PRIMARY KEY,
                    emp_FN VARCHAR(50),
                    emp_LN VARCHAR(50),
                    emp_dob DATE,
                    emp_city VARCHAR(50)
                )
            """)
            
            # CDC log table: automatically populated by triggers
            # action_id (SERIAL) provides natural ordering for offset tracking
            cur.execute("""
                CREATE TABLE IF NOT EXISTS emp_cdc (
                    action_id SERIAL PRIMARY KEY,
                    emp_id INT,
                    emp_FN VARCHAR(50),
                    emp_LN VARCHAR(50),
                    emp_dob DATE,
                    emp_city VARCHAR(50),
                    action VARCHAR(10)
                )
            """)
            
            # Trigger function: captures INSERT/UPDATE/DELETE operations
            # Automatically writes change records to emp_cdc table
            cur.execute("""
                CREATE OR REPLACE FUNCTION log_employee_changes()
                RETURNS TRIGGER AS $$
                BEGIN
                    IF TG_OP = 'INSERT' THEN
                        INSERT INTO emp_cdc (emp_id, emp_FN, emp_LN, emp_dob, emp_city, action)
                        VALUES (NEW.emp_id, NEW.emp_FN, NEW.emp_LN, NEW.emp_dob, NEW.emp_city, 'insert');
                        RETURN NEW;
                    ELSIF TG_OP = 'UPDATE' THEN
                        INSERT INTO emp_cdc (emp_id, emp_FN, emp_LN, emp_dob, emp_city, action)
                        VALUES (NEW.emp_id, NEW.emp_FN, NEW.emp_LN, NEW.emp_dob, NEW.emp_city, 'update');
                        RETURN NEW;
                    ELSIF TG_OP = 'DELETE' THEN
                        INSERT INTO emp_cdc (emp_id, emp_FN, emp_LN, emp_dob, emp_city, action)
                        VALUES (OLD.emp_id, OLD.emp_FN, OLD.emp_LN, OLD.emp_dob, OLD.emp_city, 'delete');
                        RETURN OLD;
                    END IF;
                END;
                $$ LANGUAGE plpgsql;
            """)
            
            # Attach trigger to employees table
            # AFTER trigger ensures data is committed before logging
            cur.execute("""
                DROP TRIGGER IF EXISTS employee_cdc_trigger ON employees;
                CREATE TRIGGER employee_cdc_trigger
                AFTER INSERT OR UPDATE OR DELETE ON employees
                FOR EACH ROW EXECUTE FUNCTION log_employee_changes();
            """)
            
            cur.close()
            conn.close()
            print("Database initialized successfully")
        except Exception as err:
            print(f"Database initialization error: {err}")
    
    def fetch_cdc(self):
        """
        Poll emp_cdc table for new records and publish to Kafka.
        Uses action_id > last_processed_id for simple offset tracking.
        Processes in batches (LIMIT 100) for efficiency.
        """
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="postgres",
                port='5432',
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            
            # Query unprocessed records using action_id as offset
            # ORDER BY ensures sequential processing
            cur.execute("""
                SELECT action_id, emp_id, emp_FN, emp_LN, emp_dob, emp_city, action
                FROM emp_cdc
                WHERE action_id > %s
                ORDER BY action_id
                LIMIT 100
            """, (self.last_processed_id,))
            
            records = cur.fetchall()
            encoder = StringSerializer('utf-8')
            
            for record in records:
                action_id, emp_id, emp_FN, emp_LN, emp_dob, emp_city, action = record
                employee = Employee(action_id, emp_id, emp_FN, emp_LN, str(emp_dob), emp_city, action)
                json_data = employee.to_json()
                
                # Send to Kafka topic
                self.produce(employee_topic_name, encoder(json_data))
                # Update offset after successful produce
                self.last_processed_id = action_id
            
            # Flush to ensure messages are sent immediately
            self.flush()
            
            cur.close()
            conn.close()
            
            return len(records)
        except Exception as err:
            print(f"Error fetching CDC: {err}")
            return 0

if __name__ == '__main__':
    producer = cdcProducer()
    
    # Continuous polling loop: scan for new CDC records
    # Sleep when no records found to avoid excessive CPU usage
    while producer.running:
        count = producer.fetch_cdc()
        if count == 0:
            time.sleep(0.5)  # Back off when no new records
        else:
            print(f"Processed {count} CDC records")
    
