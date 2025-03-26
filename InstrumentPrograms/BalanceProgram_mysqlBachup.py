import json
import mysql.connector
from mysql.connector import Error, pooling
import socket
import threading
import time
import re
from datetime import datetime  # Import datetime to handle current date and time
import requests

# Global connection pool
connection_pool = None
config = None

def read_config():
    global config
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    return config

def create_connection_pool(db_config, pool_size=1):
    global connection_pool
    try:
        connection_pool = pooling.MySQLConnectionPool(
            pool_name="my_pool",
            pool_size=pool_size,
            **db_config
        )
        print("Connection pool created successfully")
    except Error as e:
        print(f"Error creating connection pool: {e}")

def get_connection():
    global connection_pool
    try:
        return connection_pool.get_connection()
    except Error as e:
        print(f"Error getting connection from pool: {e}")
        return None

def create_table_if_not_exists():
    connection = get_connection()
    if connection:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS equipment_data_json (
            id INT AUTO_INCREMENT PRIMARY KEY,
            json_data JSON,
            inserted_to_hana TINYINT DEFAULT 0,
            insert_time DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()

def insert_into_mysql(data):
    connection = get_connection()
    if connection:
        cursor = connection.cursor()
        insert_query = "INSERT INTO equipment_data_json (json_data) VALUES (%s)"
        cursor.execute(insert_query, (json.dumps(data),))
        connection.commit()
        cursor.close()
        connection.close()

def insert_into_sap_hana(data, record_id=None):
    try:
        json_data = json.dumps(data)  # Convert the data to JSON format

        # URL of your XSJS service
        xsjs_url = "https://atachiqaz3rujcsu8g.us1.hana.ondemand.com/Adcock1/xsds/AdCock/AutoWeightDataCaptureIntoHana2.xsjs"

        # Headers for the request
        headers = {
            'Content-Type': 'application/json'
        }

        # Send the POST request with the JSON data
        response = requests.post(xsjs_url, headers=headers, data=json_data)

        # Check if the request was successful
        if response.status_code == 200 or response.status_code == 201:
            print(f"JSON data inserted successfully into SAP HANA: {json_data}")
            # If record_id is provided, update the MySQL record to mark it as inserted into SAP HANA
            if record_id:
                mark_as_inserted_to_hana(record_id)
        else:
            print(f"Failed to insert data into SAP HANA. Status code: {response.status_code}, Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"Error inserting data into SAP HANA: {e}")

def mark_as_inserted_to_hana(record_id):
    connection = get_connection()
    if connection:
        cursor = connection.cursor()
        update_query = "UPDATE equipment_data_json SET inserted_to_hana = 1 WHERE id = %s"
        cursor.execute(update_query, (record_id,))
        connection.commit()
        cursor.close()
        connection.close()

def handle_client(client_socket, equipment_id):
    try:
        data = client_socket.recv(1024).decode('utf-8')
        if data:
            # Clean the data: Remove '\r\n' and ignore standalone '0's
            cleaned_data = re.sub(r'(^|\D)0($|\D)', '', data).strip('\r\n')
            # Only process if there's meaningful data after cleaning
            if cleaned_data:
                try:
                    # Create a JSON object with Equipment_ID, Date, and Data
                    json_data = {
                        "Equipment_ID": equipment_id,
                        "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "Data": cleaned_data
                    }
                    insert_into_mysql(json_data)
                    insert_into_sap_hana(json_data)
                except json.JSONDecodeError:
                    print(f"Received invalid JSON data: {cleaned_data}")
    finally:
        client_socket.close()

def run_server(port, equipment_id):
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen(1)
        print(f"Listening on port {port}")
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Accepted connection from {addr}")
            threading.Thread(target=handle_client, args=(client_socket, equipment_id)).start()
    except Exception as e:
        print(f"Failed to bind to port {port}: {e}")

def send_data_to_port(port, ip, equipment_id):
    buffer = ''
    while True:
        try:
            # Create a socket object
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(20)  # Timeout of 20 seconds
                try:
                    # Connect to the instrument
                    s.connect((ip, port))
                    print(f"Connected to {ip}:{port}")
                except socket.error as connect_error:
                    print(f"Failed to connect: {connect_error}. Retrying in 1 second...")
                    time.sleep(1)
                    continue

                # Read data from the instrument
                while True:
                    try:
                        data = s.recv(4096)  # Buffer size of 4096 bytes
                        if not data:
                            print("No data received. Connection might be closed. Reconnecting...")
                            break
                        
                        # Accumulate the received data
                        buffer += data.decode('utf-8')
                        
                        # Process each line of data if newline character is found
                        if '\n' in buffer:
                            lines = buffer.split('\n')
                            for line in lines[:-1]:  # Process all complete lines
                                cleaned_data = line.replace('\r', '').strip()  # Remove carriage return and extra spaces
                                if cleaned_data:
                                    try:
                                        # Create a JSON object with Equipment_ID, Date, and Data
                                        json_data = {
                                            "Equipment_ID": equipment_id,
                                            "Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                            "Data": cleaned_data
                                        }
                                        insert_into_mysql(json_data)
                                        insert_into_sap_hana(json_data)
                                    except ValueError:
                                        print(f"Invalid data format received: {cleaned_data}")
                            # Keep the last incomplete line in the buffer
                            buffer = lines[-1]
                        
                    except socket.timeout:
                        print("Timed out while waiting for data. Reconnecting...")
                        break
                    except socket.error as read_error:
                        print(f"Read error: {read_error}. Reconnecting...")
                        break
 
        except socket.error as main_error:
            print(f"Socket error: {main_error}. Retrying in 1 second...")
            time.sleep(1)  # Wait for 1 second before retrying

def check_connection_pool():
    while True:
        connection = get_connection()
        if connection:
            connection.close()
            print("Connection pool is working")
        else:
            print("Connection pool is not working, attempting to recreate")
            create_connection_pool(config['database'])
        time.sleep(60)  # Check every 60 seconds

def main():
    global config
    config = read_config()
    create_connection_pool(config['database'])
    create_table_if_not_exists()
    
    # Start connection pool checker thread
    threading.Thread(target=check_connection_pool, daemon=True).start()
    
    # Start server threads for each configured port
    server_threads = []
    for equipment in config['equipment_data']:
        port = equipment['Port_Number']
        ip_address = equipment['IP_Address']
        equipment_id = equipment['Equipment_ID']
        thread = threading.Thread(target=send_data_to_port, args=(port, ip_address, equipment_id))
        thread.daemon = True
        thread.start()
        server_threads.append(thread)
    
    # Keep the main thread alive to allow all threads to run
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Simulation stopped.")

if __name__ == "__main__":
    main()
