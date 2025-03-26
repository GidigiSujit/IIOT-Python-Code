import json
import mysql.connector
from mysql.connector import Error, pooling
import socket
import threading
import time
import re
import requests
from datetime import datetime

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
            Date DATETIME,
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
        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_query = "INSERT INTO equipment_data_json (json_data,Date) VALUES (%s, %s)"
        cursor.execute(insert_query, (json.dumps(data),current_date))
        connection.commit()
        cursor.close()
        connection.close()

def insert_into_sap_hana(data, record_id=None):
    try:
        json_data = json.dumps(data)
        xsjs_url = "https://atachiqaz3rujcsu8g.us1.hana.ondemand.com/Adcock1/xsds/AdCock/Strips/THTDataCaptureIntoHana2.xsjs"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(xsjs_url, headers=headers, data=json_data)
        if response.status_code == 200 or response.status_code == 201:
            print(f"JSON data inserted successfully into SAP HANA: {json_data}")
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

def parse_data(serial_data):
    parsed_data = {}

    # Assuming each data line follows the pattern: thickness; diameter; hardness
    pattern = r"(\d+\.\d+)\s*;\s*(\d+\.\d+)\s*;\s*(\d+)"

    match = re.search(pattern, serial_data)

    if match:
        # Extract the three values
        thickness = match.group(1)
        diameter = match.group(2)
        hardness = match.group(3)

        # Store them in the parsed_data dictionary (directly)
        parsed_data['thickness'] = thickness
        parsed_data['diameter'] = diameter
        parsed_data['hardness'] = hardness
    else:
        parsed_data['error'] = "Unable to parse data"

    return parsed_data




def handle_client(client_socket, equipment_id):
    try:
        data = client_socket.recv(1024).decode('utf-8')
        if data:
            parsed_data = parse_data(data)
            if 'error' not in parsed_data:
                json_data = {
                    "Equipment_ID": equipment_id,
                    "Data": parsed_data
                }
                insert_into_mysql(json_data)
                # Uncomment if you want to send data to SAP HANA
                insert_into_sap_hana(json_data)
            else:
                print(f"Error parsing client data: {parsed_data['error']}")
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
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(20)
                try:
                    s.connect((ip, port))
                    print(f"Connected to {ip}:{port}")
                except socket.error as connect_error:
                    print(f"Failed to connect: {connect_error}. Retrying in 1 second...")
                    time.sleep(1)
                    continue

                while True:
                    try:
                        data = s.recv(4096)
                        if not data:
                            print("No data received. Connection might be closed. Reconnecting...")
                            break
                        
                        buffer += data.decode('utf-8')
                        if '\n' in buffer:
                            lines = buffer.split('\n')
                            for line in lines[:-1]:
                                parsed_data = parse_data(line)
                                if 'error' not in parsed_data:
                                    json_data = {
                                        "Equipment_ID": equipment_id,
                                        "Data": parsed_data
                                    }
                                    insert_into_mysql(json_data)
                                    # Uncomment if you want to send data to SAP HANA
                                    insert_into_sap_hana(json_data)
                                else:
                                    print(f"Error parsing data: {parsed_data['error']}")
                            buffer = lines[-1]
                    except socket.timeout:
                        print("Timed out while waiting for data. Reconnecting...")
                        break
                    except socket.error as read_error:
                        print(f"Read error: {read_error}. Reconnecting...")
                        break
        except socket.error as main_error:
            print(f"Socket error: {main_error}. Retrying in 1 second...")
            time.sleep(1)

def check_connection_pool():
    while True:
        connection = get_connection()
        if connection:
            connection.close()
            print("Connection pool is working")
        else:
            print("Connection pool is not working, attempting to recreate")
            create_connection_pool(config['database'])
        time.sleep(60)

def main():
    global config
    config = read_config()
    create_connection_pool(config['database'])
    create_table_if_not_exists()
    
    threading.Thread(target=check_connection_pool, daemon=True).start()
    
    server_threads = []
    for equipment in config['equipment_data']:
        port = equipment['Port_Number']
        ip_address = equipment['IP_Address']
        equipment_id = equipment['Equipment_ID']
        thread = threading.Thread(target=send_data_to_port, args=(port, ip_address, equipment_id))
        thread.daemon = True
        thread.start()
        server_threads.append(thread)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Simulation stopped.")

if __name__ == "__main__":
    main()
