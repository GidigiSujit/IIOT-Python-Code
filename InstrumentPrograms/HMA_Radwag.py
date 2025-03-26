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
        xsjs_url = "https://atachiqaz3rujcsu8g.us1.hana.ondemand.com/Adcock1/xsds/AdCock/Strips/HMADataCaptureIntoHana2.xsjs"
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

    # Trim the data to include only the relevant section from 'Drying process report' to 'Signature'
    start_marker = r"Drying process report"
    end_marker = r"Signature"
    
    # Extract the section of the data between these markers
    data_section = re.search(rf"{start_marker}(.*?){end_marker}", serial_data, re.DOTALL)
    
    if data_section:
        data_section = data_section.group(1).strip()

        # Define patterns to extract relevant fields within the section
        patterns = {
            'start_date': r'Start date\s+(\d{2}\.\d{2}\.\d{4})',
            'start_time': r'Start time\s+([\d:]+)',
            'instrument_id': r'Instrument ID:\s*(\w+-\d+)',
            'start_mass': r'Start mass\s+([\d\.]+)\s*g',
            'end_mass': r'End mass\s+([\d\.]+)\s*g',
            'drying_time': r'Drying time\s+([\d:]+)',
            'result_percent': r'Result\s+([\d\.]+)\s*%M',
            'status': r'Status\s+([\w]+)',
            'drying_profile': r'Drying profile\s+[A-Za-z0-9\s]*?(\d{2,3}[°?]C)',
            'finish_mode': r'Finish mode\s+([A-Za-z0-9\s]+)',
            'interval': r'Interval\s+([\d]+)s',
            'variable': r'Variable \d+\s+([A-Za-z0-9\s]+):\s*([\w-]+)'
        }
        
        # Extract the fields using regular expressions
        for key, pattern in patterns.items():
            match = re.search(pattern, data_section)
            parsed_data[key] = match.group(1).strip() if match else None

        # Extract additional variables if available
        additional_variables = re.findall(r'Variable \d+\s+([A-Za-z0-9\s]+):\s*([\w-]+)', data_section)
        parsed_data['additional_variables'] = {var[0]: var[1] for var in additional_variables}

    else:
        parsed_data['error'] = "Relevant data section not found."

    return parsed_data


def handle_client(client_socket, equipment_id):
    try:
        data = client_socket.recv(1024).decode('utf-8')
        if data:
            parsed_data = parse_data(data)
            if parsed_data:
                json_data = {
                    "Equipment_ID": equipment_id,
                    "Date": parsed_data['date'],
                    "Data": parsed_data
                }
                insert_into_mysql(json_data)
                insert_into_sap_hana(json_data)
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
                        
                        # Process data only when the section starts with "Drying process report" and ends with "Signature"
                        while "Drying process report" in buffer and "Signature" in buffer:
                            start_index = buffer.find("Drying process report")
                            end_index = buffer.find("Signature", start_index) + len("Signature")
                            
                            if end_index != -1:
                                full_message = buffer[start_index:end_index]
                                buffer = buffer[end_index:]  # Keep remaining data
                                
                                # Parse and process the extracted message
                                parsed_data = parse_data(full_message)
                                if parsed_data:
                                    json_data = {
                                        "Equipment_ID": equipment_id,
                                        "Date": parsed_data.get('start_date', 'Unknown'),
                                        "Data": parsed_data
                                    }
                                    insert_into_mysql(json_data)
                                    # Uncomment the next line to insert into SAP HANA as well
                                    insert_into_sap_hana(json_data)
                            else:
                                break  # Wait for more data if "Signature" isn't fully received
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
