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
        insert_query = "INSERT INTO equipment_data_json (json_data, Date) VALUES (%s, %s)"
        cursor.execute(insert_query, (json.dumps(data), current_date))
        connection.commit()
        cursor.close()
        connection.close()

def insert_into_sap_hana1(data, record_id=None):
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

def insert_into_sap_hana2(data, record_id=None):
    try:
        json_data = json.dumps(data)
        xsjs_url = "https://atachiqaz3rujcsu8g.us1.hana.ondemand.com/Adcock1/xsds/AdCock/Strips/THTCaliberationDataCaptureIntoHana.xsjs"
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

def parse_data_format_one(serial_data):
    parsed_data = {}
    print("Parsing data:")
    print(serial_data) 
    # Define patterns to extract various fields from the provided format
    
    patterns = {
        'hid_number': r'HID-Number:\s*(\d+)',
        'serial_no': r'Serial number:\s*(\d+)',
        'date': r'Date:\s*([\d\.]+)',
        'time': r'Time:\s*([\d:]+)',
        'hardness_last_calibration_date': r'Hlast calibration date:\s*([\d\.]+)',
        'hardness_last_calibration_weight': r'last calibration with:\s*([\d\.]+)\s*kg',
        'hardness_newtonfactor': r'Newtonfactor:\s*([\d\.]+)',
        'diameter_last_calibration_date': r'DIAMETER/THICKNESS\s*Hlast calibration date:\s*([\d\.]+)',
        'diameter_last_calibration_size': r'last calibration with:\s*([\d\.]+)\s*mm',
        'diameter_new_calibration_date': r'HCalibration date:\s*([\d\.]+)',
        'diameter_calibration_gauge_nominal': r'Calibr\. gauge:\s*Nominal:\s*([\d\.]+)\s*mm',
        'diameter_calibration_gauge_actual': r'Nominal:\s*[\d\.]+\s*mm\s*Actual:\s*([\d\.]+)\s*mm',
        'hardness_check_date': r'last instrument check:\s*([\d\.]+)',
        'hardness_check_control_weight_nominal': r'Control weight\s*Nominal:\s*([\d\.]+)\s*kg',
        'hardness_check_control_weight_actual': r'Actual:\s*([\d\.]+)\s*kg',
        'diameter_check_control_gauge_nominal': r'Control gauge\s*Nominal:\s*([\d\.]+)\s*mm',
        'diameter_check_control_gauge_actual': r'Actual:\s*([\d\.]+)\s*mm',
    }

    # Extract basic fields based on patterns
    for key, pattern in patterns.items():
        match = re.search(pattern, serial_data)
        parsed_data[key] = match.group(1).strip() if match else None

    # Extract control weights and gauges
    control_weights = re.findall(r'Control weight\s*Nominal:\s*([\d\.]+)\s*kg\s*Actual:\s*([\d\.]+)\s*kg', serial_data)
    parsed_data['control_weights'] = [{'nominal': w[0], 'actual': w[1]} for w in control_weights]

    control_gauges = re.findall(r'Control gauge\s*Nominal:\s*([\d\.]+)\s*mm\s*Actual:\s*([\d\.]+)\s*mm', serial_data)
    parsed_data['control_gauges'] = [{'nominal': g[0], 'actual': g[1]} for g in control_gauges]

    return parsed_data
    
def parse_data_format_two(serial_data):
    parsed_data = {}
    # Assuming the second format follows a simpler pattern
    pattern = r"(\d+\.\d+)\s*;\s*(\d+\.\d+)\s*;\s*(\d+)"
    match = re.search(pattern, serial_data)

    if match:
        parsed_data['thickness'] = match.group(1)
        parsed_data['diameter'] = match.group(2)
        parsed_data['hardness'] = match.group(3)
    else:
        parsed_data['error'] = "Unable to parse data"

    return parsed_data
    

def handle_client(client_socket, equipment_id):
    try:
        data = client_socket.recv(1024).decode('utf -8')
        if data:
            # Check the format of the incoming data
            print(data)
            if "CALIBRATION:" in data:  # Assuming this indicates the first format
                parsed_data = parse_data_format_one(data)
                insert_into_sap_hana2(json_data)
            else:  # Assuming this indicates the second format
                parsed_data = parse_data_format_two(data)
                insert_into_sap_hana1(json_data)

            if 'error' not in parsed_data:
                json_data = {
                    "Equipment_ID": equipment_id,
                    "Data": parsed_data
                }
                #insert_into_mysql(json_data)
                #insert_into_sap_hana1(json_data)  # Uncomment if you want to send data to SAP HANA
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
    last_processed = None  # Variable to track the last processed message
    RawData = []

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
                        
                        # Print the raw data received
                        raw_data = data.decode('utf-8')
                        raw_data= raw_data.strip()
                        print(f"Raw data received: {raw_data}")

                        if raw_data.strip() == "":
                            
                            print("Received empty data, continuing...")
                            continue  # Skip to the next iteration if data is empty    
                        buffer += data.decode('utf-8')        
                        # Check if the data contains "ERWEKA TBH125 CALIBRATION"
                        if not (raw_data and raw_data[0].isdigit()):
                            print("Calibration detected, sending calibration data...")
                            print("we r in erweka")
                            while "Signature:" in buffer:
                                approved_by_index = buffer.find("Signature:")
                                next_line_index = buffer.find("\n", approved_by_index)
                                if next_line_index != -1:
                                    # Extract full message between two "Signature:" markers
                                    full_message = buffer[:next_line_index + 1]
                                    buffer = buffer[next_line_index + 1:]  # Remaining data
                                    
                                    # Debug print the extracted full message
                                    print(f"Full message received:\n{full_message}")
                                    
                                    # Check if this message has already been processed
                                    if full_message == last_processed:
                                        print("Duplicate message detected. Skipping insertion.")
                                        continue

                                    # Parse and insert data
                                    parsed_data = parse_data_format_one(full_message)
                                    if parsed_data:
                                        json_data = {
                                            "Equipment_ID": equipment_id,
                                            "Date": parsed_data['date'],
                                            "Data": parsed_data
                                        }
                                        insert_into_mysql(json_data)
                                        # Uncomment below to insert into SAP HANA as well
                                        insert_into_sap_hana2(json_data)

                                        # Mark this message as processed
                                        last_processed = full_message
                                else:
                                    break  # Wait for more data if newline isn't found
                        else:
                            if '\n' in buffer:
                                lines = buffer.split('\n')
                                for line in lines[:-1]:
                                    parsed_data = parse_data_format_two(line)
                                    if 'error' not in parsed_data:
                                        json_data = {
                                            "Equipment_ID": equipment_id,
                                            "Data": parsed_data
                                        }
                                        insert_into_mysql(json_data)
                                        # Uncomment if you want to send data to SAP HANA
                                        insert_into_sap_hana1(json_data)
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