import pdfplumber
import re
import json
import requests
import mysql.connector
from datetime import datetime
import os

# Step 1: Extract text from PDF
def extract_text_from_pdf(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        text = ""
        for page in pdf.pages:
            text += page.extract_text() + "\n"  # Add newline to separate pages
    return text

# Step 2: Parse the extracted text
def parse_pdf_data(text, file_name):
    # Parsing Kloudface Details
    brand_name = re.search(r"Brand Name\s*:\s*(.+)", text)
    model_no = re.search(r"Model No\.\s*:\s*(\S+)", text)
    kloudface_id = re.search(r"ID\s*:\s*(\d+)", text)
    serial_no = re.search(r"Sr\.No\.\s*:\s*(\d+)", text)
    firmware_version = re.search(r"Firmware Ver\.\s*:\s*(\S+)", text)
    company_name = re.search(r"Company Name\s*:\s*(.+)", text)
    department = re.search(r"Department\s*:\s*(.+)", text)


    # Parsing SieveShaker Details
    sieve_model_name = re.search(r"Model Name\s*:\s*(.+)", text)
    instrument_id = re.search(r"Instrument ID\s*:\s*(\d+)", text)
    sieve_serial_no = re.search(r"Serial Number\s*:\s*(\d+)", text)
    sieve_firmware_version = re.search(r"Firmware Ver\.\s*:\s*(\S+)", text)

    # Parsing User Details
    user_name = re.search(r"User Name\s*:\s*(.+)", text)
    role_name = re.search(r"Role Name\s*:\s*(.+)", text)
    group_name = re.search(r"Group Name\s*:\s*(.+)", text)


    # Parsing Product Details
    product_name = re.search(r"Product Name\s*:\s*(.+)", text)
    product_weight = re.search(r"Product Weight\s*:\s*([\d.]+)", text)
    product_power = re.search(r"Product Power\s*:\s*(\d+)", text)
    product_time = re.search(r"Product Time\s*:\s*(\d+)", text)
    product_mode = re.search(r"Product Mode\s*:\s*(\S+)", text)
    product_dia = re.search(r"Product Dia\s*:\s*(\d+)", text)
    sieve_count = re.search(r"No\.of Sieves\s*:\s*(\d+)", text)


    # Parsing Product Run Details
    batch_no = re.search(r"Batch No\s*:\s*(.+)", text)
    batch_lot = re.search(r"Batch Lot\s*:\s*(\S+)", text)
    
    sieve_start_time = re.search(r"Sieve Start Time\s*:\s*(\S+)", text)
    sieve_end_time = re.search(r"Sieve End Time\s*:\s*(\S+)", text)

    # Extracting Test Details block
    test_details_block = re.search(r"Test Details:(.*?)-{5,}", text, re.DOTALL)
    if test_details_block:
        test_details_block = test_details_block.group(1)
        test_details = re.findall(r"^\s*(\d+|PAN)\s+([\d.]+)\s+([\d.]+)", test_details_block, re.MULTILINE)
    else:
        test_details = []

    # Extracting Result Details block
    result_details_block = re.search(r"Result Details:(.*)", text, re.DOTALL)
    if result_details_block:
        result_details_block = result_details_block.group(1)
        result_details = re.findall(r"^\s*(\d+|PAN)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)", result_details_block, re.MULTILINE)
    else:
        result_details = []

    print("Test Details:", test_details)
    print("Result Details:", result_details)
    actual_sample_weight = re.search(r"Actual Sample Weight\(SW\):\s*([\d.]+)", text)
    loss_percent = re.search(r"Loss In %\s*:\s*([\d.]+)", text)

    print(result_details)

    # Parsing Test Status
    test_status = re.search(r"Test Status:\s*(\w+)", text)
    completed_by = re.search(r"Completed by\s+(\S+)", text)

    
    parsed_data = {
        "kloudface_details": {
            "brand_name": brand_name.group(1) if brand_name else None,
            "model_no": model_no.group(1) if model_no else None,
            "id": kloudface_id.group(1) if kloudface_id else None,
            "serial_no": serial_no.group(1) if serial_no else None,
            "firmware_version": firmware_version.group(1) if firmware_version else None,
            "company_name": company_name.group(1) if company_name else None,
            "department": department.group(1) if department else None,
        },
        "sieveshaker_details": {
            "model_name": sieve_model_name.group(1) if sieve_model_name else None,
            "instrument_id": instrument_id.group(1) if instrument_id else None,
            "serial_no": sieve_serial_no.group(1) if sieve_serial_no else None,
            "firmware_version": sieve_firmware_version.group(1) if sieve_firmware_version else None,
        },
       "user_details": {
            "user_name": user_name.group(1) if user_name else None,
            "role_name": role_name.group(1) if role_name else None,
            "group_name": group_name.group(1) if group_name else None,
        },
        "product_details": {
            "product_name": product_name.group(1) if product_name else None,
            "product_weight": float(product_weight.group(1)) if product_weight else None,
            "product_power": float(product_power.group(1)) if product_power else None,
            "product_time": float(product_time.group(1)) if product_time else None,
            "product_mode": product_mode.group(1) if product_mode else None,
            "product_dia": float(product_dia.group(1)) if product_dia else None,
            "sieve_count": int(sieve_count.group(1)) if sieve_count else None,
        },
        "product_run_details": {
            "batch_no": batch_no.group(1) if batch_no else None,
            "batch_lot": batch_lot.group(1) if batch_lot else None,
            
            "sieve_start_time": sieve_start_time.group(1) if sieve_start_time else None,
            "sieve_end_time": sieve_end_time.group(1) if sieve_end_time else None,
        },
         "test_details" : [
    {
        "Sr.No.": result[0],
        "Pre(PRW)": result[1],
        "Pos(POW))": result[2],
        
    }
    for result in test_details  # Skip index 0, assuming it contains metadata
],

 "result_details" : [
    {
        "Sr.No.": result[0],
        "Retained": result[1],
        "cumulative": result[2],
        "pass": result[3]
        
    }
    for result in result_details  # Skip index 0, assuming it contains metadata
],
       
          "File Name": file_name 
    }
    
    return parsed_data

# Step 3: Print parsed data as JSON
def print_parsed_data(parsed_data):
    # Convert the parsed data dictionary to JSON format and print it
    json_data = json.dumps(parsed_data, indent=4)
    print(json_data)

# Step 4: Main function to drive the process
def main(pdf_path):
    # Extract data from PDF
    pdf_text = extract_text_from_pdf(pdf_path)

    # Parse the extracted data
    parsed_data = parse_pdf_data(pdf_text)

    file_name = os.path.basename(pdf_path)
    parsed_data["file_name"] = file_name

    # Print the parsed data in JSON format
    print_parsed_data(parsed_data)
    insert_data_to_db(parsed_data,file_name)

def get_connection():
    try:
        # Attempt to establish a connection to the MySQL database
        connection = mysql.connector.connect(
            host="127.0.0.1",  # Change to your MySQL server address
            user="root",
            password="root",
            database="equipment_monitoring"
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

# Step 3: Check if the file already exists in the database
def is_file_in_db(file_name):
    try:
        connection = get_connection()
        if connection is None:
            print("Failed to connect to the database.")
            return False
        
        cursor = connection.cursor()
        # Query to check if file_name exists in the table
        check_query = "SELECT COUNT(*) FROM equipment_data_json WHERE fileName = %s"
        cursor.execute(check_query, (file_name,))
        (count,) = cursor.fetchone()
        
        return count > 0

    except mysql.connector.Error as err:
        print(f"Error checking file: {err}")
        return False

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()



# Step 4: Insert parsed data into the database
def insert_data_to_db(parsed_data, file_name):
    try:
        connection = get_connection()
        if connection is None:
            print("Failed to connect to the database.")
            return

        cursor = connection.cursor()
        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_query = "INSERT INTO equipment_data_json (json_data, Date, fileName) VALUES (%s, %s, %s)"
        json_data = json.dumps(parsed_data)
        

        # Pass the parameters in the correct order
        cursor.execute(insert_query, (json_data, current_date, file_name))
        connection.commit()
        print(f"Data and file name inserted successfully for file: {file_name}")

    except mysql.connector.Error as err:
        print(f"Error inserting data: {err}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

                    


def insert_into_sap_hana(data, record_id=None):
    try:
        json_data = json.dumps(data)
        xsjs_url = "https://atachiqaz3rujcsu8g.us1.hana.ondemand.com/Adcock1/xsds/AdCock/Strips/EMSDataCaptureIntoHana.xsjs"
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
                
# Main function to process each file in the folder
def process_files_in_folder(folder_path):
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".pdf"):
            # Check if file already exists in the database
            if not is_file_in_db(file_name):
                print(f"Processing new file: {file_name}")
                pdf_path = os.path.join(folder_path, file_name)
                pdf_text = extract_text_from_pdf(pdf_path)
                parsed_data = parse_pdf_data(pdf_text, file_name)
                insert_data_to_db(parsed_data,file_name)
                insert_into_sap_hana(parsed_data)

            else:
                print(f"File {file_name} is already processed. Skipping.")

# Sample PDF path
folder_path = r"C:\Users\Atachi.IND052\Downloads\Sample PDFs2\SamplePDFs\ElwctromagneticSeiveShaker"
process_files_in_folder(folder_path)