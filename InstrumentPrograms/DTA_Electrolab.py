import pdfplumber
import re
import json
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
def parse_pdf_data(text,file_name):
    # Parsing Kloudface Details
    brand_name = re.search(r"Company Name\s*:\s*(.+)", text)
    model_no = re.search(r"Model No\.\s*:\s*(\S+)", text)
    serial_no = re.search(r"Serial No\.\s*:\s*(\S+)", text)
    firmware_version = re.search(r"Firmware Ver\.\s*:\s*(\S+)", text)
    instrument_id = re.search(r"Instrument ID\.\s*:\s*(\S+)", text)
    department = re.search(r"Department\s*:\s*(\S+)", text)
    companyName = re.search(r"Company Name\s*:\s*(\S+)", text)

    # Parsing Instrument Details
    instrument_model = re.search(r"Model No\.\s*:\s*(\S+)", text)
    instrument_serial_no = re.search(r"Serial No\.\s*:\s*(\S+)", text)
    instrument_firmware_version = re.search(r"Firmware Ver\.\s*:\s*(\S+)", text)

    # Parsing User Details
    user_name = re.search(r"User Name\s*:\s*(.+)", text)
    role_name = re.search(r"Role\s*:\s*(.+)", text)
    group_name = re.search(r"Group\s*:\s*(.+)", text)

    # Parsing Product Details
    product_name = re.search(r"Product Name\s*:\s*(.+)", text)
    product_type = re.search(r"Product Type\s*:\s*(.+)", text)
    apparatus_type = re.search(r"Apparatus Type\s*:\s*(.+)", text)
    set_temp = re.search(r"Set Temp\(deg.C\)\s*:\s*([\d.]+)", text)
    test_mode = re.search(r"Test Mode\s*:\s*(\S+)", text)

    # Parsing Test Details
    Test_arm_name = re.search(r"Test ARM Name\.?\s*:\s*(.+)", text)
    batch_no = re.search(r"Batch No\.\s*:\s*(\S+)", text)
    lot_no = re.search(r"Lot No\.\s*:\s*(\S+)", text)
    start_date = re.search(r"Start Date\s*:\s*(\S+)", text)
    start_time = re.search(r"Start Time\s*:\s*(\S+)", text)
    end_date = re.search(r"End Date\s*:\s*(\S+)", text)
    end_time = re.search(r"End Time\s*:\s*(\S+)", text)
    test_id = re.search(r"Test ID\.?\s*:\s*(.+)", text)
    test_status = re.search(r"Test Status\s*:\s*(\S+)", text)
    on_duration = re.search(r"On Duration\.?\s*:\s*(.+)", text)
    halt_duration = re.search(r"Halt Duration\.?\s*:\s*(.+)", text)

    # Parsing Time Details (Position No. and Time)
    time_details = re.findall(r"(\d+)\s+(\d{2}:\d{2}:\d{2})", text)
    time_details_parsed = [{"position_no": t[0], "time_in": t[1]} for t in time_details]

    # Parsing Remarks and Signature Fields
    remark = re.search(r"Remark\s*:\s*([^_]+)", text)
    analyzed_by = re.search(r"Analyzed By\s*:\s*(.*)", text)
    checked_by = re.search(r"Checked By\s*:\s*(.*)", text)

    # Structuring parsed data into a dictionary
    parsed_data = {
        "kloudface_details": {
            
            "brand_name": brand_name.group(1) if brand_name else None,
            "model_no": model_no.group(1) if model_no else None,
            "serial_no": serial_no.group(1) if serial_no else None,
            "firmware_version": firmware_version.group(1) if firmware_version else None,
            "instrument_id": instrument_id.group(1) if instrument_id else None,
            "department": department.group(1) if department else None,
            "companyName": companyName.group(1) if companyName else None,
        },
        "instrument_details": {
            "model_no": instrument_model.group(1) if instrument_model else None,
            "serial_no": instrument_serial_no.group(1) if instrument_serial_no else None,
            "firmware_version": instrument_firmware_version.group(1) if instrument_firmware_version else None,
        },
        "user_details": {
            "user_name": user_name.group(1) if user_name else None,
            "role_name": role_name.group(1) if role_name else None,
            "group_name": group_name.group(1) if group_name else None,
        },
        "product_details": {
            "product_name": product_name.group(1) if product_name else None,
            "product_type": product_type.group(1) if product_type else None,
            "apparatus_type": apparatus_type.group(1) if apparatus_type else None,
            "set_temp": float(set_temp.group(1)) if set_temp else None,
            "test_mode": test_mode.group(1) if test_mode else None,
        },
        "test_details": {
            "Test_arm_name": Test_arm_name.group(1) if Test_arm_name else None,
            "batch_no": batch_no.group(1) if batch_no else None,
            "lot_no": lot_no.group(1) if lot_no else None,
            "start_date": start_date.group(1) if start_date else None,
            "start_time": start_time.group(1) if start_time else None,
            "end_date": end_date.group(1) if end_date else None,
            "end_time": end_time.group(1) if end_time else None,
            "test_id": test_id.group(1) if test_id else None,
            "test_status": test_status.group(1) if test_status else None,
            "on_duration": on_duration.group(1) if on_duration else None,
            "halt_duration": halt_duration.group(1) if halt_duration else None,
        },
        "time_details": time_details_parsed,
        "remark": remark.group(1) if remark else None,
        "analyzed_by": analyzed_by.group(1) if analyzed_by else None,
        "checked_by": checked_by.group(1) if checked_by else None,
        "file_name": file_name
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
    insert_data_to_db(parsed_data)

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
def insert_data_to_db(parsed_data):
    try:
        connection = get_connection()
        if connection is None:
            print("Failed to connect to the database.")
            return

        cursor = connection.cursor()
        insert_query = "INSERT INTO equipment_data_json (json_data, fileName) VALUES (%s, %s)"
        json_data = json.dumps(parsed_data)
        file_name = parsed_data.get("file_name")

        cursor.execute(insert_query, (json_data, file_name))
        connection.commit()
        print(f"Data and file name inserted successfully for file: {file_name}")

    except mysql.connector.Error as err:
        print(f"Error inserting data: {err}")

    finally:
        if connection.is_connected():
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
                insert_data_to_db(parsed_data)
            else:
                print(f"File {file_name} is already processed. Skipping.")

# Sample PDF path
folder_path = r"C:\Users\Atachi.IND052\Downloads\Sample PDFs2\SamplePDFs\Disintegration"
process_files_in_folder(folder_path)