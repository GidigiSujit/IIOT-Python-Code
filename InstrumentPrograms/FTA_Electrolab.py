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

def parse_pdf_data(text, file_name):
    # Parsing Kloudface Details
    brand_name = re.search(r"Brand\s*:\s*(.+?)\s+Model No\.\s*:\s*(\S+)", text)
    instrument_id = re.search(r"Instrument ID\s*:\s*(\S+)", text)
    serial_no = re.search(r"Serial Number\s*:\s*(\S+)", text)
    firmware_version = re.search(r"Firmware Ver\.\s*:\s*(\S+)", text)
    company_name = re.search(r"Company\s*:\s*(\S+)", text)
    department = re.search(r"Department\s*:\s*(\S+)", text)

    # Parsing Parent Instrument Details
    instrument_model = re.search(r"Model No\.\s*:\s*(\S+)", text)
    instrument_serial_no = re.search(r"Serial Number\s*:\s*(\S+)", text)
    instrument_firmware_version = re.search(r"Firmware Ver\.\s*:\s*(\S+)", text)
    parent_instrument_id = re.search(r"Instrument ID\s*:\s*(\S+)", text)

    # Parsing User Details
    user_name = re.search(r"User\s*:\s*(\S+)", text)
    role_name = re.search(r"Role\s*:\s*(\S+)", text)
    group_name = re.search(r"Group\s*:\s*(\S+)", text)

    # Parsing Product Setup Details
    product_name = re.search(r"Product Name\s*:\s*(\S+)", text)
    run_mode = re.search(r"Run Mode\s*:\s*(\S+)", text)
    rpm = re.search(r"RPM\s*:\s*(\S+)", text)
    drum_type = re.search(r"Drum Type\s*:\s*(\S+)", text)
    num_of_drums = re.search(r"No\.\s*of\s*Drums\s*:\s*(\S+)", text)
    fr_limit = re.search(r"Fr\.Limit\(%\s*W/W\)\s*:\s*(\S+)", text)
    set_count = re.search(r"Set Count\s*:\s*(\S+)", text)

    # Parsing Test Details
    test_id = re.search(r"Test ID\s*:\s*(\S+)", text)
    start_date = re.search(r"Start Date\s*:\s*(\S+)", text)
    start_time = re.search(r"Start Time\s*:\s*(\S+)", text)
    end_date = re.search(r"End Date\s*:\s*(\S+)", text)
    end_time = re.search(r"End Time\s*:\s*(\S+)", text)
    batch_no_d1 = re.search(r"Batch No\.\s*D1\s*:\s*(\S*)", text)
    ar_no_d1 = re.search(r"AR No\.\s*D1\s*:\s*(\S*)", text)
    batch_no_d2 = re.search(r"Batch No\.\s*D2\s*:\s*(\S*)", text)
    ar_no_d2 = re.search(r"AR No\.\s*D2\s*:\s*(\S*)", text)

    # Parsing RPM History
    rpm_history = re.findall(r"(\d{4})\s+(\d{3}\.\d)", text)
    rpm_history_parsed = [{"interval": t[0], "rpm": t[1]} for t in rpm_history]

       # Parsing Calculations with more flexible whitespace handling
    # Regular expression patterns for each field
    weight_before_pattern = r"Weight Before Test\(g\):\s+(\d+\.\d+)"
    weight_after_pattern = r"Weight After\s+Test\(g\):\s+(\d+\.\d+)"
    friability_pattern = r"Friability\(%\)\s+:\s+(\d+\.\d+)"
    result_pattern = r"Result\s+:\s+(\w+)"

    # Finding values for each field
    weight_before_drum1, weight_before_drum2 = re.findall(weight_before_pattern, text)
    weight_after_drum1, weight_after_drum2 = re.findall(weight_after_pattern, text)
    friability_drum1, friability_drum2 = re.findall(friability_pattern, text)
    result_drum1, result_drum2 = re.findall(result_pattern, text)
    
    
    # Parsing Remarks and Signature Fields
    remark = re.search(r"Remark\s*:\s*([^_]+)", text)
    analyzed_by = re.search(r"Analysed By\s*:\s*(.*)", text)
    reviewed_by = re.search(r"Reviewed By\s*:\s*(.*)", text)

    # Structuring parsed data into a dictionary
    parsed_data = {
        "kloudface_details": {
            "brand_name": brand_name.group(1) if brand_name else None,
            "model_no": brand_name.group(2) if brand_name else None,
            "instrument_id": instrument_id.group(1) if instrument_id else None,
            "serial_no": serial_no.group(1) if serial_no else None,
            "firmware_version": firmware_version.group(1) if firmware_version else None,
            "company_name": company_name.group(1) if company_name else None,
            "department": department.group(1) if department else None,
        },
        "parent_instrument_details": {
            "model_no": instrument_model.group(1) if instrument_model else None,
            "serial_no": instrument_serial_no.group(1) if instrument_serial_no else None,
            "firmware_version": instrument_firmware_version.group(1) if instrument_firmware_version else None,
            "instrument_id": parent_instrument_id.group(1) if parent_instrument_id else None,
        },
        "user_details": {
            "user_name": user_name.group(1) if user_name else None,
            "role_name": role_name.group(1) if role_name else None,
            "group_name": group_name.group(1) if group_name else None,
        },
        "product_setup_details": {
            "product_name": product_name.group(1) if product_name else None,
            "run_mode": run_mode.group(1) if run_mode else None,
            "rpm": rpm.group(1) if rpm else None,
            "drum_type": drum_type.group(1) if drum_type else None,
            "num_of_drums": num_of_drums.group(1) if num_of_drums else None,
            "fr_limit": fr_limit.group(1) if fr_limit else None,
            "set_count": set_count.group(1) if set_count else None,
        },
        "test_details": {
            "test_id": test_id.group(1) if test_id else None,
            "start_date": start_date.group(1) if start_date else None,
            "start_time": start_time.group(1) if start_time else None,
            "end_date": end_date.group(1) if end_date else None,
            "end_time": end_time.group(1) if end_time else None,
            "batch_no_d1": batch_no_d1.group(1) if batch_no_d1 else None,
            "ar_no_d1": ar_no_d1.group(1) if ar_no_d1 else None,
            "batch_no_d2": batch_no_d2.group(1) if batch_no_d2 else None,
            "ar_no_d2": ar_no_d2.group(1) if ar_no_d2 else None,
        },
        "rpm_history": rpm_history_parsed,
        "calculations": {
            "weight_before_drum1": weight_before_drum1,
            "weight_before_drum2": weight_before_drum2,
            "weight_after_drum1": weight_after_drum1,
            "weight_after_drum2": weight_after_drum2,
            "friability_drum1": friability_drum1,
            "friability_drum2": friability_drum2,
            "result_drum1": result_drum1,
            "result_drum2": result_drum2,
        
},
        "remark": remark.group(1) if remark else None,
        "analyzed_by": analyzed_by.group(1) if analyzed_by else None,
        "reviewed_by": reviewed_by.group(1) if reviewed_by else None,
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
folder_path = r"C:\Users\Atachi.IND052\Downloads\Sample PDFs2\SamplePDFs\Friability"
process_files_in_folder(folder_path)