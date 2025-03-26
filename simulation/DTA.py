import socket
import time

# Configuration for the simulated Telnet server
HOST = 'localhost'  # Localhost for testing
PORT = 23           # Default Telnet port

# Simulated data to send to Telnet clients
telnet_data = """
      ERWEKA INDIA PVT LTD401,ASHIRWAD PARAS, 
                      PRAHALAD NAGAR,     AHMEDABAD-380015    
               iNWEKA DIGITAL DISINTEGRATION TEST APPARATUS
       SERIAL NO : 608059.1222            INSTRUMENT NO: EQ-072         
       TIME      : 17:38:05               DATE         : 12/11/12
       TEST TIME : 00:15:00               PRODUCT NAME : BETAGESIC TABLETS   
       TEST NO   : 15                     BATCH NO     : EBRM             
       TESTED IN : BEAKER1                SET TEMP     : 37.0Deg
       LOT NO    : 1                      MEDIA        : jater      
                               TEST REPORT -------------------------------------------------------------------------------
      Time   |         Elapsed Time |         Temperature  Deg
      Hr:Min |              Hr:Min  |            BEAKER 1   -------------------------------------------------------------------------------
       TEST ABORTED AT   17:39:11-------------------------------------------------------------------------------
                          BEAKER 1-1 =  00:00:00
                          BEAKER 1-2 =  00:00:00
                          BEAKER 1-3 =  00:00:00
                          BEAKER 1-4 =  00:00:00
                          BEAKER 1-5 =  00:00:55
                          BEAKER 1-6 =  00:00:00
 MIN TANK TEMP: 36.9             MAX TANK TEMP: 37.0
 BKR1 MIN TEMP: 37.1             BKR1 MAX TEMP: 37.2
 HALT DURATION:  00:00 MIN:SEC
 COMMENT :  
       ------------------------------------------------------------------------------------------------------------------------------------------------
       Operator name                                             Approved by 
       MR.ARUN                                                  SRINIVAS RAO
"""

# Create a simple Telnet server
def start_telnet_server():
    # Create a socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen(1)
        print(f"Telnet server running on {HOST}:{PORT}")

        # Accept a client connection
        client_socket, client_address = server_socket.accept()
        with client_socket:
            print(f"Connection from {client_address}")
            # Send the data to the client with appropriate line breaks
            for line in telnet_data.splitlines():
                # Send each line with \r\n to mimic real serial data
                client_socket.send((line + "\r\n").encode('utf-8'))
                time.sleep(0.1)  # Simulate slight delay between lines

            # After sending all data, close the connection
            print("Data sent, closing connection.")
            client_socket.close()  # Close the connection after sending all data

# Start the Telnet server
if __name__ == "__main__":
    start_telnet_server()
