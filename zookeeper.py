import subprocess
import socket
import time

ip= "127.0.0.1" 

while True:
    host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number
    port_1 = 5001
    port_2=5002
    try:
        consumer_socket = socket.socket()  # instantiate
        consumer_socket.connect((host, port))
        print("Leader is alive.")
        time.sleep(5)
    except:
        print("Broker 1 is dead. ")
        try:
            consumer_socket_1 = socket.socket()  # instantiate
            consumer_socket_1.connect((host, port_1))
            print("Broker 2 is alive ")
            m = "9"
            consumer_socket_1.send(m.encode())
            consumer_socket_1.close()
 
        except:
            print("Broker 2 is dead. ")
            try:
                consumer_socket_2 = socket.socket()  # instantiate
                consumer_socket_2.connect((host, port_2))
                print("Broker 3 is alive ")
                m = "9"
                consumer_socket_2.send(m.encode())
                consumer_socket_2.close()
    
            except:
                break
                


    
