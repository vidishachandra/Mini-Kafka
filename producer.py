import socket

def client_program():
    host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number
    
    
    while True:
        try:
            client_socket = socket.socket()  # instantiate
            client_socket.connect((host, port))
            # connect to the server
            # n = int(input("Enter the number of producers : "))
            topic = input("Enter topic name-> ")
            message = input(" Enter messasge-> ")
            key = input(" Enter key-> ")
            # take input  
            s = "1"+ topic + "@" + message + "@"+ key
            print("Sending to broker: ", s)
            client_socket.send(s.encode())  # send message
            if key==-1:
                break
        except:
            print("BROKER FAILED! New broker to be appointed...")
    client_socket.close()  # close the connection

if __name__ == '__main__':
    client_program()
    
    
    
    
    