import socket

def consumer_program():
    host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number
    while True:
        try:
            consumer_socket = socket.socket()  # instantiate
            consumer_socket.connect((host, port))  # connect to the server
            print(consumer_socket)
            topic = input("Which topic do you want-> ")  # take input
            from_beg = input("Enter 0 for current and 1 for the whole file-> ")
            s = "2" + topic + "@" + from_beg
            print("Sending to broker: ", s)

            consumer_socket.send(s.encode())  # send message
            data = consumer_socket.recv(1024).decode()  # receive response
            print('Received from broker: ' + data)  # show in terminal
            if from_beg==-1:
                break
        except:
            print("BROKER FAILED! New broker to be appointed...")
    consumer_socket.close()  # close the connection


if __name__ == '__main__':
    consumer_program()






    