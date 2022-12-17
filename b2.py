import socket
import os
import shutil

from broker import broker_program
from _thread import *
host = '127.0.0.1'

# f = open('C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project'+'\\logs_b1.txt',"w")
# f.close()

src = 'C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics'
dst = 'C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics2'
shutil.copytree(src, dst)

def broker2():
    host = socket.gethostname()
    port1 = 5002  # initiate port1 no above 1024

    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port1))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(10)
    # shutil.copyfile("log_try.log","logs_b11.txt")
    f = open('logcopy.txt','r')

    # print("Connection from: " + str(address))
    while True:
        
       
        # shutil.copyfile("logcopy.txt","log_b2.txt")

        print(1)
        Client, address = server_socket.accept()
        message = Client.recv(1024).decode()
        
        if message == "9":
            broker_program()
        

             
if __name__ == '__main__':
    broker2()