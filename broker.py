import socket
import os
from _thread import *
import shutil
import logging.handlers
fb1 = open('C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project'+'\\log_b1.txt',"w")
fb1.write("")
fb1.close()
fb2 = open('C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project'+'\\log_b2.txt',"w")
fb2.write("")
fb2.close()
f = open('C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\logs.txt','w')
data = []
def partition(x):
    x = int (x)
    return str(x%3)


def broker_program():
     
    # get the hostname
    host = socket.gethostname()
    port1 = 5000  # initiate port1 no above 1024

    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port1))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(10)
    # print("Connection from: " + str(address))
    

    while True:
        Client, address = server_socket.accept()
        # print('Connected to: ' + address[0] + ':' + str(address[1]))
        start_new_thread(multi_threaded_client, (Client, ))
 
def multi_threaded_client(connection):
    temp=open("C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project" + "\\logs.txt",'a')
    v = ""
    bd = ""
    while True:
        
        message1=connection.recv(1024).decode()
        m=str(message1[1:])
       

        s2="Keep listening"

        if not message1:
            break

        elif message1[0]=="1":
            print('Received from Producer(topicname@message@key): ' + m)
            #RECEIVE FROM PRODUCER
            #MESSAGE-> topic, message, key
            x = m.split("@")
            print(x)
            s1 = x[0]
            s2 = x[1]
            data.append([s1,s2])
            v = s1
            bd = s2
            s3=int(x[2])
            zz=s2+ " is published under "+ s1+"\n"
            temp.write(zz)
            logging.info(zz)

            tpat0="C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics\\"+ s1+"\\message0.txt"
            tpat1="C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics\\"+ s1+"\\message1.txt"
            tpat2="C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics\\"+ s1+"\\message2.txt"

            print("from connected user: " + str(m))
            dir = os.path.join("C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics", s1)
            if not os.path.exists(dir):
                os.mkdir(dir)
                tt0=open(tpat0,"w")
                tt0.close()
                tt1=open(tpat1,"w")
                tt1.close()
                tt2=open(tpat2,"w")
                tt2.close()

            path = "C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics" + "\\" + s1 + "\\message"+partition(s3)+".txt"    #s1 is directory name
            f = open(path, "a")
            f.write(s2)
            f.close()


        elif message1[0]=="2":
            #RECEIVE FROM THE CONSUMER
            #MESSAGE-> topic, flag
            print('Received from Consumer (topic name@flag): ' + m)
            x = m.split("@")
            
            topicc = x[0]
            flag = x[1]
            path0 = "C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics" + "\\" + topicc + "\\message0.txt"    #s1 is directory name
            path1 = "C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics" + "\\" + topicc + "\\message1.txt"    #s1 is directory name
            path2 = "C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics" + "\\" + topicc + "\\message2.txt"    #s1 is directory name

            zz2="Consumer requests for topic "+ topicc+ "\n"
            temp.write(zz2)
            logging.info(zz2)


            #SEND TO THE CONSUMER
            tpath0="C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics\\"+ topicc+"\\message0.txt"
            tpath1="C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics\\"+ topicc+"\\message1.txt"
            tpath2="C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics\\"+ topicc+"\\message2.txt"

            dirr = os.path.join("C:\\Users\\tanus\\Desktop\\sem5\\BD\\BD project\\topics", topicc)
            if not os.path.exists(dirr):
                os.mkdir(dirr)
                t0=open(tpath0,"w")
                t0.close()
                t1=open(tpath1,"w")
                t1.close()
                t2=open(tpath2,"w")
                t2.close()
                break

            else:

                fread0=open(path0,"r")
                fr0=fread0.read()
                fread0.close()
                print(fr0)

                fread1=open(path1,"r")
                fr1=fread1.read()
                fread1.close()
                print(fr1)

                fread2=open(path2,"r")
                fr2=fread2.read()
                fread2.close()
                print(fr2)
                fr=fr0+"\n"+fr1+"\n"+fr2+"\n"
                if flag=="0":
                    bd = " "
                    for i in data:
                        if i[0] == topicc:
                            bd = bd + i[1]
                    if bd == " ":
                        connection.send(s2.encode())
                    else:
                        connection.send(bd.encode())
                        #vidisha run this 

                    
                elif flag=="1":
               

                       connection.send(fr.encode())
                    
                zz4="Broker sends topic messge "+ fr+ " to the consumer \n"
                temp.write(zz4)
                logging.info(zz4)
                shutil.copyfile("logs.txt","log_b1.txt")
                shutil.copyfile("logs.txt","log_b2.txt")
        

    connection.close()
    

if __name__ == '__main__':
    broker_program()

