import random
import socket
import string
from random import randrange

from DataConverter import DataConverter
from Validator import Validator


def client_program():
    host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server
    message = input(" -> ")  # take input

    validator = Validator()
    converter = DataConverter()

    while message != 'bye':
        # message = "use database test;"
        msg = validator.validate(message.strip())
        if msg:
            print(msg)
        else:
            try:
                msgToSend = converter.parseExpression(message.strip())
                client_socket.send(msgToSend.encode())  # send message
                data = client_socket.recv(1024).decode()  # receive response
                print('Received from server: ' + data)  # show in terminal
            except Exception as ex:
                print(ex)

    # letters = string.ascii_lowercase
    # for x in range(1000000):
    #     a = ''.join(random.choice(letters) for i in range(2))+str(x)
    #     message = "insert into table2 (t2id, row, column) values ("+str(x)+", \""+a+"\", "+str(x)+"); "
    #     if x == 30:
    #         message = "insert into table2 (t2id, row, column) values (" + str(x) + ", \"latrat\", " + str(x) + "); "
    #     msg = validator.validate(message.strip())
    #     if msg:
    #         print(msg)
    #     else:
    #         try:
    #             msgToSend = converter.parseExpression(message.strip())
    #             client_socket.send(msgToSend.encode())  # send message
    #             data = client_socket.recv(1024).decode()  # receive response
    #             print('Received from server: ' + data)  # show in terminal
    #         except Exception as ex:
    #             print(ex)

        message = input(" -> ")  # again take input

    client_socket.close()  # close the connection


if __name__ == '__main__':
    client_program()
