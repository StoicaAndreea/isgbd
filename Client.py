import socket

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

        message = input(" -> ")  # again take input

    client_socket.close()  # close the connection


if __name__ == '__main__':
    client_program()