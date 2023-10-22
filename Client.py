import socket

from DataConverter import DataConverter
from Validator import Validator




def client_program():
    host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server
    message = input(" -> ")  # take input

    validator =Validator()
    converter =DataConverter()
    while message != 'bye':

        #1 se trimite mesaj de eroare daca e gresita
        msg=validator.validate(message.lower().strip())
        if msg == False:
            print("SyntaxError")
        else:
            #2 mesajul se trimite la server daca e corect
            #todo aici am impresia ca vrea cu HTTP deci aici s ar face parsarea
            #todo s-ar stabili ce metoda din server sa se apeleze si s ar trimite datele necesare
            #CREATING JSON FILE TO SEND TO SERVER
            if  msg== 1 or msg== 2 or msg == 4:
                msgToSend = converter.convertSimple(msg,message.lower().strip())
            elif msg==3:
                msgToSend = converter.convertCreateTable(msg,message.lower().strip())
            else: msgToSend="other"



            client_socket.send(msgToSend.encode())  # send message


            #raspunsul primit de la server ,todo aici cand e vreun select am sa incerc eu sa aranjez datele frumos ca intr-un tabel
            data = client_socket.recv(1024).decode()  # receive response
            print('Received from server: ' + data)  # show in terminal

        message = input(" -> ")  # again take input

    client_socket.close()  # close the connection


if __name__ == '__main__':
    client_program()