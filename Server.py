import socket
import json
from CatalogController import CatalogController


def server_program():
    #   '''
    # CREATE DB       -1
    # DROP DB         -2
    # CREATE TB       -3
    # DROP TB         -4
    # CREATE INDEX    -5
    #    '''
    catalog = CatalogController()
    # get the hostname
    currentDatabase=""
    host = socket.gethostname()
    port = 5000  # initiate port no above 1024
    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together
    # configure how many client the server can listen simultaneously
    server_socket.listen(2)
    conn, address = server_socket.accept()  # accept new connection
    print("Connection from: " + str(address))
    while True:
        # receive data stream. it won't accept data packet greater than 1024 bytes
        data = conn.recv(1024).decode()
        if not data:
            # if data is not received break
            break
        print("from connected user: " + data)#aici daca vrea sa facem cumva cu mai multi clienti conectati
        # deodata ar fi naspa...
        answer=True
        # parse data:
        y = json.loads(data)

        if y["command"] == 0:
            answer ="Using "+ y["databaseName"]
            currentDatabase = y["databaseName"]
            return answer
        #create database
        if y["command"] == 1:
            print(y["databaseName"])
            answer = catalog.createDatabase(y["databaseName"])
            if answer == "Baza de date a fost adaugata":
                currentDatabase=y["databaseName"]
            data=answer
        #drop database
        if y["command"] == 2:
            answer = catalog.dropDatabase(y["databaseName"])
            data=answer
            if answer == "database dropped succesfully":
                currentDatabase=""
            print(answer)
        # create table
        if y["command"] == 3:
            answer = catalog.createTable(currentDatabase,y)
            data = answer
            print(answer)
        # drop table
        if y["command"] == 4:
            answer = catalog.dropTable(currentDatabase,y["databaseName"])
            data = answer
            print(answer)
        # create index

        #if data != "":data = input(' -> ')
        conn.send(data.encode())  # send data to the client

    conn.close()  # close the connection


if __name__ == '__main__':
    server_program()