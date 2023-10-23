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
    currentDatabase = ""
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
        print("from connected user: " + data)  # aici daca vrea sa facem cumva cu mai multi clienti conectati
        # deodata ar fi naspa...
        answer = True
        # parse data:
        dataJson = json.loads(data)

        # use database
        if dataJson["command"] == 0:
            answer = "Using " + dataJson["databaseName"]
            currentDatabase = dataJson["databaseName"]
            data = answer
        # create database
        if dataJson["command"] == 1:
            print(dataJson["databaseName"])
            answer = catalog.createDatabase(dataJson["databaseName"])
            if answer == "Baza de date a fost adaugata":
                currentDatabase = dataJson["databaseName"]
            data = answer
        # drop database
        if dataJson["command"] == 2:
            answer = catalog.dropDatabase(dataJson["databaseName"])
            data = answer
            if answer == "database dropped succesfully":
                currentDatabase = ""
            print(answer)
        # create table
        if dataJson["command"] == 3:
            answer = catalog.createTable(currentDatabase, dataJson)
            data = answer
            print(answer)
        # drop table
        if dataJson["command"] == 4:
            answer = catalog.dropTable(currentDatabase, dataJson["tableName"])
            data = answer
            print(answer)
        # create index
        if dataJson["command"] == 5:
            answer = catalog.createIndex(currentDatabase, dataJson)
            data = answer
            print(answer)
        # drop index
        if dataJson["command"] == 6:
            answer = catalog.dropIndex(currentDatabase, dataJson)
            data = answer
            print(answer)

        # if data != "":data = input(' -> ')
        conn.send(data.encode())  # send data to the client

    conn.close()  # close the connection


if __name__ == '__main__':
    server_program()
