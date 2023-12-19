import socket
import json
from CatalogController import CatalogController


def server_program():
    #   '''
    # USE DB          -0
    # CREATE DB       -1
    # DROP DB         -2
    # CREATE TB       -3
    # DROP TB         -4
    # CREATE INDEX    -5
    # DROP INDEX      -6
    #    '''
    g = open("selectRESULTS.txt", "w")

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
        response = conn.recv(1024).decode()
        if not response:
            # if data is not received break
            break
        print("from connected user: " + response)
        # parse response:
        dataJson = json.loads(response)
        try:
            # use database
            if dataJson["command"] == 0:
                catalog.useDatabase(dataJson["databaseName"])
                currentDatabase = dataJson["databaseName"]
                response = "Using " + dataJson["databaseName"]
            # create database
            elif dataJson["command"] == 1:
                response = catalog.createDatabase(dataJson["databaseName"])
                if response == "Database was created":
                    currentDatabase = dataJson["databaseName"]
            # drop database
            elif dataJson["command"] == 2:
                response = catalog.dropDatabase(dataJson["databaseName"])
                if response == "Database dropped successfully":
                    currentDatabase = ""
            # create table
            elif dataJson["command"] == 3:
                response = catalog.createTable(currentDatabase, dataJson)
            # drop table
            elif dataJson["command"] == 4:
                response = catalog.dropTable(currentDatabase, dataJson["tableName"])
            # create index
            elif dataJson["command"] == 5:
                response = catalog.createIndex(currentDatabase, dataJson)
            # create index cu name dat de user
            elif dataJson["command"] == 55:
                response = catalog.createIndexWithName(currentDatabase, dataJson)
            # drop index
            elif dataJson["command"] == 6:
                response = catalog.dropIndex(currentDatabase, dataJson)
            elif dataJson["command"] == 7:
                response = catalog.insert(currentDatabase, dataJson)
            elif dataJson["command"] == 8:
                response = catalog.delete(currentDatabase, dataJson)
            elif dataJson["command"] == 9:
                response = catalog.select(currentDatabase, dataJson)
            else:
                response = "Invalid command id..."
        except Exception as ex:
            response = str(ex)

        print(response)
        g.close()
        g = open("selectRESULTS.txt", "w")
        g.write(response)

        # if response != "":response = input(' -> ')
        if len(response)>1000:
            response="Select success , check file"
        conn.send(response.encode())  # send response to the client
    conn.close()  # close the connection


if __name__ == '__main__':
    server_program()
