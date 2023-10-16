import socket


def client_program():
    host = socket.gethostname()  # as both code is running on same pc
    port = 5000  # socket server port number

    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server

    message = input(" -> ")  # take input

    while message.lower().strip() != 'bye':
        #todo se verifica daca comanda scrisa este corecta
        #1 se trimite mesaj de eroare daca e gresita


        #2 mesajul se trimite la server daca e corect
        #todo aici am impresia ca vrea cu HTTP deci aici s ar face parsarea
        #todo s-ar stabili ce metoda din server sa se apeleze si s ar trimite datele necesare
        client_socket.send(message.encode())  # send message


        #raspunsul primit de la server ,todo aici cand e vreun select am sa incerc eu sa aranjez datele frumos ca intr-un tabel
        data = client_socket.recv(1024).decode()  # receive response
        print('Received from server: ' + data)  # show in terminal

        message = input(" -> ")  # again take input

    client_socket.close()  # close the connection


if __name__ == '__main__':
    client_program()