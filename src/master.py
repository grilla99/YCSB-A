import time
from time import sleep
from datetime import datetime
from socket import socket, timeout
from threading import Thread
from argparse import ArgumentParser
import logging
import os
import tqdm

class Master:
    #Master takes a string as the address and an integer for the port
    def __init__(self, address: str ="", port: int = 50000):
        self.server_address = address
        self.server_port = port
        self.socket = socket()
        #3 second timeout
        self.socket.settimeout(3)
        self.connected_socket_list = []
        self.connected_address_list = []
        self.stop = False
        self.log_file = "master.log"
        self.workers_connected = 0
        self.encoding = "UTF-8"
        self.waiting_connection = Thread(target=self.__waiting_for_connection, name="WaitingConnection")
        self.listening_message = Thread(target=self.__listening_message, name="ListeningMessage")

    def start(self):
        logging.basicConfig(
            filename=self.log_file,
            #UK time
            datefmt='%d-%m-%y %H:%M:%S',
            filemode='w',
            ## If this level of information too much, use logging info
            level=logging.DEBUG,
            format='%(asctime)s %(threadName)-17s %(levelname)-8s- %(message)s'
        )
        logging.info("Master node started")
        self.__print_help()
        self.__setup_socket()
        self.waiting_connection.start()
        self.listening_message.start()

        while not self.stop:
            time.sleep(20)
            print(f"Awaiting ready confirmation. {self.workers_connected} connected to master. ")
            raw_input = input()
            if raw_input != '':
                split_input = raw_input.split(" ")
                len_msg = len(split_input)
                now = datetime.now()
                if split_input[0] == "start":  # #### START #####
                    if len_msg == 1:
                        self.__start_log()
                    else:
                        print(f"[{now.hour}:{now.minute}] Too many arguments : Try like this 'start'.")
                elif split_input[0] == "stop":  # #### STOP #####
                    if len_msg == 1:
                        self.__stop_log()
                    else:
                        print(f"[{now.hour}:{now.minute}] Too many arguments : Try like this 'stop'.")
                elif split_input[0] == "get":  # #### GET #####
                    if len_msg == 2:
                        lines = int(split_input[1])
                        self.__get_log(lines)
                    else:
                        print(f"[{now.hour}:{now.minute}] Wrong syntax : Try like this 'get <lines>'.")
                elif split_input[0] == "help":  # #### HELP #####
                    if len_msg == 1:
                        self.__print_help()
                    else:
                        print(f"[{now.hour}:{now.minute}] Too many arguments : Try like this 'help'.")

                elif split_input[0] == "worker":  # #### WORKER #####
                    if len_msg == 1:
                        self.__print_connected_worker()
                    else:
                        print(f"[{now.hour}:{now.minute}] Too many arguments : Try like this 'worker'.")

                elif split_input[0] == "load":
                    if len_msg == 5:
                        self.__load(split_input)
                    else:
                        print(f"[{now.hour}:{now.minute}] Too many arguments : Try like this 'load <database>"
                              f" <workloads/workload<X>> > outputFile ")

                elif split_input[0] == "run":
                    if len_msg == 5:
                        self.__run(split_input)
                    else:
                        print(f"[{now.hour}:{now.minute}] Too many arguments : Try like this 'run <database>"
                              f" <workloads/workload<X>> > outputFile ")

                elif split_input[0] == "benchmark" and split_input[1] == "logs":
                    if len_msg == 3:
                        print(split_input)
                        self.__get_benchmark_log(split_input)
                    else:
                        print(f"[{now.hour}:{now.minute}] Too many arguments : Try like this 'benchmark logs")

                elif split_input[0] == "exit":  # #### EXIT #####
                    if len_msg == 1:
                        self.__exit(0)  # Exit master only
                    elif len_msg == 2 and split_input[1] == "all":
                        self.__exit(1)  # Exit and stop workers execution
                    else:
                        print(f"[{now.hour}:{now.minute}] Too many arguments : Try like this 'exit [all]'.")
                else:
                    print(f"[{now.hour}:{now.minute}] Unknown command : '{raw_input}'.")
                    logging.info(f"Unknown command : '{raw_input}'.")
            sleep(0.01)
        logging.info("Exiting master...")

    def __setup_socket(self):
        try:
            self.socket.bind((self.server_address, self.server_port)) #Bind socket address to localhost / 127.0.0.1:5000
            self.socket.listen()
        except OSError:
            print(f"/!\\ Socket {self.server_address}:{self.server_port} already used")
            logging.error("OSError > Desired socket already in use. ")
            self.__exit(0)

    def __waiting_for_connection(self):
        while not self.stop:
            try:
                conn, address = self.socket.accept()
                conn.settimeout(0.000001)
                self.__add_worker(conn, address)
                print(f"[{datetime.now().hour}:{datetime.now().minute}] "
                      f"New worker connected : {address[0]}:{address[1]}.")
                logging.info(f"New worker connected : {address[0]}:{address[1]}.")
            except timeout:
                pass

    def __listening_message(self):
        while not self.stop:
            i = 0
            while i < self.workers_connected and not self.stop:  # loop on every worker to receive messages
                try:
                    msg_received = self.connected_socket_list[i].recv(4096).decode(self.encoding)
                    if msg_received == "exit":  # If message == "exit"
                        self.__remove_worker(i)  # Remove the workers
                    else:
                        filename, filesize = msg_received.split("<SEPARATOR>")
                        # filename = os.path.basename(filename)
                        filesize = int(filesize)

                        progress = tqdm.tqdm(range(filesize), f"Receiving {filename}", unit="B", unit_scale=True,
                                              unit_divisor=1024)

                        with open(filename, "wt") as f:
                            while True:
                                # Read 1024 bytes from socket
                                if msg_received == " ":
                                    break
                                else:
                                    f.write(msg_received)
                                    print(msg_received)
                                break

                        # else
                    #     now = datetime.now()
                    #     print(f"[{now.hour}:{now.minute}] {self.connected_address_list[i][0]}:"
                    #           f"{self.connected_address_list[i][1]} >>>", msg_received)
                    #     msg_received = " ".join(msg_received.split("\n"))  # replace "\n" with " "
                    #     logging.info(f"{self.connected_address_list[i][0]}:"
                    #                  f"{self.connected_address_list[i][1]} >>> " + msg_received)
                        i += 1
                except (ConnectionResetError, ConnectionAbortedError):  # If connection Reset ou Aborted
                    logging.error("ConnectionError > Removing worker.")
                    self.__remove_worker(i)  # Remove the disconnected worker
                except timeout:
                    pass

    def __add_worker(self, connection: socket, address: tuple):
        self.connected_socket_list.append(connection)  # add worker's address to address list
        self.connected_address_list.append(address)  # add worker's socket to socket list
        self.workers_connected += 1

    def __remove_worker(self, position: int):
        now = datetime.now()
        print(f"[{now.hour}:{now.minute}] Worker {self.connected_address_list[position][0]}:"
              f"{self.connected_address_list[position][1]} disconnected.")
        logging.info(f"Worker {self.connected_address_list[position][0]}:"
                     f"{self.connected_address_list[position][1]} disconnected.")
        self.connected_socket_list[position].close()  # Closing socket with worker i
        self.connected_address_list.pop(position)  # remove worker's address from address list
        self.connected_socket_list.pop(position)  # remove worker's socket from socket list
        self.workers_connected -= 1

    def __start_log(self):
        self.__send_message_to_all("start_log")

    def __stop_log(self):
        self.__send_message_to_all("stop_log")

    def __get_log(self, lines: int):
        data = "get_log" + " " + str(lines)
        self.__send_message_to_all(data)


    def __send_message(self, target: socket, data: str):
        target.send(data.encode(self.encoding))

    def __send_message_to_all(self, data: str):
        for i in range(self.workers_connected):
            try:
                self.__send_message(self.connected_socket_list[i], data)  # Send message
            except ConnectionResetError:  # If connection Reset
                self.__remove_worker(i)  # Remove the disconnected worker
                i -= 1

    #TODO: Add the -s -P -p on client side, -s reports status, -P loads property files and -p is params
    def __load(self, input_msg: list):
        database = input_msg[1]  # Example "load mongodb workloads/workloada > outputLoad.txt"
        workload_string = input_msg[2]
        output_file = input_msg[4]
        if len(input_msg) == 5:
            try:
                records_to_insert = int(input("How many records would you like to insert?: "))
                records_per_node = str(int(records_to_insert / self.workers_connected))
                for x in range(self.workers_connected):
                    try:
                        data = "load" + " " + database + " " + "-s -P" + " " + workload_string + " > " + output_file + " " \
                           + records_per_node + " " + str(x) + \
                           " " + str(records_to_insert)  # load mongodb -s -P workloads/workloada > outputLoad.txt 1000 1
                        print(data)
                        self.__send_message(self.connected_socket_list[x], data)
                    except ConnectionResetError:
                        self.__remove_worker(x)  # Remove the disconnected worker
                        x -= 1
            except ValueError:
                print("No such database")
        else:
            print(">> Bad format!")

    # Record count here needs to be the same as in the load phase
    def __run(self, input_msg: list):
        database = input_msg[1] # Example "run mongodb workloads/workloada > outputRun.txt"
        workload_string = input_msg[2]
        output_file = input_msg[4]
        if len(input_msg) == 5:
            try:
                operation_count = input("How many operations do you wish to perform?"
                                        ". (Same as total records in loading phase)")
                individual_ops = str(int(int(operation_count) / self.workers_connected))
                for x in range(self.workers_connected):
                    try:
                        data = "run" + " " + database + " " + "-s -P" + " " + workload_string + " > " + output_file + \
                            " " + individual_ops + " " + str(x)
                        self.__send_message(self.connected_socket_list[x], data)
                    except ConnectionResetError:
                        self.__remove_worker(x) # Remove disconnected worker
                        x -= 1
            except ValueError:
                print("No such database")
        else:
            print(">> Bad format!")


    def __get_benchmark_log(self, input_msg: list):
        database = input_msg[2]
        if len(input_msg) == 3:
            try:
                for x in range(self.workers_connected):
                    try:
                        data = "get" + " " + "benchmark" + " " + "logs" + " " + database
                        self.__send_message(self.connected_socket_list[x], data)
                    except ConnectionResetError:
                        self.__remove_worker(x)
                        x -= 1
            except ValueError:
                print("Check string formatting")
        else:
            print(">> Bad format !")



    @classmethod
    def __print_help(cls):
        print(
            " ---------------------------------- HELP COMMANDS LIST ---------------------------------------------")
        print("  - start : Start log recording on all connected workers.")
        print("  - stop : Stop log recording on all connected workers.")
        print("  - get <lines> : Ask to workers to send the last <lines> of log file.")
        print(" "
              "  - load <mongodb>|<cassandra> workloads/workload<X> > outputLoad.txt")
        print("  "
              "  - run <mongodb>|<cassandra> -s -P workloads/workload<X> > outputRun.txt")
        print("  - help : Print this help menu.")
        print("  - worker : Print addresses of connected workers.")
        print("  - benchmark logs <mongodb>|<cassandra>: Retrieve and collate benchmark logs from workers.")
        print("  - exit [all] : Exit master only. Add 'all' option to stop worker execution too.")
        print(
            "----------------------------------------------------------------------------------------------------")

    def __print_connected_worker(self):
        if self.workers_connected > 0:
            print(f"  -- Connected Worker List ({self.workers_connected}) -- ")
            for i in range(self.workers_connected):
                print(f"   - {self.connected_address_list[i][0]}:{self.connected_address_list[i][1]}")
        else:
            print("  -- No connected workers --")

    def __exit(self, exit_code: int):
        if exit_code == 1:  # 1 =  Quit all
            if self.workers_connected:  # If worker_connected > 0
                self.__send_message_to_all("exit")  # Send the exit message
            else:
                print("  -- No connected workers --")
            while self.workers_connected:  # While they're worker connected, wait they all disconnect
                sleep(0.1)
        self.stop = True  # Set the strop flag to True

def main():
    parser = ArgumentParser(add_help=False)
    parser.add_argument('address', type=str, action='store',
                        help='Address range to listen to (empty mean all addresses)')
    parser.add_argument('port', type=int, action='store', help='Listening port')
    args = parser.parse_args()

    master = Master(args.address, args.port)
    master.start()

if __name__ == '__main__':
    main()
