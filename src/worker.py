from platform import node, version, system, processor, machine
from shutil import disk_usage
from time import sleep
from argparse import ArgumentParser
from datetime import datetime, date
from socket import socket, gethostbyname, gethostname
from threading import Thread
from re import findall
from uuid import getnode
from psutil import cpu_count, virtual_memory
from os import remove
import subprocess
import tqdm
import os
from requests import get
from requests.exceptions import ConnectionError


class Slave:
    def __init__(self, address: str, port: int):
        self.address = address
        self.port = port
        self.socket = socket()
        self.stop = False
        self.recording = False
        self.connected = False
        self.file_name = "slave.log"
        self.encoding = "UTF-8"

    def start(self):
        while not self.stop:  # While exit flag is False
            if self.connected:
                print(">> Waiting order from master...")
                msg = ""
                try:
                    msg = self.socket.recv(1024).decode(self.encoding).split(" ")  # Split received string with " " sep
                    len_msg = len(msg)  # Store length of msg receive
                    print(len_msg, msg)
                    if len_msg == 1:
                        if msg[0] == "start_log":  # If msg is only "start_log"
                            record = Thread(target=self.__start_log)  # Create record Thread
                            record.start()  # Start record Thread
                        elif msg[0] == "stop_log":  # If msg is only "stop_log"
                            self.__stop_log()  # Stop the record
                        elif msg[0] == "exit":  # If msg is only "exit"
                            self.__exit()
                        else:
                            self.connected = False
                    elif len_msg == 2:
                        if msg[0] == "get_log":  # If msg is only "get_log" + arg (arg must be int!!!)
                            self.__get_log(int(msg[1]))
                    elif len_msg == 4:
                        if msg[0] == "get" and msg[1] == "benchmark":
                            self.__get_all_benchmark_logs(msg)
                    elif len_msg == 10 and msg[0] == "load":
                        self.__load_data(msg)
                    elif len_msg == 9 and msg[0] == "run":
                        self.__run_benchmark(msg)

                except ConnectionResetError:
                    print("/!\\ Disconnected...")
                    self.socket.close()
                    self.connected = False
                    self.__connection()
            else:
                self.__connection()

    def __connection(self):
        try:
            print(">> Connection ...")
            self.socket.connect((self.address, self.port))
            print(f">> Connected to {self.address}:{self.port}")
            self.connected = True
        except ConnectionRefusedError:
            self.connected = False
            print(f">> Unable to connect to {self.address}:{self.port}, retrying in 5 seconds...")
            sleep(4)
        except OSError:
            print(f">> OSError, creating new socket...")
            self.socket.close()
            self.socket = socket()

    def __start_log(self):  # Start recording logs
        if self.recording:
            self.__send_message("Error, still recording.")
        else:
            print(">> Starting log record...")
            self.recording = True  # Set recording flag to True
            self.__send_message("Log started!")
            while self.recording:  # While record is active
                timer = 10
                with open(self.file_name, 'w') as file:
                    file.write(self.__get_sys_info())  # Write sys info into log file
                while timer > 0 and self.recording:
                    sleep(1)
                    timer -= 1

    def __stop_log(self):  # Stop recording logs
        if self.recording:
            print(">> Stopping logs record...")
            self.__send_message("Log stopped!")
            self.recording = False
        else:
            self.__send_message("Error, not recording.")

    @staticmethod
    def get_log_name(self, operation, node_num, database):
        date = datetime.today()
        format_date = date.strftime('%d-%m-%y %H:%M:%S')

        if operation == "run":
            filename = "run_logs/node_" + node_num+ "_" + database + "_" + format_date + ".out"
            script_dir = os.path.dirname(__file__)  # Absolute directory this script is in
            abs_file_path = os.path.join(script_dir, filename)

            return abs_file_path

        elif operation == "load":
            filename = "load_logs/node_" + node_num + "_" + database + "_" + format_date + ".out"
            script_dir = os.path.dirname(__file__)  # Absolute directory this script is in
            abs_file_path = os.path.join(script_dir, filename)

            return abs_file_path
        else:
            print("Invalid operation type. Exiting...")

    def __get_log(self, nbr_lines: int):  # Send log to master (arg = line number from the bottom of log file)
        if self.recording:
            self.__stop_log()  # Stopping log before sending
        to_send, nbr_lines = self.__get_log_into_str(nbr_lines)  # Get log from sys
        if to_send is not None:
            self.__send_message("\n" + to_send)  # Send message to master
            print(f">> Sending the {nbr_lines} last lines...")
        else:
            print("/!\\ File doesn't exist!")
            self.__send_message("Log file doesn't exist.")

    def __remove_log(self):
        try:  # Try to remove file
            remove(self.file_name)  # Try to remove file
            print(">> File removed!")  # Printed if file removed
        except FileNotFoundError:
            pass

    def __send_message(self, data: str):
        try:
            self.socket.send(data.encode(self.encoding))  # Send message
        except ConnectionResetError:
            print("/!\\ Disconnected...")
            self.socket.close()
            self.connected = False
            self.__connection()


    def __get_log_into_str(self, nbr_lines: int):
        try:
            log_file = open(self.file_name, "r")  # Open log file in write mode
            lines_list = log_file.readlines()  # Reading lines fro log file
            log_file.close()  # Close log file
            ret, length = "", len(lines_list)  # Initiate "ret" as empty string, "len" as number of log file's lines
            if nbr_lines > length:  # If asked nbr_lines is above max lines of log file then...
                nbr_lines = length  # Store number of log file lines in nbr_lines
            for line in range(length - nbr_lines, length):  # For each "x" last line
                ret += str(lines_list[line])  # add line to "ret" string
            return ret, nbr_lines
        except FileNotFoundError:
            return None

    # Insert Count = How many Records a YCSB client will be inserting during a load phase
    # Record Count = How many Records a YCSB client assumes are present or will te be present in the data store
    # Record Count > Insert count
    def __load_data(self, data:list):
        # Shell = True can be a security hazard if combined with untrusted input
        has_ycsb = subprocess.call("./ycsb_script.sh", shell=True)
        if has_ycsb == 1:
            operation = data[0]  # In this case, will be load
            database = data[1]  # v1: mongodb or cassandra
            run_param = data[2]  # -s
            additional_param = data[3]  # -p / -P
            workload_data = data[4]  # e.g. workloads/workloada
            node = data[8]
            # e.g. if node = 0 and record count = 10: insert start = 0
            # e.g. if node = 1 and record count = 10: = 1 * 10, insert start = 10
            # e.g. if node = 2 and record counst = 10: = 2 * 10, insert start = 20
            # Data[8] is the 'Node number' in relation to master
            # Data[7] is the number of records to insert
            insert_start = 0 if node == "0" else int(node) * int(data[7])
            insert_start_string = "insertstart=" + str(insert_start)
            insert_count = "insertcount=" + data[7]
            record_count = data[9]
            connection_string = 'mongodb.url=mongodb://172.31.26.152:27017,172.31.18.215:27017,172.31.27.18:27017/?replicaSet=rs0'
            # connection_string = "mongodb.url=mongodb://127.0.0.1:27017"

            # If a file exists already, will write to it. If not it shall create a file and write the output
            # Of the YCSB log to it
            file = self.get_log_name(self, "load", data[8], database)


            with open(file, "w+") as f:
                run = subprocess.call(["../ycsb-0.17.0/bin/ycsb",
                                      operation, database, run_param, additional_param, "../ycsb-0.17.0/" + workload_data,
                                      "-p", connection_string, "-p", insert_count, "-p", insert_start_string,
                                       "-p", "recordcount=" + record_count], stdout=f)

        elif has_ycsb == 0:  # If the node doesn't have YCSB installed, issue error message and exit
            print(f"Node {self.address}:{self.port} does not have YCSB installed.")
            print("\n Disconnecting... ")
            self.__exit()

    def __run_benchmark(self, data:list):
        has_ycsb = subprocess.call("./ycsb_script.sh", shell=True)
        print(data)
        if has_ycsb == 1 and data[0] == "run":
            if data[1] == "mongodb":
                operation = "run"
                database = "mongodb"
                run_param = data[2]
                additional_param = data[3]
                workload_data = data[4]
                connection_string = 'mongodb.url=mongodb://172.31.26.152:27017,172.31.18.215:27017,172.31.27.18:27017/?replicaSet=rs0'
                # connection_string = "mongodb.url=mongodb://127.0.0.1:27017"
                operation_count = data[7]
                node = data[8]

                file = self.get_log_name(self, "run", node, database)
                print("file is" + file)

                # Saves the output of the run to file
                with open(file, "w+") as f: # Performs the run phase of YCSB and saves it to the run_logs folder
                    run = subprocess.call(["../ycsb-0.17.0/bin/ycsb",
                                           operation, database, run_param, additional_param,
                                           "../ycsb-0.17.0/" + workload_data, "-p", connection_string
                                           , "-p", "operationcount=" + operation_count], stdout=f)

                self.__get_benchmark_log(file)


        elif has_ycsb == 0:
            print(f"Node {self.address}:{self.port} does not have YCSB installed.")
            print("\n Disconnecting... ")
            self.__exit()

    #TODO: It's breaking on this after executing the function from insert file
    #TODO: File path being passed is incorrect and is using absolute file path, needs to be relative to executing
    # directory
    def __get_benchmark_log(self, file: str):
        try:
            buffer_size = 4096

            filesize = os.path.getsize(file)
            progress = tqdm.tqdm(range(filesize), f"Sending...", unit="B", unit_scale=True,
                                 unit_divisor=1024)

            with open(file, 'r') as f:
                while True:
                    bytes_read = f.read(buffer_size)
                    print(bytes_read)
                    if not bytes_read:
                        break
                    self.socket.sendall(bytes_read.encode())
                    progress.update(len(bytes_read))
        except FileNotFoundError:
            print("File does not Exist")

    def __get_all_benchmark_logs(self, data:str):
        script_dir = os.path.dirname(__file__)
        dirname = "run_logs"
        abs_file_path = os.path.join(script_dir, dirname)
        buffer_size = 4096

        for log_file in os.listdir(abs_file_path):
            if log_file.endswith(".out"):
                filename = abs_file_path + "/" + log_file
                filesize = os.path.getsize(abs_file_path + "/" + log_file)
                progress = tqdm.tqdm(range(filesize), f"Sending {filename}", unit="B", unit_scale=True,
                                     unit_divisor=1024)

                with open(filename, 'r') as f:
                    while True:
                        bytes_read = f.read(buffer_size)
                        print(bytes_read)
                        progress.update(len(bytes_read))
                        self.socket.sendall(bytes_read.encode())
                        if not bytes_read:
                            # File transmission done
                            break


    @classmethod
    def __get_sys_info(cls):
        total, used, free = disk_usage("/")  # Get disk usage in bytes
        mem = virtual_memory()
        return f"{datetime.now()} # computer_name = {node()}\n" \
               f"{datetime.now()} # system = {system()}\n" \
               f"{datetime.now()} # os_version = {version()}\n" \
               f"{datetime.now()} # processor = {processor()}\n" \
               f"{datetime.now()} # architecture = {machine()}\n" \
               f"{datetime.now()} # processor_core = {cpu_count(logical=False)}\n" \
               f"{datetime.now()} # ip_address = {gethostbyname(gethostname())}\n" \
               f"{datetime.now()} # mac_address = {':'.join(findall('..', '%012x' % getnode()))}\n" \
               f"{datetime.now()} # main_disk_usage = {round(used / 2 ** 30, 1)}/{round(total / 2 ** 30, 1)} GB\n" \
               f"{datetime.now()} # ram = {round(mem[0] / 2 ** 30)} GB"

    def __exit(self):  # exit program
        self.stop = True  # Set exit flag to true
        if self.recording:
            self.__stop_log()  # Stop log
        self.__send_message("exit")
        self.socket.close()  # Close socket
        self.__remove_log()  # Remove log file
        print(">> Exiting...")


def main():
    parser = ArgumentParser(add_help=False)
    parser.add_argument('address', type=str, action='store', help='Address of master')
    parser.add_argument('port', type=int, action='store', help='Listening port of master')
    args = parser.parse_args()

    slave = Slave(args.address, args.port)
    slave.start()


if __name__ == '__main__':
    main()