from platform import node, version, system, processor, machine
from shutil import disk_usage
from time import sleep
from argparse import ArgumentParser
from datetime import datetime
from socket import socket, gethostbyname, gethostname
from threading import Thread
from re import findall
from uuid import getnode
from psutil import cpu_count, virtual_memory
from os import remove
import subprocess
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
                    elif len_msg == 7:
                        if msg[0] == "load":  # If msg is a load operation, will load database with
                            self.__load_data(msg)


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

    def __load_data(self, data:list):
        # Shell = True can be a security hazard if combined with untrusted input
        output = subprocess.call("./ycsb_script.sh", shell=True)
        print(output)

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