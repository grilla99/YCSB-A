from master import *
from worker import *
import time


def main():
    master = Master(address="", port=50000)
    master.start()

if __name__ == "__main__":
    main()
