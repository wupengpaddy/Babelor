# coding=utf-8
# Copyright 2019 StrTrek Shanghai Pudong International Airport Team Authors.

# System Required
from multiprocessing import Process
# Outer Required
from Babelor import MSG, URL, CASE, MQ, TEMPLE
# Inner Required
# Global Parameters


def receiver(url):
    myself = TEMPLE(url)
    myself.open(role="receiver")


def main():
    # -————————————------------------------ TEMPLE -------
    temple_url = URL("tcp://127.0.0.1:10001")
    edge_node_url = {
        "inner": URL("tcp://*:10002"),
        "outer": URL("tcp://10.140.0.8:10002"),
    }
    origination_url = URL("oracle://spia_acdm:Wonders@10.28.130.13:1521/orcl")
    destination_url = URL("ftp://pvghbpz:pVGHbpz2018@10.139.140.10#?model=PASV")
    # -————————————------------------------ PROCESS ------
    temple_receiver = Process(target=receiver, args=(temple_url, ))
    temple_receiver.start()
    # -————————————------------------------ MESSAGE -----
    case = CASE("{0}#{1}".format(origination_url, destination_url))
    receiver_msg = MSG()
    receiver_msg.case = case
    receiver_msg.origination = edge_node_url["inner"]
    receiver_msg.destination = destination_url
    receiver_msg.activity = "init"
    # -————————————------------------------ RECEIVER ----
    receiver_init = MQ(temple_url)
    receiver_init.push(receiver_msg)
    receiver_init.close()


if __name__ == '__main__':
    main()
