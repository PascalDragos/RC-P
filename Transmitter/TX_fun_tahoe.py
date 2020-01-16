import threading
import time
from threading import Lock
from TX_fun_encode import *


BUFFER_SIZE = MSS + 8  # 4 segment_number, 1 tip, 2 lungime
TIME_TO_WAIT = 0.8  # Round Trip Time
cwnd = 2  # congestion window pleaca de la 2, pentru ca pentru cwnd 1 am functia recvfrom care e blocanta
sstresh = 30  # slow start threshhold

timers_queue = []
lock = Lock()

segments_in_pipe = 0
sending_done = False

last_ack_of_file = 0
last_ack_of_file_final = 0
flag_retransmit = False

last_segment_transmitted = 2
last_ack_received = 1


def packet_received():
    global cwnd
    global sstresh
    global lock

    lock.acquire()
    if cwnd < sstresh:
        cwnd = cwnd + 1
    else:
        cwnd = cwnd + 1 / cwnd
    lock.release()


def packet_dropped():
    global cwnd
    global sstresh
    global lock
    global flag_retransmit

    flag_retransmit = True
    lock.acquire()
    sstresh = cwnd / 2
    cwnd = 1
    lock.release()



def tahoe_congestion_control(sock, address_port, file_name_to_send):
    global last_ack_of_file
    global last_ack_of_file_final
    print('Trimitem pachetul de start...')
    time.sleep(0.2)

    segment = encode('START', file_name_to_send)
    ack_binary = bytearray([segment[i] for i in range(4)])
    segment_number_sent = int.from_bytes(ack_binary, byteorder='big', signed=False)

    timer_start = threading.Timer(TIME_TO_WAIT, lambda:  sock.sendto(segment, address_port))
    timer_start2 = threading.Timer(TIME_TO_WAIT + 1, lambda:  sock.sendto(segment, address_port))
    sock.sendto(segment, address_port)

    timer_start.start()
    timer_start2.start()


    print('A fost trimis pachetul de start, astept confirmarea primirii...')

    data, addr = sock.recvfrom(BUFFER_SIZE)  # functie blocanta

    timer_start.cancel()
    timer_start2.cancel()

    ack_binary = bytearray([data[i] for i in range(4)])
    segment_number_received = int.from_bytes(ack_binary, byteorder='big', signed=False)
    if segment_number_received == (segment_number_sent + 1):
        print('Am primit ack pentru pachetul de START\n')
        # s-a creat fisierul la destinatie, pot incepe popularea acestuia cu informatii

        # creez thread pentru primirea confirmarilor pachetelor
        TX_RX_thread = threading.Thread(target=TX_RX_fun, args=(sock,address_port))
        TX_RX_thread.start()

        # creez thread pentru trimiterea pachetelor
        TX_TX_thread = threading.Thread(target=TX_TX_fun, args=(sock, address_port, file_name_to_send))
        TX_TX_thread.start()

        # astept ca cele 2 threaduri sa-si fi terminat executia
        TX_RX_thread.join()
        TX_TX_thread.join()
        last_ack_of_file_final = last_ack_of_file
    else:
        print('Am primit ack gresit pentru pachetul de START')
        print('Reincercati conexiunea\n')


def segments_to_list(file_name_to_send):
    list_of_bytes = []
    segment_number = 0
    for segment in encode_bytes(file_name_to_send):
        list_of_bytes.append(segment)
        segment_number += 1
    return list_of_bytes



def TX_RX_fun(sock, address_port):
    global timers_queue
    global segments_in_pipe
    global lock
    global sending_done
    global last_ack_of_file
    global last_ack_received
    global flag_retransmit

    print("TX_read_ack thread started")

    number_ack_duplicate = 0
    last_ack_received += 1  # ack = 2 este pentru pachetul de start (are segment number = 1)
    # mai trebuie sa merg niste iteratii cand sending_done, abia am trimis pachetul end, nu am primit ack de la el
    while sending_done == False or (last_ack_received != last_ack_of_file + 1):
        data, addr = sock.recvfrom(MSS)  # functie blocanta
        ack_received = int.from_bytes(data, byteorder='big', signed=False)   # segment_number este fix data
        #print('last ack received=  {}...'.format(last_ack_received))
        #print('A fost receptionat segment_number = {}... \n'.format(ack_received))

        lock.acquire()
        segments_in_pipe = segments_in_pipe - 1
        lock.release()

        print(f"RX: ack received : last_ack_received {ack_received}  {last_ack_received}")
        if ack_received == last_ack_received + 1:  # am primit un ack bun
            last_ack_received += 1

            lock.acquire()
            try:
                timer = timers_queue.pop(0)
                timer.cancel()
            except :
                pass
            lock.release()

            number_ack_duplicate = 0
            packet_received()

        else:   # am primit acelasi ack
            print('--RX: Ack primit este duplicat...')
            number_ack_duplicate = number_ack_duplicate + 1
            if 3 == number_ack_duplicate:
                number_ack_duplicate = 0
                if not flag_retransmit:
                    flag_retransmit = True
                    print(f'--- 3 ack duplicate!!!')
                    packet_dropped()

    sending_done = False
    print('TX_RX_done')





def TX_TX_fun(sock, address_port, file_name_to_send):
    global timers_queue
    global segments_in_pipe
    global lock
    global sending_done
    global last_ack_of_file
    global flag_retransmit
    global last_ack_received
    global last_ack_of_file_final

    print(f"TX_send thread started sending_done = {sending_done}")
    list_of_bytes = segments_to_list(file_name_to_send)

    i = 0
    while i < len(list_of_bytes) or segments_in_pipe != 0:
        # daca pipe-ul e plin
        # de implementat: porneste un TIMER sa nu astepte la infinit

        while segments_in_pipe >= cwnd and flag_retransmit == False:
            print("TX: pipeline is full")
            time.sleep(0.2)
            #asteapta un ack
        # else
        #   increamenteaza contor si trimite

        if i >= len(list_of_bytes):
            time.sleep(2)
            if segments_in_pipe == 0:
                break


        if (flag_retransmit == True):
            while len(timers_queue) > 0:
                lock.acquire()
                timer = timers_queue.pop(0)
                lock.release()

                timer.cancel()
            time.sleep(0.5)

            lock.acquire()
            segments_in_pipe = 0
            lock.release()

            lock.acquire()
            flag_retransmit = False
            lock.release()

            i = last_ack_received - 2 - last_ack_of_file_final
            print(f"Incep retransmisia cu segment_number = {last_ack_received}")

            # !! retransmite pachete aici
            continue


        segment = list_of_bytes[i]
        tip = segment_decode(segment)['tip']
        if tip == 3:
            lock.acquire()
            sending_done = True
            lock.release()
            last_ack_of_file = segment_decode(segment)['ack']
            print(f'TX: last_ack_of_file = {last_ack_of_file }\n')

        # creez si pornesc timer
        timer = threading.Timer(TIME_TO_WAIT, lambda : packet_dropped(), args=None, kwargs=None)
        timer.start()

        lock.acquire()
        timers_queue.append(timer)
        lock.release()

        lock.acquire()
        segments_in_pipe = segments_in_pipe + 1
        lock.release()

        sock.sendto(segment, address_port)

        segment_number = segment_decode(segment)['ack']
        print(f'TX:Sent segment_number = {segment_number}')

        i += 1

