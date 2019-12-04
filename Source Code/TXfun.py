DEFAULT_SIZE = 1
ack = 11


# a = int.from_bytes(segment_number, byteorder='big', signed=False)
# print(a)

def incrementAck():
    global ack
    ack = ack + 1

#impachetarea primului segment care va contine numele si extensia
def encode_start(segment_data):
    incrementAck()
    segment_number = ack.to_bytes(4, byteorder='big', signed=False)
    segment_type = b'\x01'
    segment_data_len = len(segment_data)
    segment_len = segment_data_len.to_bytes(2, byteorder='big', signed=False)
    segment = segment_number + segment_type + segment_len + segment_data  # impachetarea efectiva
    return segment


#impachetarea unui segment din interiorul fisierului
def encode_data(segment_data):
    incrementAck()
    segment_number = ack.to_bytes(4, byteorder='big', signed=False)
    segment_type = b'\x02'
    segment_data_len = DEFAULT_SIZE
    segment_len = segment_data_len.to_bytes(2, byteorder='big', signed=False)
    segment = segment_number + segment_type + segment_len + segment_data
    return segment


#impachetarea ulitmului segment
#in campul de segment_code, al doilea octet va fi lungimea caracterelor utile
def encode_end(segment_data):
    incrementAck()
    segment_number = ack.to_bytes(4, byteorder='big', signed=False)
    segment_type = b'\x03'
    segment_data_len = len(segment_data)
    segment_data += segment_data + "0"*(DEFAULT_SIZE - segment_data_len)
    segment_len = segment_data_len.to_bytes(2, byteorder='big', signed=False)
    segment = segment_number + segment_type + segment_len + segment_data
    return segment


def encode_error(segment_data):
    pass


segment_type = {
        'START': encode_start,
        'DATA': encode_data,
        'END': encode_end
    }


def encode(tip, data):
    return segment_type.get(tip, encode_error)(data)  # un fel de switch


#citirea fisier ca pachete de octeti
def bytes_from_file(filename, chunk_size=DEFAULT_SIZE):
    with open(filename, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if chunk:
                yield chunk
            else:
                break


#codificarea pachetelor de octeti
def encode_bytes(filename):
    for b in bytes_from_file(filename):
        if len(b) == DEFAULT_SIZE:
            yield encode('DATA', b)
        else:
            #yield encode('END', b)
            yield encode('DATA', b)