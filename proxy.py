from email import header
import http
from socket import * 
from urllib.parse import urlparse 
import sys 
from pathlib import Path 

BUFF_SIZE = 1024
NEW_LINE_SEPARATOR = "\r\n"
HTTP_VERSION = 'HTTP/1.1'
INVALID_INPUT_STRING = 'INVALID_INPUT'
NOT_SUPPORTED_STRING = 'NOT SUPPORTED'
VALID_STRING = 'VALID'
COLON_SEPARATOR = ':'
LOCAL_HOST_IP = "127.0.0.1"
CACHE_PATH = "./cache/"

class HttpRequestInfo():

    def __init__(self, client_info, method: str, requested_host: str,
                 requested_port: int,
                 requested_path: str,
                 headers: dict):
        self.method = method
        self.client_address_info = client_info
        self.requested_host = requested_host
        self.requested_port = requested_port
        self.requested_path = requested_path
        self.headers = headers

    def to_http_string(self):
        message = ""
        request_line = self.method + " " + self.requested_path + " " + HTTP_VERSION + NEW_LINE_SEPARATOR
        headers_str =  NEW_LINE_SEPARATOR.join([": ".join([header_name, header_value]) for header_name, header_value in self.headers.items()])

        message = message + request_line + headers_str + NEW_LINE_SEPARATOR + NEW_LINE_SEPARATOR
        return message

    def to_byte_array(self, http_string):
        """
        Converts an HTTP string to a byte array.
        """
        return bytes(http_string, "UTF-8")

    def construct_message(self):
        return self.to_byte_array(self.to_http_string())

class HttpProxy():
    def __init__(self, port):
        self.proxy_port = port

    def entry_point(self):
        self.setup_sockets()

    def setup_sockets(self):
        print(f"Starting HTTP proxy on port: {self.proxy_port}")
        with socket(AF_INET, SOCK_STREAM) as proxy:
            proxy.bind((LOCAL_HOST_IP, int(self.proxy_port)))
            proxy.listen()
            self.get_connection(proxy)
    
    def get_connection(self, proxy):
        conn, client_addr = proxy.accept()
        print(f"Connecting to client with address: {client_addr}")
        request_buffer = self.receive_data(conn)

        conn.sendall(self.parse_http_request(client_addr, request_buffer))
        conn.close() # Question: Should I close connection

    def check_http_request_validity(self, http_raw_data):
        methods=['HEAD', 'POST', 'PUT','DELETE','CONNECT','OPTIONS','TRACE','PATCH']
        data_lines = http_raw_data.split(NEW_LINE_SEPARATOR)
        first_line = data_lines[0].split()
        if len(first_line)!= 3 or HTTP_VERSION not in first_line:
            return INVALID_INPUT_STRING

        method = first_line[0]
        url = first_line[1]
    
        # 0 for absolute and 1 for relative 
        # url_type = 0

        
        # if url[0]=='/':
        #     url_type=1

        
        # if url_type==1 and 'Host: ' not in data_lines[1]:
        #     return HttpRequestState.INVALID_INPUT

        # for i in range(1,len(data_lines)-2):
        #     if data_lines[i]=='':
        #         continue

        #     if ': ' not in data_lines[i]:
        #         return HttpRequestState.INVALID_INPUT
        
    
        if method in methods :
            return NOT_SUPPORTED_STRING
        elif method != 'GET':
            return INVALID_INPUT_STRING
    
        return VALID_STRING

    def parse_http_request(self, client_addr, http_raw_data):
        data_lines = http_raw_data.split(NEW_LINE_SEPARATOR)
        first_line = data_lines[0].strip().split()
        method = first_line[0]
        url = first_line[1]
        headers = {}


        if url[0]=='/':
            requested_path = url
        else:
            requested_host,requested_path = parse_url(url)
            port,requested_host=check_port(requested_host)
    
        host_header_exist = False

        # dict(item.split("=") for item in s.split(";"))  # USE THIS INSTEAD 
        for i in range(1, len(data_lines)):
            if not data_lines[i]:
                continue

            header_line = data_lines[i].strip().split(COLON_SEPARATOR)
            if header_line[0]=='Host':
                host_header_exist = True

            headers[header_line[0].strip()] = header_line[1].strip()

        parsing = urlparse(url)
        host_name = parsing.hostname
        headers['Connection'] = 'close'
        headers['Host'] = host_name
        relative_path = parsing.path

        http_request = HttpRequestInfo(client_addr, method, host_name, 80, relative_path, headers)
        print(headers)
        print(host_header_exist)
        message = http_request.construct_message()
        print(http_request.construct_message())
        http_request.display()
        s_from_proxy_to_server = socket(AF_INET, SOCK_STREAM)
        s_from_proxy_to_server.connect((http_request.requested_host, http_request.requested_port))
        print(f"message is {message}")
        s_from_proxy_to_server.sendall(message)
        # conn.sendto(correct_message,(response_msg.requested_host, response_msg.requested_port))
        data = self.receive_html(s_from_proxy_to_server)
        # client_cache[response_msg.requested_host+response_msg.requested_path]=data
        print("Data received from server :\n",data.decode('UTF-8'))
        self.write_to_file(data.decode('UTF-8'), http_request.requested_path)
        s_from_proxy_to_server.close()
        return data

    def receive_html(self, s_from_proxy_to_server):
        data = b''
        while True:
            print("BEFORE")
            try:
                data_from_server_to_proxy = s_from_proxy_to_server.recv(1024)
            except Exception as e:
                print(f'e {e}')
            print(f'after {data_from_server_to_proxy}')
            if len(data_from_server_to_proxy) <= 0:
                break 
            data += data_from_server_to_proxy
        print("#DEBUG DID WE REACH THIS POINT")
        return data
        
    def receive_data(self, conn):
        request_buffer = ''
        last_received = ''
        data = ''
        while data != NEW_LINE_SEPARATOR and last_received != NEW_LINE_SEPARATOR:
            data = conn.recv(BUFF_SIZE)
            data = data.decode('UTF-8')
            request_buffer += data
            last_received = data
            if not data: break

        print(f"Received a message from client: {request_buffer}\n")
        return request_buffer
    
    def write_to_file(self, content, file_name):
        path = Path(CACHE_PATH + file_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding ="utf-8") as f:
            f.write(content)

def main():
    if len(sys.argv) != 2:
        print("Please enter valid command i.e python3 proxy.py PROXY_PORT")
        exit(1)

    proxy_port = int(sys.argv[1])
    proxy = HttpProxy(proxy_port)
    proxy.entry_point()

if __name__ == "__main__":
    main()