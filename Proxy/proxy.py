"""
Computer Networks, A Simple HTTP Web Proxy
:Authors: Noha Nomier
"""
from socket import *
from urllib.parse import urlparse
import sys
from pathlib import Path

BUFF_SIZE = 2048
NEW_LINE_SEPARATOR = "\r\n"
HTTP_VERSION = 'HTTP/1.1'
COLON_SEPARATOR = ':'
LOCAL_HOST_IP = "127.0.0.1"
CACHE_PATH = "./cache/"
CACHE_HIT_STR = 'Cache-Hit: 1' + NEW_LINE_SEPARATOR
CACHE_MISS_STR = 'Cache-Hit: 0' + NEW_LINE_SEPARATOR
INTERNAL_ERROR_RESPONSE = 'HTTP/1.1 500 Internal Error'

class HttpProxy():
    def __init__(self, port):
        """ Constructs an HTTP Proxy that listens on the given port """
        self.proxy_port = port

    def entry_point(self):
        """ Proxy starting Point"""
        self.setup_sockets()

    def setup_sockets(self):
        """ 
        Creates a TCP socket that listen on the given port and waits for 
        connections from clients
        """
        print(f"Starting HTTP proxy on port: {self.proxy_port}")
        with socket(AF_INET, SOCK_STREAM) as proxy:
            proxy.bind((LOCAL_HOST_IP, int(self.proxy_port)))
            proxy.listen()
            self.get_connection(proxy)

    def get_connection(self, proxy):
        """
        Keeps listening for client connections,
        receives client request and return back appropriate response
        """
        while True:
            print("*" * 25 + " Ready to serve... " + "*" * 25 + "\n")
            conn, client_addr = proxy.accept()
            print(f"Connecting to client with address: {client_addr}")
            request_buffer = self.receive_data(conn)
            response = b''
            if (self.is_http_request_invalid(request_buffer) == True):
                print(f'Request sent from client is invalid ...')
                response = bytes(
                    'Invalid Request, Please Try Again \n', 'UTF-8')
            else:
                response = self.handle_http_request(request_buffer)

            print("Now sending response back to client..")
            conn.sendall(response)
            print("All Done! Closing connection...")
            conn.close()

    def is_http_request_invalid(self, http_raw_data):
        """
        Checks if the request sent from the client is valid by making sure it is a `GET` 
        request, contains an ABSOLUTE url and has HTTP version 1.1

        """
        data_lines = http_raw_data.strip().split(NEW_LINE_SEPARATOR)
        first_line = data_lines[0].strip().split()
        return (len(first_line) != 3 or HTTP_VERSION not in first_line or
                first_line[0].strip() != 'GET' or
                first_line[1].strip().startswith("/"))

    def construct_http_request(self, method, requested_path, headers):
        """
        Given @method, @requested_path:relative path needed, @headers: dictionary of headers
        returns a byte array containaing the entire request body to be sent to the server
        """
        message = ""
        request_line = method + " " + requested_path + \
            " " + HTTP_VERSION + NEW_LINE_SEPARATOR
        headers_str = NEW_LINE_SEPARATOR.join([": ".join([header_name.strip(
        ), header_value.strip()]) for header_name, header_value in headers.items()])

        message = message + request_line + headers_str + \
            NEW_LINE_SEPARATOR + NEW_LINE_SEPARATOR
        return self.to_bytes(message)

    def handle_http_request(self, http_raw_data):
        """
        Takes client request @http_raw_data and parses it by retrieving:
        host_name, host_port, relative_path from the url.
        Checks if the requested url is cached, then it returns it immediately,
        otherwise, sends a request to the requested server to retrieve 
        the desired response and returns it
        """
        data_lines = http_raw_data.split(NEW_LINE_SEPARATOR)
        first_line = data_lines[0].strip().split()
        method = first_line[0]
        url = first_line[1]

        parsed_url = urlparse(url)
        host_name = parsed_url.hostname
        relative_path = parsed_url.path
        host_port = parsed_url.port
        if host_port is None:
            host_port = 80

        # Check for cache in here, We assume that url with different ports are always different requests
        cache_entry_path = CACHE_PATH + host_name + \
            f'/{str(host_port)}' + relative_path

        if Path(cache_entry_path).exists():
            print("Requested data is found in cache and will be sent to client!")
            file_data = self.read_file(cache_entry_path)
            return file_data

        print(f"No cache hit, sending request to the server {(host_name, host_port)}")

        headers = dict(line.split(COLON_SEPARATOR) for line in data_lines[1:] if line != '')
        headers['Host'] = host_name
        headers['Connection'] = 'close'

        http_request_to_server = self.construct_http_request(
            method, relative_path, headers)

        return self.get_response_from_server(host_name, host_port, http_request_to_server, cache_entry_path)

    def get_response_from_server(self, host_name, host_port, http_request_to_server, cache_entry_path):
        """
        Sends @http_request_to_server with address (@host_name, @host_port),
        Caches the response in a file in @cache_entry_path and returns it or
        in the case of an error returns a failure response instead
        """
        print(f"Request message being sent to server: {http_request_to_server}")

        received_data = ''

        with socket(AF_INET, SOCK_STREAM) as s_from_proxy_to_server:
            try:
                s_from_proxy_to_server.connect((host_name, host_port))
                s_from_proxy_to_server.sendall(http_request_to_server)
            except Exception as e:
                print(f'Failed to connect to {(host_name, host_port)}: {e}')
                return b'Failed to connect to server host \n\r'
            else:
                received_data = self.receive_server_response(s_from_proxy_to_server)
                decoded_received_data = received_data.decode('UTF-8')
                received_data_lines = decoded_received_data.split(NEW_LINE_SEPARATOR)

                first_response_line = received_data_lines[0]
                remaining_server_response = NEW_LINE_SEPARATOR.join(received_data_lines[1:])
                content_lines = NEW_LINE_SEPARATOR + NEW_LINE_SEPARATOR.join(decoded_received_data.split(NEW_LINE_SEPARATOR + NEW_LINE_SEPARATOR)[1:])
                status_code = first_response_line.split()[1]

                if status_code == '404':
                    print(f"Status code is {status_code}, No Cache Writing")
                    return self.to_bytes(received_data_lines[0] + NEW_LINE_SEPARATOR + CACHE_MISS_STR + content_lines + NEW_LINE_SEPARATOR)
                elif status_code != '200':
                    print(f"Status code is not 200 or 404, No Cache Writing")
                    return self.to_bytes(INTERNAL_ERROR_RESPONSE + NEW_LINE_SEPARATOR + CACHE_MISS_STR + content_lines + NEW_LINE_SEPARATOR)

                print(f"Status code 200, Now writing to file...")
                content_of_file = received_data_lines[0] + NEW_LINE_SEPARATOR + \
                    CACHE_HIT_STR + content_lines + NEW_LINE_SEPARATOR
                self.write_to_file(content_of_file, cache_entry_path)
                s_from_proxy_to_server.close()
            
            return self.to_bytes(received_data_lines[0] + NEW_LINE_SEPARATOR + CACHE_MISS_STR + remaining_server_response + NEW_LINE_SEPARATOR)

    @staticmethod
    def to_bytes(text):
        """
        Converts a given string into bytes array UTF-8 encoding
        """
        return bytes(text, 'UTF-8')

    def receive_server_response(self, s_from_proxy_to_server):
        """
        Handles content received from server and returns it
        """
        data = b''
        while True:
            try:
                data_from_server_to_proxy = s_from_proxy_to_server.recv(BUFF_SIZE)
            except Exception as e:
                print(f'e {e}')
            if len(data_from_server_to_proxy) <= 0:
                break
            data += data_from_server_to_proxy
        return data

    def receive_data(self, conn):
        """
        Returns request buffer received from the client
        """
        request_buffer = ''
        last_received = ''
        data = ''
        while data != NEW_LINE_SEPARATOR and last_received != NEW_LINE_SEPARATOR:
            data = conn.recv(BUFF_SIZE)
            data = data.decode('UTF-8')
            request_buffer += data
            last_received = data
            if not data:
                break

        print(f"Received a message from client: {request_buffer}")
        return request_buffer

    def write_to_file(self, content, file_path):
        """
        Writes @content to the @file_path given using pathlib
        """
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            f.write(content)

    def read_file(self, full_path):
        """
        Reads file contents of file at @full_path, this is only called if the file exists
        """
        return Path(full_path).read_bytes()


def main():
    if len(sys.argv) != 2:
        print("Please enter valid command i.e python3 proxy.py PROXY_PORT")
        exit(1)

    proxy_port = int(sys.argv[1])
    proxy = HttpProxy(proxy_port)
    proxy.entry_point()


if __name__ == "__main__":
    main()
