"""
CPSC 5520, Seattle University
This is an implementation of bully assignment in distributed systems
For Probe Bonus: check probe() and uncomment its call in run()
For Feigning Failure Bonus: check fake_failure() and uncooment its call in run()
Lab2: Bully
:Authors: Noha Nomier
"""

from datetime import datetime
from enum import Enum
import random
import time
import pickle
import sys
import socket
import selectors

BUF_SIZE = 1024  # tcp receive buffer size
CHECK_INTERVAL_SECONDS = 0.2 
ASSUME_FAILURE_TIMEOUT_SECONDS = 2
LISTEN_BACKLOG = 100

class State(Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    #States for probing to not mess with the existent states
    SEND_PROBE = 'PROBE' 
    WAIT_FOR_PROBE_OK = 'WAIT_FOR_PROBE_OK'
    SEND_PROBE_OK = 'OK'  #just as SEND_OK but for clarification in the code

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server

    def is_incoming(self):
        """Categorization helper."""
        return self not in (State.SEND_ELECTION, State.SEND_VICTORY, State.SEND_OK, State.SEND_PROBE)

class Bully():
    def __init__(self, gcd_address, next_birthday, su_id):
        """Constructs a Lab2 Object to talk to the GCD and join peers"""
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birthday - datetime.now()).days
        self.pid = (days_to_birthday, int(su_id))
        self.members = {}
        self.states = {}
        self.bully = None
        self.selector = selectors.DefaultSelector()
        self.listener, self.listener_address = self.start_a_server()
        self.last_probe_time = datetime.now()
        self.last_fake_failure = datetime.now()
        self.is_election_in_progress = False #determines if I am currently in an election or not

    @staticmethod
    def start_a_server():
        """A static method to create a Non blocking listening socket"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('localhost', 0))  # use any free port
        sock.listen(LISTEN_BACKLOG)  
        sock.setblocking(False)
        something = sock.getsockname()
        return sock, something

    def join_group(self):
        """
        Sends a "JOIN" message to the GCD to receive the list of addresses 
        of group peers
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print(f"self: SENDING JOIN to GCD: {self.gcd_address}")
            try: 
                s.connect(self.gcd_address)
            except Exception as e: 
                print(f"Error connecting to GCD server: {e}")

            message_name = "JOIN"
            message_data = (self.pid, self.listener_address)

            try:
                s.sendall(pickle.dumps((message_name, message_data)))
            except socket.error as e: 
                 print (f"Error sending JOIN to GCD: {e}") 

            try:
                server_data = s.recv(BUF_SIZE)
            except socket.error as e:
                print(f"Error receiving data from {self.gcd_address}: {e}")
   
            try:
                self.members = pickle.loads(server_data)
            except (pickle.PickleError, KeyError, EOFError):
                print('Expected a pickled message, got ' + str(server_data)[:100] + '\n')

        print(f"self: Received list of members from gcd: {self.members}")
        
    def run(self):
        """
        The main runner method that contains a selector loop to check for any
        scoekt events and perform the appropriate actions
        """
        self.selector.register(self.listener, selectors.EVENT_READ)
        self.start_election("Just Joined")
        while True:
            events = self.selector.select(CHECK_INTERVAL_SECONDS) 
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)
            self.check_timeouts()
            # self.probe()
            # self.fake_failure()

    def fake_failure(self):
        """
        #Bonus#
        sleeps for a random time every random duration to fake that the
        socket is disabled
        """
        rand_timer = random.randint(0, 10000)
        if self.is_bigger_than_threshold(self.last_fake_failure, rand_timer/1000):
            sleep_duration_seconds = random.randint(1000, 4000)/1000
            print(f'self: I will now sleep for {sleep_duration_seconds} seconds')
            time.sleep(sleep_duration_seconds)
            self.last_fake_failure = datetime.now()

    def start_election(self, reason):
        """
        A method to call for election by sending an election message to all other peers
        @reason: reason for starting an election
        """
        print(f"self ({self.pid}):  starting new election : {reason}")
        bigger_peer_exist = False
        self.is_election_in_progress = True
        for member in self.members:
            # We want to send election message to the higher peers only
            self.set_state(State.WAITING_FOR_OK)
            if member > self.pid:
                peer = self.get_connection(member)
                if peer is None: #peer is disconnected
                    continue
                self.set_state(State.SEND_ELECTION, peer)
                bigger_peer_exist = True
                
        if bigger_peer_exist == False:
            self.declare_victory("I am the biggest bully")

    def accept_peer(self):
        """Accept new TCP/IP conncetions from a peer."""
        try:
            peer, peer_address = self.listener.accept()
            self.set_state(State.WAITING_FOR_ANY_MESSAGE, peer)
        except Exception as e:
            print(f'accept failed to {peer}: {e}')

    def send_message(self, peer):
        """
        Sends a message to the given peer, message title is the
        state value of that peer, message body is the members list
        except for the probe message
        @peer: the peer socket which the message should be sent to
        """
        state = self.get_state(peer)
        message = None

        try:
            if state == State.SEND_VICTORY:
                #We need to only send list with smaller ids to avoid problems when calculating bully
                message_data = self.get_smaller_members()
            elif state == State.SEND_PROBE or state == State.SEND_PROBE_OK:
                message_data = None
            else: 
                message_data = self.members
                
            message = (state.value, message_data)

            print('{}: sending {} [{}]'.format(self.pr_sock(peer), state.value, self.pr_now()))
            peer.sendall(pickle.dumps(message))
        except ConnectionError as e:
            print(f"Connection error while sending {state.value} to {peer} : {e}")
        except Exception as e:
            print(f"Error sending {state.value} to {peer} : {e}")

        if state == State.SEND_ELECTION:
            self.set_state(State.WAITING_FOR_OK, peer, switch_mode=True)
        elif state == State.SEND_PROBE:
            self.set_state(State.WAIT_FOR_PROBE_OK, peer, switch_mode=True)
        else:
            self.set_quiescent(peer)
   
    def get_smaller_members(self):
        """
        return a dictionary of all members with pids smaller than self
        """
        members_less_than_me = {}
        for member_id in self.members:
            if member_id <= self.pid:
               members_less_than_me[member_id] = self.members[member_id]
        return members_less_than_me
                
    def receive_message(self, peer):
        """
        handles message receiving from a given peer and performs 
        the appropriate actions based on the state and type of
        message received
        @peer socket to receive message from
        """
        state = self.get_state(peer)
        response = None
        try:
            response_before_loading = peer.recv(BUF_SIZE)
            response = pickle.loads(response_before_loading)
        except socket.error as e:
            print(f'Error receiving data from {peer} : {e}')
        except (pickle.PickleError, KeyError, EOFError):
            print('Expected a pickled message, got ' + str(response_before_loading)[:100] + '\n')

        message_name = response[0]
        message_body = response[1]

        if message_body is not None:
            self.update_members(message_body)

        print(f'self: received {message_name}')

        if message_name == State.SEND_ELECTION.value:
            self.set_state(State.SEND_OK, peer, switch_mode=True) #send OK in all cases even if an election is in progress
            if self.is_election_in_progress == False:
                self.start_election(reason = "Received an election from another peer")
        elif message_name == State.SEND_PROBE.value:
            self.set_state(State.SEND_PROBE_OK, peer, switch_mode=True)
        elif message_name == State.SEND_VICTORY.value:
            self.set_leader(self.detect_leader(members=message_body) )
            print(f'self: {self.pr_now()}:  Leader is: {self.pr_leader()}\n')
            self.set_quiescent()
            self.set_quiescent(peer)
        elif message_name == state.SEND_OK.value:
            if self.get_state() == State.WAITING_FOR_OK:
                self.set_state(State.WAITING_FOR_VICTOR)
            elif self.get_state() == State.WAIT_FOR_PROBE_OK:
                self.set_quiescent()
            self.set_quiescent(peer)

    def detect_leader(self, members):
        """
            returns elader from a given list of members based on the 
            biggest id
        """
        largest_id = (-1, -1)
        for member_id in members:
            if member_id > largest_id:
                largest_id = member_id
        return largest_id

    def check_timeouts(self):
        """
        This method is used in two cases:
        1- If I sent an election and didn't receive any `OK` after a threshold,
            I should be declared as bully
        2- If I sent a probe message to the current bully and didn't receive an `OK`
            after a threshold, I should start an election to determine the coordinator
        """
        if (self.get_state() == State.WAITING_FOR_OK and 
                len(self.states) > 1 and 
                self.is_bigger_than_threshold(self.states[self][1])):
            self.declare_victory("Didn't receive OK from any peers")
        elif (self.get_state() == State.WAIT_FOR_PROBE_OK and
                self.is_bigger_than_threshold(self.states[self][1])):
            self.start_election("Sent probe message and didn't receive ok")

    def get_connection(self, member):
        """
        Tries to connect with @member and returns the connection socket
        """
        try:
            peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_sock.connect(self.members[member])
        except Exception as e:
            print(f'failed to connect to {member}: {e}')
            return None
        else:
            return peer_sock

    def update_members(self, their_list_of_members):
        """
        Merges @their_list_of_members into the current self.members 
        to update any differences
        """
        if their_list_of_members != None:
            self.members.update(their_list_of_members)
            print(f'Updated list of members: {self.members} \n')

    def probe(self):
        """
        ##Bonus##
        responsible to occasionally send probe message to the current bully
        to determine if it's active or not, if it's not active, we should start a 
        new election
        This message is sent if there's not a current election happening 
        to avoid violating the current states and since we will know the bully from 
        the election anyway
        """
        rand_duration = random.randint(500, 3000)
        if self.is_bigger_than_threshold(self.last_probe_time, rand_duration/1000):
            if (self.bully is not self.pid and self.bully is not None and 
                    self.is_election_in_progress == False 
                    and self.get_state() == State.QUIESCENT):
                peer_conn = self.get_connection(self.bully)
                if peer_conn is None:
                    self.start_election("Bully is not active")
                else:
                    print(f'self: sending PROBE to bully: {self.bully} after {rand_duration/1000} sec')
                    self.set_state(State.WAIT_FOR_PROBE_OK)
                    self.set_state(State.SEND_PROBE, peer_conn)

                self.last_probe_time = datetime.now()

    def set_state(self, state, peer=None, switch_mode=False):
        """
        Sets the @state of the given @peer (self if None) approprietly,
        registers the peer with an appropriate even based on the state type or
        modifies it @switch_mode=True

        """
        if peer is None:
            peer = self
        
        if state.is_incoming():
            event = selectors.EVENT_READ
        else:
            event = selectors.EVENT_WRITE

        if peer != self:
            if switch_mode == True and peer in self.states:
                self.selector.modify(peer, event) 
            else:
                peer.setblocking(False)
                self.selector.register(peer, event)

        self.states[peer] = (state, datetime.now())

    def set_quiescent(self, peer=None):
        """
        Sets the @peer (self if None) to the state QUIESCENT to erase it 
        if it's not self, we need to unregister it from the selector as well
        """
        if peer is not None:
            self.selector.unregister(peer)
        else:
            self.is_election_in_progress = False
            peer = self

        if peer in self.states:
            del self.states[peer]
    
    def is_bigger_than_threshold(self, timestamp, threshold=ASSUME_FAILURE_TIMEOUT_SECONDS):
        """
        Returns whether the time that has passed since the given @timestamp is
        bigger than @threshold or not

        """
        return datetime.now() - timestamp > timedelta(seconds=threshold)

    def get_state(self, peer=None, detail=False):
        """
        Look up current state in state table.

        :param peer: socket connected to peer process (None means self)
        :param detail: if True, then the state and timestamp are both returned
        :return: either the state or (state, timestamp) depending on detail (not found gives (QUIESCENT, None))
        """
        if peer is None:
            peer = self
        status = self.states[peer] if peer in self.states else (State.QUIESCENT, None)
        return status if detail else status[0]

    def set_leader(self, new_leader):
        """ sethe the current bully to @new_leader"""
        self.bully = new_leader

    def declare_victory(self, reason):
        """
        declares victory in an election for self for the given @reason by setting 
        the leader and notifying every lower pid peer with a COORDINATOR message
        """
        print(f'Victory for self {self.pid} to be bully: {reason}\n')
        self.set_leader(self.pid)
        for member in self.members:
            if member < self.pid: 
                peer = self.get_connection(member)
                if peer is None: #peer is disconnected
                    continue
                self.set_state(State.SEND_VICTORY, peer, switch_mode=True)
        self.set_quiescent()

    def pr_leader(self):
        """Printing helper for current leader's name"""
        return 'unknown' if self.bully is None \
            else ('self' if self.bully == self.pid else self.bully)

    def pr_sock(self, sock):
        """
        Printing helper for given socket
        """
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)

    @staticmethod
    def pr_now():
        """printing helper for current timestamp"""
        return datetime.now().strftime('%H:%M:%S.%f')

    @staticmethod
    def cpr_sock(sock):
        """
        Static version of helper for printing given socket
        """
        l_port = sock.getsockname()[1]
        try:
            r_port = sock.getpeername()[1] 
        except OSError:
            r_port = '???'
        return '{}->{} ({})'.format(l_port, r_port, id(sock))

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Please enter valid command i.e python3 bully.py GCDHOST GCDPORT SUID DOB(M-D-Y)")
        exit(1)

    gcd_host = sys.argv[1]
    gcd_port = sys.argv[2]
    su_id = sys.argv[3]
    next_birthday = datetime.strptime(sys.argv[4], '%m-%d-%Y')

    lab2 = Bully((gcd_host, gcd_port), next_birthday, su_id)
    lab2.join_group()
    lab2.run()
  