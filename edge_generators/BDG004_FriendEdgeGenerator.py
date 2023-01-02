"""
FYP : 22013

Module:
    Base Data Generator

Description:
    This python script contains the definition of the FriendEdgeGenerator class
    and its unit tests.

    The FriendEdgeGenerator class generates the friend edges and its execute
    method returns the friend edge adjacency list (in the form of a dictionary)
    for the MirrorEdgeGenerator to use.

"""


# Imports from built-in modules
import numpy as np
import threading

class FriendEdgeGenerator:

    def __init__(self, thread_number=5,\
                 lines_per_thread=1000,\
                 destination_file="edge.csv",\
                 number_of_friend_edges=2,\
                 follower_list=[0,1,2,3],\
                 leader_list_1=[3,2,1,0],\
                 leader_list_2=[2,1,0,3],\
                 follower_list_friend_power_dis_param=2,\
                 leader_list_1_friend_power_dis_param=2,\
                 leader_list_2_friend_power_dis_param=2,\
                 choose_leader_list_1_as_friend_prob=0.5,\
                 lock_list_element_cardinality=5):

        # Number of threads to be used for generating data
        self.thread_number = thread_number

        # Number of lines given to a thread to generate at a time
        self.lines_per_thread = lines_per_thread

        # Destination file to store the generated data
        self.destination_file = destination_file

        # Edge ID of the first edge for which the data is being generated
        self.current_start_ID = 0

        # Number of friend edges to be generated
        self.number_of_friend_edges = number_of_friend_edges

        # Lock for restriciting access to writing file and global adjacency list
        self.file_write_lock = threading.Lock()

        # Lock for acquiring next line batch
        self.next_line_batch_lock = threading.Lock()

        # ID of the last edge to generate the data for
        self.last_valid_edge_ID = number_of_friend_edges + self.current_start_ID - 1

        # Protects the thread_terminated_count variable defined next
        self.thread_terminate_execution_lock = threading.Lock()

        # Number of threads that have finished execution
        self.thread_terminated_count = 0

        # Semaphore used as a condition variable to indicate all threads have
        # finished execution, i.e, all the data has been generated
        self.main_thread_wait_semaphore = threading.Semaphore(0)

        # Stores the (ordered) follower list
        self.follower_list = follower_list

        # Stores the (ordered) leader list 1
        self.leader_list_1 = leader_list_1

        # Stores the (ordered) leader list 2
        self.leader_list_2 = leader_list_2

        # Performing checks to ensure all three investor lists have the same size
        assert len(follower_list) == len(leader_list_1),\
            "FriendEdgeGenerator_ERROR: FollowerList and LeaderList1 must have same number of elements"

        assert len(leader_list_1) == len(leader_list_2),\
            "FriendEdgeGenerator_ERROR: LeaderList1 and LeaderList2 must have same number of elements"

        # Getting the number of investors
        number_of_investors = len(follower_list)

        # Getting the maximum number of edges that can exist: C(number_of_investors, 2)
        max_edges = (number_of_investors * (number_of_investors - 1)) / 2

        # Checking if the number_of_friend_edges <= maximum possible friend edges
        assert number_of_friend_edges <= max_edges,\
            "FriendEdgeGenerator_ERROR: Number of friend edges must be smaller than C(Number_of_Investors, 2)"

        # Storing the number_of_investors in the object
        self.number_of_investors = number_of_investors

        # Storing the follower list friend power distribution parameter
        self.follower_list_friend_power_dis_param = follower_list_friend_power_dis_param

        # Storing the leader list 1 friend power distribution parameter
        self.leader_list_1_friend_power_dis_param = leader_list_1_friend_power_dis_param

        # Storing the leader list 2 friend power distribution parameter
        self.leader_list_2_friend_power_dis_param = leader_list_2_friend_power_dis_param

        # Storing the probability to choose a leader from leader list 1 as a friend
        self.choose_leader_list_1_as_friend_prob = choose_leader_list_1_as_friend_prob

        # The adjacency list, stored as a dictionary for the friend edges
        self.friend_adjacency_dict = {}

        # The adjacency matrix for the friend edges. 0 means no edge, 1 means an edge exists
        self.friend_adjacency_matrix = np.zeros((number_of_investors,number_of_investors), dtype=np.int8)

        # stores the size of the lock list to protect the adjacency matrix
        # determines how many vertices in the adjacency matrix should 1 lock in the lock list protect
        self.lock_list_element_cardinality = lock_list_element_cardinality

        #check which lock to acquire by dividing the sampled ID with lock_list_element_granularity
        self.lock_list_element_granularity = number_of_investors / lock_list_element_cardinality

        # the lock list that protects the adjacency matrix
        self.vertex_lock_list = [threading.Lock() for i in range(0, self.lock_list_element_cardinality)]



    def fetch_next_line_batch(self):
        """
        Description:
            Threads call this function to get the next batch of lines

        Returns:
            - start ID for the batch and the number of items in the batch
            - (-1, -1) if all batches have finished
        """

        self.next_line_batch_lock.acquire()

        if (self.current_start_ID > self.last_valid_edge_ID):
            self.next_line_batch_lock.release()
            return (-1, -1)

        batch_size = self.lines_per_thread
        assigned_start_ID = self.current_start_ID
        if (self.current_start_ID + batch_size > self.last_valid_edge_ID):
            batch_size = self.last_valid_edge_ID - self.current_start_ID + 1

        self.current_start_ID += batch_size

        self.next_line_batch_lock.release()

        return (assigned_start_ID, batch_size)

    def thread_end_execution(self):
        """
        Description:
            Threads call this function to indicate that they have finished
            execution by updating thread_terminated_count. This happens when
            they get (-1, -1) when calling fetch_next_line_batch(). The last
            thread wakes up the main thread.
        """

        self.thread_terminate_execution_lock.acquire()
        self.thread_terminated_count += 1
        if (self.thread_terminated_count >= self.thread_number):
            self.main_thread_wait_semaphore.release()
        self.thread_terminate_execution_lock.release()

    def save_edges_to_file_and_update_adjacency_list(self, lines, new_adjacency_dict):
        """
        Description:
            Threads call this function to write their generated data in the form
            of a string to the destination file and update the adjacency list (in
            the form of a dictionary). Only one thread can be writing
            to the file at a time and updating the adjacency list at a time.
        """

        self.file_write_lock.acquire()
        with open(self.destination_file, mode='a') as out_file:
            out_file.write(lines)
            out_file.close()

        for vertex in new_adjacency_dict:
            if vertex in self.friend_adjacency_dict:
                self.friend_adjacency_dict[vertex].extend(new_adjacency_dict[vertex])
            else:
                self.friend_adjacency_dict[vertex] = new_adjacency_dict[vertex]

        self.file_write_lock.release()

    def lines_generator(self):
        """
        Description:
            The thread must first acquire a batch and check if it's valid. If the
            batch is not valid, the function must return, otherwise keep
            acquiring batches and generating data and saving it to the
            destination file.
        """

        # Executes until batches no longer exist
        while True:
            start_id, batch_size = self.fetch_next_line_batch()

            #if run out of batches, return
            if ((start_id < 0) or (batch_size <= 0)):
                return

            # stores the number of edges generated for this batch so far
            generated_edges = 0

            # local adjacency dictionary
            current_adjacency_dict = {}

            # string storing the lines to be written to the file for this batch
            file_lines = ""

            while generated_edges < batch_size:
                number_of_edges_to_generate = batch_size - generated_edges

                follower_samples = np.random.power(a=self.follower_list_friend_power_dis_param,\
                                                    size=(number_of_edges_to_generate,)) * self.number_of_investors

                leader1_samples = np.random.power(a=self.leader_list_1_friend_power_dis_param,\
                                                    size=(number_of_edges_to_generate,)) * self.number_of_investors

                leader2_samples = np.random.power(a=self.leader_list_2_friend_power_dis_param,\
                                                    size=(number_of_edges_to_generate,)) * self.number_of_investors

                leader_choice_samples = np.random.uniform(low=0.0, high=1.0, size=(number_of_edges_to_generate,))

                for i in range(0, number_of_edges_to_generate):
                    follower_vertex_id = int(follower_samples[i])

                    leader_vertex_id = -1


                    if (leader_choice_samples[i] < self.choose_leader_list_1_as_friend_prob):
                        leader_vertex_id = int(leader1_samples[i])
                    else:
                        leader_vertex_id = int(leader2_samples[i])

                    # proceed only when both vertex IDs are distinct
                    if (follower_vertex_id == leader_vertex_id):
                        continue

                    # ensures that the smaller id cannot be the source
                    # this protects potential repetition of edges in multiple threads
                    smaller_vertex_id = min(follower_vertex_id, leader_vertex_id)
                    larger_vertex_id = max(follower_vertex_id, leader_vertex_id)

                    lock_index = min(self.lock_list_element_cardinality - 1,\
                                        int(smaller_vertex_id / self.lock_list_element_granularity))

                    self.vertex_lock_list[lock_index].acquire()

                    if (self.friend_adjacency_matrix[smaller_vertex_id][larger_vertex_id] == 1):
                        # edge already exists
                        self.vertex_lock_list[lock_index].release()
                        continue
                    else:
                        self.friend_adjacency_matrix[smaller_vertex_id][larger_vertex_id] = 1

                        # The follower-leader order is preserved in the file, so
                        # if a database supporting directed edges is to be
                        # benchmarked, the directed edges can be stored
                        file_lines += str(follower_vertex_id) + "|" + str(leader_vertex_id) + "\n"

                        # add to local adjacency dictionary
                        if (smaller_vertex_id in current_adjacency_dict):
                            current_adjacency_dict[smaller_vertex_id].append(larger_vertex_id)
                        else:
                            current_adjacency_dict[smaller_vertex_id] = [larger_vertex_id,]

                        if (larger_vertex_id in current_adjacency_dict):
                            current_adjacency_dict[larger_vertex_id].append(smaller_vertex_id)
                        else:
                            current_adjacency_dict[larger_vertex_id] = [smaller_vertex_id,]

                        self.vertex_lock_list[lock_index].release()
                        generated_edges += 1

            # after all the edges for the batch have been generated
            # save the lines in the file and update the global adjacency list
            self.save_edges_to_file_and_update_adjacency_list(file_lines, current_adjacency_dict)


    def thread_job(self):
        """
        Description:
            Defines the function of single thread. Once the lines_generator
            function returns, it indicates that the thread has finished
            execution by calling thread_end_execution().
        """

        self.lines_generator()
        self.thread_end_execution()

    def reset_destination_file(self):
        """
        Description:
            This function is called first in the execute method to create the
            destination file (if it doesn't exist) or resetting the pre-existing
            file with this name.
        """
        
        with open(self.destination_file, mode='w') as out_file:
            out_file.write('Friend Edges\nSourceVertexID|DestinationVertexID\n')
            out_file.close()

    def execute(self):
        """
        Description:
            Executes the friend edge generator to generate friend edges and returns
            the adjacency list in the form of a python dictionary.
        """

        # reset destination_file, if it exists
        self.reset_destination_file()

        # create and start threads
        for i in range(0, self.thread_number):
            temp_thread_object = threading.Thread(target=self.thread_job, )
            temp_thread_object.start()

        # wait for end_semaphore
        self.main_thread_wait_semaphore.acquire()
        print("Friend Edge Generation Complete")

        #returns the adjacency list in the form of a python dictionary
        return self.friend_adjacency_dict

# Unit tests to test if FriendEdgeGenerator is initializing correctly
def test_friend_edge_generator_init():
    test_object = FriendEdgeGenerator(thread_number=5,\
                                lines_per_thread=1000,\
                                destination_file="v.csv",\
                                number_of_friend_edges=2)

    assert test_object.thread_number == 5,\
        "FriendEdgeGenerator_INIT_ERROR thread_number unexpectdly changed"

    assert test_object.lines_per_thread == 1000,\
        "FriendEdgeGenerator_INIT_ERROR lines_per_thread unexpectdly changed"

    assert test_object.destination_file == "v.csv",\
        "FriendEdgeGenerator_INIT_ERROR destination_file unexpectdly changed"

    assert test_object.current_start_ID == 0,\
        "FriendEdgeGenerator_INIT_ERROR current_start_ID unexpectdly changed"

    assert test_object.number_of_friend_edges == 2,\
        "FriendEdgeGenerator_INIT_ERROR number_of_friend_edges unexpectdly changed"

    assert test_object.last_valid_edge_ID == 1,\
        "FriendEdgeGenerator_INIT_ERROR last_valid_vertex_ID is invalid"


# Unit test to check if friend edges are generated as expected
def test_generate_friend_edges():
    test_object = FriendEdgeGenerator( thread_number=5,\
                 lines_per_thread=5,\
                 destination_file="friend_edge_test1.csv",\
                 number_of_friend_edges=15,\
                 follower_list=[0,1,2,3,4,5,6],\
                 leader_list_1=[3,2,1,0,6,5,4],\
                 leader_list_2=[6,1,0,3,2,4,5],\
                 follower_list_friend_power_dis_param=2,\
                 leader_list_1_friend_power_dis_param=2,\
                 leader_list_2_friend_power_dis_param=2,\
                 choose_leader_list_1_as_friend_prob=0.5,\
                 lock_list_element_cardinality=5)
    adjacency_list = test_object.execute()

    # friend adjacency dict size = 2 * number of friend edges
    edge_count=0
    for i in adjacency_list:
        edge_count += len(adjacency_list[i])

    assert edge_count == 30,\
        "FriendEdgeGenerator_GEN_ERROR Wrong number of edges generated"

    print("A file named 'friend_edge_test1.csv' must have been created, check for issues")
    print("The adjacency list is as follows:")
    print(adjacency_list)

    test_object = FriendEdgeGenerator( thread_number=5,\
                 lines_per_thread=10,\
                 destination_file="friend_edge_test2.csv",\
                 number_of_friend_edges=1000,\
                 follower_list=np.random.permutation(1000).tolist(),\
                 leader_list_1=np.random.permutation(1000).tolist(),\
                 leader_list_2=np.random.permutation(1000).tolist(),\
                 follower_list_friend_power_dis_param=2,\
                 leader_list_1_friend_power_dis_param=2,\
                 leader_list_2_friend_power_dis_param=2,\
                 choose_leader_list_1_as_friend_prob=0.5,\
                 lock_list_element_cardinality=5)
    adjacency_list = test_object.execute()

    # friend adjacency dict size = 2 * number of friend edges
    edge_count=0
    for i in adjacency_list:
        edge_count += len(adjacency_list[i])

    assert edge_count == 2000,\
        "FriendEdgeGenerator_GEN_ERROR Wrong number of edges generated"

    print("A file named 'friend_edge_test2.csv' must have been created, check for issues")
    print("The adjacency list is as follows:")
    print(adjacency_list)

# Function to execute all defined unit tests for FriendEdgeGenerator
def execute_all_unit_tests():
    test_friend_edge_generator_init()
    test_generate_friend_edges()
