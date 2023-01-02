"""
FYP : 22013

Module:
    Base Data Generator

Description:
    This python script contains the definition of the MirrorEdgeGenerator class
    and its unit tests.

    The MirrorEdgeGenerator class generates the mirror edges and the remove
    mirror edges by using the friend edge adjacency list (defined as a python
    dictionary).

"""


# Imports from built-in modules
import numpy as np
import threading

class MirrorEdgeGenerator:

    def __init__(self, thread_number=5,\
                 lines_per_thread=1000,\
                 mirror_destination_file="mirror_edge.csv",\
                 remove_mirror_destination_file="remove_mirror_edge.csv",\
                 follower_list=[0,1,2,3],\
                 number_of_friend_edges=2,\
                 number_of_mirror_edges=1,\
                 follower_mirrors_a_friend_probability=0.75,\
                 follower_removes_a_mirror_probability=0.95,\
                 follower_list_mirror_power_dis_param=2,\
                 friend_adjacency_dict={1:[2,],3:[0,],2:[1,],0:[3,]},\
                 lock_list_element_cardinality=5):

        # Number of threads to be used for generating data
        self.thread_number = thread_number

        # Number of lines given to a thread to generate at a time
        self.lines_per_thread = lines_per_thread

        # Destination file to store the generated data for mirror edges
        self.mirror_destination_file = mirror_destination_file

        # Destination file to store the generated data for remove mirror edges
        self.remove_mirror_destination_file = remove_mirror_destination_file

        # Edge ID of the first edge for which the data is being generated
        self.current_start_ID = 0

        # number of friend edges
        self.number_of_friend_edges = number_of_friend_edges

        # Lock for restriciting access to writing file and global adjacency list
        self.file_write_lock = threading.Lock()

        # Lock for acquiring next line batch
        self.next_line_batch_lock = threading.Lock()

        # ID of the last edge to generate the data for
        self.last_valid_edge_ID = number_of_mirror_edges + self.current_start_ID - 1

        # Protects the thread_terminated_count variable defined next
        self.thread_terminate_execution_lock = threading.Lock()

        # Number of threads that have finished execution
        self.thread_terminated_count = 0

        # Semaphore used as a condition variable to indicate all threads have
        # finished execution, i.e, all the data has been generated
        self.main_thread_wait_semaphore = threading.Semaphore(0)

        # Stores the (ordered) follower list
        self.follower_list = follower_list

        # Checking if number of mirror edges < probability of mirroring a friend * number of friends
        assert number_of_mirror_edges < number_of_friend_edges * follower_mirrors_a_friend_probability,\
            "MirrorEdgeGenerator_ERROR: Number of mirror edges must be smaller than (number_of_friend_edges x follower_mirrors_a_friend_probability)"

        # Stores the number of investors
        self.number_of_investors = len(follower_list)

        # Stores the probability that a friend edge also has a mirror edge
        self.follower_mirrors_a_friend_probability = follower_mirrors_a_friend_probability

        # Stores the probability that a mirror edge is removed in the future
        self.follower_removes_a_mirror_probability = follower_removes_a_mirror_probability

        # Stores the follower list mirror power distribution parameter
        self.follower_list_mirror_power_dis_param = follower_list_mirror_power_dis_param

        # Stores the adjacency list for the friend edges in the form of a python dictionary
        self.friend_adjacency_dict = friend_adjacency_dict

        # Stores the adjacency matrix for the mirror edges.
        # It does not store whether the edge exists but if we have already sampled
        # that edge for mirror edge generation (which is supposed to happen without replacement)
        # 0 means the edge has not been checked yet
        # 1 means the edge has already been tested
        self.mirror_adjacency_matrix = np.zeros((self.number_of_investors,self.number_of_investors), dtype=np.int8)

        # stores the size of the lock list to protect the adjacency matrix
        # determines how many vertices in the adjacency matrix should 1 lock in the lock list protect
        self.lock_list_element_cardinality = lock_list_element_cardinality

        #check which lock to acquire by dividing the sampled ID with lock_list_element_granularity
        self.lock_list_element_granularity = self.number_of_investors / lock_list_element_cardinality

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

    def save_mirror_and_remove_mirror_edges_to_file(self, mirror_lines, remove_mirror_lines):
        """
        Description:
            Threads call this function to write their generated data for mirror
            edges and remove mirror edges in the form of strings to the destination
            files. Only one thread can be writing to the files at a time.
        """

        self.file_write_lock.acquire()
        with open(self.mirror_destination_file, mode='a') as out_file:
            out_file.write(mirror_lines)
            out_file.close()

        with open(self.remove_mirror_destination_file, mode='a') as out_file:
            out_file.write(remove_mirror_lines)
            out_file.close()

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

            # string storing the mirror edge lines to be written to the file for this batch
            mirror_lines = ""

            # string storing the remove mirror edge lines to be written to the file for this batch
            remove_mirror_lines = ""

            while generated_edges < batch_size:
                power_distribution_sample = np.random.power(a=self.follower_list_mirror_power_dis_param,\
                                                    size=(1,))[0]

                follower_vertex_id = int(power_distribution_sample * self.number_of_investors)

                if (follower_vertex_id not in self.friend_adjacency_dict):
                    continue
                else:
                    copied_list = self.friend_adjacency_dict[follower_vertex_id].copy()
                    copied_list.append(follower_vertex_id)
                    copied_list.sort()

                    mirror_samples = np.random.uniform(low=0.0, high=1.0, size=(len(copied_list) - 1,))
                    remove_mirror_samples = np.random.uniform(low=0.0, high=1.0, size=(len(copied_list) - 1,))
                    mirror_sample_index = 0

                    #acquiring locks in ascending order for all vertices to prevent deadlock
                    acquired_lock_indices = []
                    for vertex_id in copied_list:
                        lock_index = min(self.lock_list_element_cardinality - 1,\
                                        int(vertex_id / self.lock_list_element_granularity))
                        if (lock_index not in acquired_lock_indices):
                            self.vertex_lock_list[lock_index].acquire()
                            acquired_lock_indices.append(lock_index)

                    for vertex_id in copied_list:
                        if (vertex_id != follower_vertex_id):

                            should_mirror = mirror_samples[mirror_sample_index]
                            should_remove_mirror = remove_mirror_samples[mirror_sample_index]
                            mirror_sample_index += 1

                            source_tradebook_id = follower_vertex_id + self.number_of_investors
                            destination_tradebook_id = vertex_id + self.number_of_investors

                            if (self.mirror_adjacency_matrix[follower_vertex_id][vertex_id] == 1\
                                    or self.mirror_adjacency_matrix[vertex_id][follower_vertex_id] == 1):
                                pass
                            else:
                                #do mirror prob, do remove mirror prob and add to list
                                if (should_mirror < self.follower_mirrors_a_friend_probability):
                                    mirror_lines += str(source_tradebook_id) + "|" + str(destination_tradebook_id) + "\n"
                                    generated_edges += 1

                                    if (should_remove_mirror < self.follower_removes_a_mirror_probability):
                                        remove_mirror_lines += str(source_tradebook_id) + "|" + str(destination_tradebook_id) + "\n"

                                    if (generated_edges >= batch_size):
                                        self.mirror_adjacency_matrix[follower_vertex_id][vertex_id] = 1
                                        self.mirror_adjacency_matrix[vertex_id][follower_vertex_id] = 1
                                        break

                        self.mirror_adjacency_matrix[follower_vertex_id][vertex_id] = 1
                        self.mirror_adjacency_matrix[vertex_id][follower_vertex_id] = 1


                    for lock_index in acquired_lock_indices:
                        self.vertex_lock_list[lock_index].release()

            # after mirror edges for the batch have been generated, store the lines
            self.save_mirror_and_remove_mirror_edges_to_file(mirror_lines, remove_mirror_lines)


    def thread_job(self):
        """
        Description:
            Defines the function of single thread. Once the lines_generator
            function returns, it indicates that the thread has finished
            execution by calling thread_end_execution().
        """

        self.lines_generator()
        self.thread_end_execution()

    def reset_destination_files(self):
        """
        Description:
            This function is called first in the execute method to create the
            destination files (if they don't exist) or resetting the pre-existing
            files with the names.
        """

        with open(self.mirror_destination_file, mode='w') as out_file:
            out_file.write('Mirror Edges\nSourceTradeBookID|DestinationTradeBookID\n')
            out_file.close()

        with open(self.remove_mirror_destination_file, mode='w') as out_file:
            out_file.write('Remove Mirror Edge List\nSourceTradeBookID|DestinationTradeBookID\n')
            out_file.close()

    def execute(self):
        """
        Description:
            Executes the mirror edge generator to generate mirror edges and
            remove mirror edges.
        """

        #reset destination_file, if it exists
        self.reset_destination_files()

        #create and start threads
        for i in range(0, self.thread_number):
            temp_thread_object = threading.Thread(target=self.thread_job, )
            temp_thread_object.start()

        #wait for end_semaphore
        self.main_thread_wait_semaphore.acquire()
        print("Mirror Edge Generation Complete")
        print("Remove Mirror Edge Generation Complete")

# Unit tests to test if MirrorEdgeGenerator is initializing correctly
def test_mirror_edge_generator_init():
    test_object = MirrorEdgeGenerator(thread_number=5,\
                                lines_per_thread=1000,\
                                mirror_destination_file="mirror_edge.csv",\
                                remove_mirror_destination_file="remove_mirror_edge.csv",\
                                number_of_friend_edges=2)

    assert test_object.thread_number == 5,\
        "MirrorEdgeGenerator_INIT_ERROR thread_number unexpectdly changed"

    assert test_object.lines_per_thread == 1000,\
        "MirrorEdgeGenerator_INIT_ERROR lines_per_thread unexpectdly changed"

    assert test_object.current_start_ID == 0,\
        "MirrorEdgeGenerator_INIT_ERROR current_start_ID unexpectdly changed"

    assert test_object.number_of_friend_edges == 2,\
        "MirrorEdgeGenerator_INIT_ERROR number_of_friend_edges unexpectdly changed"

    assert test_object.last_valid_edge_ID == 0,\
        "MirrorEdgeGenerator_INIT_ERROR last_valid_vertex_ID is invalid"

# Unit test to check if mirror edges are generated as expected
def test_generate_mirror_edges():
    test_object = MirrorEdgeGenerator(thread_number=5,\
                 lines_per_thread=5,\
                 mirror_destination_file="mirror_edge.csv",\
                 remove_mirror_destination_file="remove_mirror_edge.csv",\
                 follower_list=[0,1,2,3,4,5,6,7,8],\
                 number_of_friend_edges=6,\
                 number_of_mirror_edges=3,\
                 follower_mirrors_a_friend_probability=0.75,\
                 follower_removes_a_mirror_probability=0.5,\
                 follower_list_mirror_power_dis_param=2,\
                 friend_adjacency_dict={0:[3,2,5,8], 1:[4,7], 2:[0,], 3:[0,], 5:[0,], 8:[0,], 4:[1,], 7:[1,]},\
                 lock_list_element_cardinality=5)
    test_object.execute()

    print("A file named 'mirror_edge.csv' must have been created, check for issues")
    print("A file named 'remove_mirror_edge.csv' must have been created, check for issues")

# Function to execute all defined unit tests for MirrorEdgeGenerator
def execute_all_unit_tests():
    test_mirror_edge_generator_init()
    test_generate_mirror_edges()
