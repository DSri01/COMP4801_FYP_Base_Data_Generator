"""
FYP : 22013

Module:
    Base Data Generator

Description:
    This python script contains the definition of the VertexGenerator class and
    its unit tests.

    The VertexGenerator class is the base class for NamedVertexGenerator and
    NumberedVertexGenerator classes. These classes extend the VertexGenerator to
    provide their functionalities.

"""

# Imports from built-in modules
import threading

class VertexGenerator:
    """
    Description:
        Contains functionality to generate vertex data in multithreaded fashion.
        Subclasses need only to define the following methods:

            - lines_generator(): defining the function of a single thread
            - get_vertex_type(): returns the type of the vertex
            - reset_destination_file(): resets the destination file
    """

    def __init__(self, thread_number=5,\
                 lines_per_thread=1000,\
                 destination_file="vertex.csv",\
                 current_start_ID=0,\
                 item_cardinality=10):

        # Number of threads to be used for generating data
        self.thread_number = thread_number

        # Number of lines given to a thread to generate at a time
        self.lines_per_thread = lines_per_thread

        # Destination file to store the generated data
        self.destination_file = destination_file

        # Vertex ID of the first vertex for which the data is being generated
        self.current_start_ID = current_start_ID

        # Number of vertices for which the data is to be generated
        self.item_cardinality = item_cardinality

        # Lock for restriciting access to writing file
        self.file_write_lock = threading.Lock()

        # Lock for acquiring next line batch
        self.next_line_batch_lock = threading.Lock()

        # ID of the last vertex to generate the data for
        self.last_valid_vertex_ID = item_cardinality + current_start_ID - 1

        # Protects the thread_terminated_count variable defined next
        self.thread_terminate_execution_lock = threading.Lock()

        # Number of threads that have finished execution
        self.thread_terminated_count = 0

        # Semaphore used as a condition variable to indicate all threads have
        # finished execution, i.e, all the data has been generated
        self.main_thread_wait_semaphore = threading.Semaphore(0)


    def fetch_next_line_batch(self):
        """
        Description:
            Threads call this function to get the next batch of lines

        Returns:
            - start ID for the batch and the number of items in the batch
            - (-1, -1) if all batches have finished
        """

        self.next_line_batch_lock.acquire()

        if (self.current_start_ID > self.last_valid_vertex_ID):
            self.next_line_batch_lock.release()
            return (-1, -1)

        batch_size = self.lines_per_thread
        assigned_start_ID = self.current_start_ID
        if (self.current_start_ID + batch_size > self.last_valid_vertex_ID):
            batch_size = self.last_valid_vertex_ID - self.current_start_ID + 1

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


    def save_vertices_to_file(self, lines):
        """
        Description:
            Threads call this function to write their generated data in the form
            of a string to the destination file. Only one thread can be writing
            to the file at a time.
        """

        self.file_write_lock.acquire()
        with open(self.destination_file, mode='a') as out_file:
            out_file.write(lines)
            out_file.close()
        self.file_write_lock.release()

    def lines_generator(self):
        """
        Description:
            This function is to be overriden by the subclasses. The thread must
            first acquire a batch and check if it's valid. If the batch is not
            valid, the function must return, otherwise keep acquiring batches
            and generating data and saving it to the destination file.
        """
        pass

    def thread_job(self):
        """
        Description:
            Defines the function of single thread. Once the lines_generator
            function returns, it indicates that the thread has finished
            execution by calling thread_end_execution().
        """
        self.lines_generator()
        self.thread_end_execution()

    def get_vertex_type(self):
        """
        Description:
            This function is to be overriden by the subclass for their function.
        """
        return "Base"

    def reset_destination_file(self):
        """
        Description:
            This function is to be overriden by the subclass for their function.
            This function is called first in the execute method to create the
            destination file (if it doesn't exist) or resetting the pre-existing
            file with this name.
        """
        with open(self.destination_file, mode='w') as out_file:
            out_file.write('')
            out_file.close()

    def execute(self):
        """
        Description:
            Executes the Vertex Data generator for VertexGenerator and its
            subclasses.
        """
        #reset destination_file, if it exists
        self.reset_destination_file()

        #create and start threads
        for i in range(0, self.thread_number):
            temp_thread_object = threading.Thread(target=self.thread_job, )
            temp_thread_object.start()

        #wait for end_semaphore
        self.main_thread_wait_semaphore.acquire()
        print(self.get_vertex_type(),"Vertex Data Generation Complete")



# Unit tests to test if VertexGenerator is initializing correctly
def test_vertex_generator_init():
    test_object = VertexGenerator(thread_number=5,\
                                lines_per_thread=1000,\
                                destination_file="v.csv",\
                                current_start_ID=0,\
                                item_cardinality=10)

    assert test_object.thread_number == 5,\
        "VertexGenerator_INIT_ERROR thread_number unexpectdly changed"

    assert test_object.lines_per_thread == 1000,\
        "VertexGenerator_INIT_ERROR lines_per_thread unexpectdly changed"

    assert test_object.destination_file == "v.csv",\
        "VertexGenerator_INIT_ERROR destination_file unexpectdly changed"

    assert test_object.current_start_ID == 0,\
        "VertexGenerator_INIT_ERROR current_start_ID unexpectdly changed"

    assert test_object.item_cardinality == 10,\
        "VertexGenerator_INIT_ERROR item_cardinality unexpectdly changed"

    assert test_object.last_valid_vertex_ID == 9,\
        "VertexGenerator_INIT_ERROR last_valid_vertex_ID is invalid"

# Unit tests to test if fetch_next_line_batch() works as expected
def test_fetch_next_line_batch():
    test_object = VertexGenerator(thread_number=5,\
                                lines_per_thread=10,\
                                destination_file="v.csv",\
                                current_start_ID=0,\
                                item_cardinality=25)

    start, batch_size = test_object.fetch_next_line_batch()

    assert start == 0,\
        "VertexGenerator_LINE_BATCH_ERROR 1st start index is wrong"

    assert batch_size == 10,\
        "VertexGenerator_LINE_BATCH_ERROR 1st batch size is wrong"

    start, batch_size = test_object.fetch_next_line_batch()

    assert start == 10,\
        "VertexGenerator_LINE_BATCH_ERROR 2nd start index is wrong"

    assert batch_size == 10,\
        "VertexGenerator_LINE_BATCH_ERROR 2nd batch size is wrong"

    start, batch_size = test_object.fetch_next_line_batch()

    assert start == 20,\
        "VertexGenerator_LINE_BATCH_ERROR 3rd start index is wrong"

    assert batch_size == 5,\
        "VertexGenerator_LINE_BATCH_ERROR 3rd batch size is wrong"

    start, batch_size = test_object.fetch_next_line_batch()

    assert start == -1,\
        "VertexGenerator_LINE_BATCH_ERROR 4th start index is wrong"

    assert batch_size == -1,\
        "VertexGenerator_LINE_BATCH_ERROR 4th batch size is wrong"

    start, batch_size = test_object.fetch_next_line_batch()

    assert start == -1,\
        "VertexGenerator_LINE_BATCH_ERROR 5th start index is wrong"

    assert batch_size == -1,\
        "VertexGenerator_LINE_BATCH_ERROR 5th batch size is wrong"

    assert test_object.current_start_ID == test_object.last_valid_vertex_ID + 1,\
        "VertexGenerator_LINE_BATCH_ERROR end start_ID is incorrect"


# Function to execute all defined unit tests for VertexGenerator
def execute_all_unit_tests():
    test_vertex_generator_init()
    test_fetch_next_line_batch()
