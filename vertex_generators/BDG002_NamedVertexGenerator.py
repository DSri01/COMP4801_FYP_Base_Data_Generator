"""
FYP : 22013

Module:
    Base Data Generator

Description:
    This python script contains the definition of the NamedVertexGenerator class
    and its unit tests.

    The NamedVertexGenerator class extends the VertexGenerator class to generate
    'name' data for different vertices like: Investors and Companies.

"""


# Imports from built-in modules
import numpy as np
import random

# Importing VertexGenerator from BDG001_VertexGenerator.py
from .BDG001_VertexGenerator import VertexGenerator as BaseVertexGenerator


class NamedVertexGenerator(BaseVertexGenerator):

    def __init__(self, thread_number=5,\
                 lines_per_thread=1000,\
                 destination_file="vertex.csv",\
                 current_start_ID=0,\
                 item_cardinality=10,
                 vertex_type="investor",
                 is_numeric=True):

        super().__init__(thread_number,\
                    lines_per_thread,\
                    destination_file,\
                    current_start_ID,\
                    item_cardinality)

        # Vertex Type for which the names are to be generated
        self.vertex_type = vertex_type

        # is_numeric indicates if the names can have digits in them
        # the characters from which to randomly generate the data are selected
        # accordingly
        if is_numeric:
            self.allowed_character_list = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
        else:
            self.allowed_character_list = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')

    # Overriding the get_vertex_type() method
    def get_vertex_type(self):
        return self.vertex_type

    # Overriding the reset_destination_file() method
    def reset_destination_file(self):
        with open(self.destination_file, mode='w') as out_file:
            out_file.write(self.get_vertex_type()+"ID|Name\n")
            out_file.close()

    # Overriding the lines_generator() method
    def lines_generator(self):

        # Executes until batches no longer exist
        while True:
            start_id, batch_size = self.fetch_next_line_batch()

            #if run out of batches, return
            if ((start_id < 0) or (batch_size <= 0)):
                return

            # the length of each name in this batch
            batch_name_length = random.randrange(16,26)

            # randomly generating a matrix of characters
            batch_names = np.random.choice(self.allowed_character_list, size=(batch_size, batch_name_length))

            # string storing the lines to be written to the file for this batch
            file_lines=""

            # storing file lines in the string
            for investor_id in range(start_id, start_id + batch_size):
                line = str(investor_id)+'|'+''.join(batch_names[investor_id - start_id])
                file_lines += line + "\n"

            # writing the data for this batch to the destination file
            self.save_vertices_to_file(file_lines)


# Unit tests to test if NamedVertexGenerator is initializing correctly
def test_vertex_generator_init():
    test_object = NamedVertexGenerator(thread_number=5,\
                                lines_per_thread=1000,\
                                destination_file="v.csv",\
                                current_start_ID=0,\
                                item_cardinality=10)

    assert test_object.thread_number == 5,\
        "NamedVertexGenerator_INIT_ERROR thread_number unexpectdly changed"

    assert test_object.lines_per_thread == 1000,\
        "NamedVertexGenerator_INIT_ERROR lines_per_thread unexpectdly changed"

    assert test_object.destination_file == "v.csv",\
        "NamedVertexGenerator_INIT_ERROR destination_file unexpectdly changed"

    assert test_object.current_start_ID == 0,\
        "NamedVertexGenerator_INIT_ERROR current_start_ID unexpectdly changed"

    assert test_object.item_cardinality == 10,\
        "NamedVertexGenerator_INIT_ERROR item_cardinality unexpectdly changed"

    assert test_object.last_valid_vertex_ID == 9,\
        "NamedVertexGenerator_INIT_ERROR last_valid_vertex_ID is invalid"

# Unit test to check if the vertex data is generated as expected
def test_generate_vertices():
    test_object = NamedVertexGenerator(thread_number=5,\
                                lines_per_thread=8,\
                                destination_file="named_test.csv",\
                                current_start_ID=67,\
                                item_cardinality=60)
    test_object.execute()
    print("A file named 'named_test.csv' must have been created, check for issues")

# Function to execute all defined unit tests for NamedVertexGenerator
def execute_all_unit_tests():
    test_vertex_generator_init()
    test_generate_vertices()
