"""
FYP : 22013

Module:
    Base Data Generator

Description:
    This python script contains the definition of the PermutedListGenerator class
    and its unit tests.

    The PermutedListGenerator class generates the list of permutation of sequence
    of numbers (IDs for vertices). It also exposes methods to store the lists in
    files as well.

"""


# Imports from built-in modules
import numpy as np

class PermutedListGenerator:

    def __init__(self,\
                 start_id=0,\
                 item_cardinality=10,):

        # Stores the start ID for the list of IDs for which the permutation is to be created
        self.start_id = start_id

        # Stores the number of IDs
        self.item_cardinality = item_cardinality

    def generate_permutation(self):
        """
        Description:
            Generates the permutation for the sequential IDs in the form of a list.
        """

        permuted_list = np.random.permutation(self.item_cardinality) + self.start_id
        return permuted_list.tolist()

    def generate_and_save_permuted_list(self, list_type, destination_file):
        """
        Description:
            Generates the permutation for the sequential IDs in the form of a
            list and stores the list in the destination file provided when calling
            the function after first printing the string in list_type.
        """

        lines = list_type + "\n"
        permuted_list = self.generate_permutation()

        for i in permuted_list:
            lines += str(i) + "\n"

        with open(destination_file, mode='w') as out_file:
            out_file.write(lines)
            out_file.close()

        return permuted_list

# Unit tests to test if PermutedListGenerator is generating correctly
def test_permutated_list_generator():
    gen_object = PermutedListGenerator(11, 10)
    test_list = gen_object.generate_permutation()
    print(test_list)
    for i in range(11, 21):
        assert i in test_list,\
            "PermutedListGenerator_LIST_GEN_ERROR"

    test_list = gen_object.generate_permutation()
    print(test_list)
    for i in range(11, 21):
        assert i in test_list,\
            "PermutedListGenerator_LIST_GEN_ERROR"

    gen_object = PermutedListGenerator(25, 100)
    test_list = gen_object.generate_permutation()
    print(test_list)
    for i in range(25, 125):
        assert i in test_list,\
            "PermutedListGenerator_LIST_GEN_ERROR"

# Unit tests to test if PermutedListGenerator is storing the list correctly
def test_list_generate_and_save():
    gen_object = PermutedListGenerator(11, 10)
    gen_object.generate_and_save_permuted_list("Test","test_list.txt")
    print("A file named 'test_list.txt' must have been created, check for issues")
    print("The file should contain permutation of 11 to 20 (inclusive)")

# Function to execute all defined unit tests for PermutedListGenerator
def execute_all_unit_tests():
    test_permutated_list_generator()
    test_list_generate_and_save()
