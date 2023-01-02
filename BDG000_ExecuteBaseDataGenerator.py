"""
FYP : 22013

Module:
    Base Data Generator

Description:
    This python script executes the necessary code in the Base Data Generator
    module to generate all data for the benchmark at once using the provided
    configuration in the configuration file.

"""

# Imports from built-in modules
import multiprocessing as mp
import sys
import threading

# Imports from Base Data Generator Module
from BDG007_Configuration import Configuration
import edge_generators.BDG004_FriendEdgeGenerator as FEG
import edge_generators.BDG005_MirrorEdgeGenerator as MEG
import list_generators.BDG006_PermutedListGenerator as PLG
import vertex_generators.BDG002_NamedVertexGenerator as NamedVG
import vertex_generators.BDG003_NumberedVertexGenerator as NumberedVG

sys.path.append("vertex_generators/")

# Generates Investor Names
def generate_investor_names(config_obj):

    # Initializing the data generator
    generator_obj = NamedVG.NamedVertexGenerator(thread_number=10,\
                                                    lines_per_thread=80,\
                                                    destination_file = config_obj.investor_name_file_name,\
                                                    current_start_ID = 0,\
                                                    item_cardinality = config_obj.number_of_investors,\
                                                    vertex_type="investor",
                                                    is_numeric=True)
    # Executing the data generator
    generator_obj.execute()
    # End of generate_investor_names

# Generates TradeBook Investment Amounts
def generate_tradebook_investment_amount(config_obj):

    # Initializing the data generator
    generator_obj = NumberedVG.NumberedVertexGenerator(thread_number=10,\
                                                         lines_per_thread=1000,\
                                                         destination_file=config_obj.tradebook_investment_amount_file_name,\
                                                         current_start_ID=config_obj.number_of_investors,\
                                                         item_cardinality=config_obj.number_of_investors,\
                                                         vertex_type="tradeBook",\
                                                         lower_limit=15000,\
                                                         upper_limit=1600000)
    # Executing the data generator
    generator_obj.execute()
    # End of generate_tradebook_investment_amount

# Generates Company Names
def generate_company_names(config_obj):

    # Initializing the data generator
    generator_obj = NamedVG.NamedVertexGenerator(thread_number=10,\
                                                    lines_per_thread=20,\
                                                    destination_file = config_obj.company_name_file_name,\
                                                    current_start_ID = 2 * config_obj.number_of_investors,\
                                                    item_cardinality = config_obj.number_of_companies,\
                                                    vertex_type="company",\
                                                    is_numeric=False)
    # Executing the data generator
    generator_obj.execute()
    # End of generate_company_names

# Generates Company List for Query Drivers
def generate_company_list(config_obj):

    # Initializing the data generator
    generator_obj = PLG.PermutedListGenerator(start_id=2 * config_obj.number_of_investors,\
                                                item_cardinality=config_obj.number_of_companies)

    # Executing the data generator
    generator_obj.generate_and_save_permuted_list(list_type="Company List",\
                                                    destination_file=config_obj.company_list_file_name)
    # End of generate_company_list

# Generates Follower List for edge generation and Query Drivers
def generate_follower_list(config_obj):

    # Initializing the data generator
    generator_obj = PLG.PermutedListGenerator(start_id=0,\
                                                item_cardinality=config_obj.number_of_investors)

    # Executing the data generator and returning the generated follower list
    return generator_obj.generate_and_save_permuted_list(list_type="Follower List",\
                                                    destination_file=config_obj.follower_list_file_name)
    # End of generate_follower_list

# Generates Leader List 1 for edge generation and Query Drivers
def generate_leader_list_1(config_obj):

    # Initializing the data generator
    generator_obj = PLG.PermutedListGenerator(start_id=0,\
                                                item_cardinality=config_obj.number_of_investors)

    # Executing the data generator and returning the generated leader list 1
    return generator_obj.generate_and_save_permuted_list(list_type="Leader List 1",\
                                                    destination_file=config_obj.leader_list_1_file_name)
    # End of generate_leader_list_1

# Generates Leader List 2 for edge generation and Query Drivers
def generate_leader_list_2(config_obj):

    # Initializing the data generator
    generator_obj = PLG.PermutedListGenerator(start_id=0,\
                                                item_cardinality=config_obj.number_of_investors)

    # Executing the data generator and returning the generated leader list 2
    return generator_obj.generate_and_save_permuted_list(list_type="Leader List 2",\
                                                    destination_file=config_obj.leader_list_2_file_name)
    # End of generate_leader_list_2

# Generates Friend and Mirror Edges and the Remove Mirror Edges for Query Drivers
def generate_edges(config_obj, follower_list, leader_list_1, leader_list_2):

    # Initializing the friend edge generator
    friend_edges_generator_obj = FEG.FriendEdgeGenerator(thread_number=10,\
                                                             lines_per_thread=1000,\
                                                             destination_file=config_obj.friend_edges_file_name,\
                                                             number_of_friend_edges=config_obj.number_of_friend_edges,\
                                                             follower_list=follower_list,\
                                                             leader_list_1=leader_list_1,\
                                                             leader_list_2=leader_list_2,\
                                                             follower_list_friend_power_dis_param=config_obj.follower_list_friend_power_dis_param,\
                                                             leader_list_1_friend_power_dis_param=config_obj.leader_list_1_friend_power_dis_param,\
                                                             leader_list_2_friend_power_dis_param=config_obj.leader_list_2_friend_power_dis_param,\
                                                             choose_leader_list_1_as_friend_prob=config_obj.choose_leader_list_1_as_friend_prob,\
                                                             lock_list_element_cardinality=20)

    # Executing the friend edge generator and getting the generated adjacency list for mirror edge generator
    friend_edges_adjacency_dict = friend_edges_generator_obj.execute()

    # Initializing the mirror edge generator
    mirror_edges_generator_obj = MEG.MirrorEdgeGenerator(thread_number=5,\
                                                             lines_per_thread=1000,\
                                                             mirror_destination_file=config_obj.mirror_edges_file_name,\
                                                             remove_mirror_destination_file=config_obj.remove_mirror_edges_file_name,\
                                                             follower_list=follower_list,\
                                                             number_of_friend_edges=config_obj.number_of_friend_edges,\
                                                             number_of_mirror_edges=config_obj.number_of_mirror_edges,\
                                                             follower_mirrors_a_friend_probability=config_obj.follower_mirrors_a_friend_probability,\
                                                             follower_removes_a_mirror_probability=config_obj.follower_removes_a_mirror_probability,\
                                                             follower_list_mirror_power_dis_param=config_obj.follower_list_mirror_power_dis_param,\
                                                             friend_adjacency_dict=friend_edges_adjacency_dict,\
                                                             lock_list_element_cardinality=20)
    # Executing the mirror edge generator
    mirror_edges_generator_obj.execute()
    # End of generate_edges

# Calls the functions defined above to generate all data
def start_base_data_generator(json_config_file):

    # Getting the configuration into the configuration object
    config_obj = Configuration(json_config_file)

    print("Starting Base Data Generator")

    # Generate Investor names using multiprocessing
    investor_name_process = mp.Process(target=generate_investor_names,\
                                        args=(config_obj,))
    investor_name_process.start()

    # Generate TradeBook Investment Amount using multiprocessing
    tradebook_investment_amount_process = mp.Process(target=generate_tradebook_investment_amount,\
                                                        args=(config_obj,))
    tradebook_investment_amount_process.start()

    # Generate Company names using multiprocessing
    company_name_process = mp.Process(target=generate_company_names,\
                                        args=(config_obj,))
    company_name_process.start()

    # Generate Company List using multiprocessing
    generate_company_list(config_obj)
    company_list_process = mp.Process(target=generate_company_list,\
                                        args=(config_obj,))
    company_list_process.start()

    # Generate 3 Investor Lists
    follow_list = generate_follower_list(config_obj)
    lead_list_1 = generate_leader_list_1(config_obj)
    lead_list_2 = generate_leader_list_2(config_obj)
    # Generate Friend Edges Using 3 generated investor lists
    # Generate Mirror Edges Using Adjacency List from Friend Generator (also generates remove list)
    generate_edges(config_obj=config_obj,\
                    follower_list=follow_list,\
                    leader_list_1=lead_list_1,\
                    leader_list_2=lead_list_2)

    # Waiting for all processes to finish
    investor_name_process.join()
    tradebook_investment_amount_process.join()
    company_name_process.join()
    company_list_process.join()

    print("Data Generation Complete")
    # End of start_base_data_generator


if __name__ == "__main__":
    arglist = sys.argv

    # Checking if the number of command line arguments is as expected
    if len(arglist) != 2:
        print("Incorrect number of command line arguments")
        print("Usage: python",arglist[0],"<JSON config file>")
    else:
        # starting the base data generator with the configuration file provided
        start_base_data_generator(arglist[1])

# End of BDG000_ExecuteBaseDataGenerator.py
