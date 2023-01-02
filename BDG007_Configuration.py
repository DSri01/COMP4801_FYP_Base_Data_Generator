"""
FYP : 22013

Module:
    Base Data Generator

Description:
    This python script contains the definition of the Configuration class.

    The Configuration class is used to read the configuration from the inputted
    configuration file and expose its variables to be used in the executor script
    defined in BDG000_ExecuteBaseDataGenerator.py.

    JSON is used as the file format for storing configurations.

"""

import json

class Configuration:
    def __init__(self, config_file,):

        # The configuration file storing the configuration in the form of a JSON string
        self.config_file = config_file

        # calling instatiate_configuration() function
        self.instatiate_configuration()

    def instatiate_configuration(self):
        """
        Description:
            Reads the JSON file in the configuration file and stores all the
            configurations in this object.
        """

        file_json_string = ""
        with open(self.config_file, mode='r') as file:
            file_json_string = file.read()
            file.close()
        configuration_dictionary = json.loads(file_json_string)

        #Base Data Generator Configurations

        self.number_of_investors = configuration_dictionary["number_of_investors"]

        self.number_of_companies = configuration_dictionary["number_of_companies"]

        self.number_of_friend_edges = configuration_dictionary["number_of_friend_edges"]

        self.number_of_mirror_edges = configuration_dictionary["number_of_mirror_edges"]

        self.follower_list_friend_power_dis_param = configuration_dictionary["follower_list_friend_power_dis_param"]

        self.leader_list_1_friend_power_dis_param = configuration_dictionary["leader_list_1_friend_power_dis_param"]

        self.leader_list_2_friend_power_dis_param = configuration_dictionary["leader_list_2_friend_power_dis_param"]

        self.choose_leader_list_1_as_friend_prob = configuration_dictionary["choose_leader_list_1_as_friend_prob"]

        self.follower_list_mirror_power_dis_param = configuration_dictionary["follower_list_mirror_power_dis_param"]

        self.follower_mirrors_a_friend_probability = configuration_dictionary["follower_mirrors_a_friend_probability"]

        self.follower_removes_a_mirror_probability = configuration_dictionary["follower_removes_a_mirror_probability"]

        #File Name Configuarations

        self.investor_name_file_name = configuration_dictionary["investor_name_file_name"]

        self.tradebook_investment_amount_file_name = configuration_dictionary["tradebook_investment_amount_file_name"]

        self.company_name_file_name = configuration_dictionary["company_name_file_name"]

        self.company_list_file_name = configuration_dictionary["company_list_file_name"]

        self.follower_list_file_name = configuration_dictionary["follower_list_file_name"]

        self.leader_list_1_file_name = configuration_dictionary["leader_list_1_file_name"]

        self.leader_list_2_file_name = configuration_dictionary["leader_list_2_file_name"]

        self.friend_edges_file_name = configuration_dictionary["friend_edges_file_name"]

        self.mirror_edges_file_name = configuration_dictionary["mirror_edges_file_name"]

        self.remove_mirror_edges_file_name = configuration_dictionary["remove_mirror_edges_file_name"]
