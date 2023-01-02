"""
FYP : 22013

Module:
    Base Data Generator

Description:
    This python script executes all the unit tests of all testable python scripts
    in the Base Data Generator module.

"""

# Imports from built-in modules
import sys

# Imports from Base Data Generator Module
import vertex_generators.BDG001_VertexGenerator as Test_base
import vertex_generators.BDG002_NamedVertexGenerator as Test_named
import vertex_generators.BDG003_NumberedVertexGenerator as Test_numbered
import edge_generators.BDG004_FriendEdgeGenerator as Test_friend_edge_gen
import edge_generators.BDG005_MirrorEdgeGenerator as Test_mirror_edge_gen
import list_generators.BDG006_PermutedListGenerator as Test_list_gen


sys.path.append("vertex_generators/")

# Executing all unit tests
try:
    Test_base.execute_all_unit_tests()
    Test_named.execute_all_unit_tests()
    Test_numbered.execute_all_unit_tests()
    Test_friend_edge_gen.execute_all_unit_tests()
    Test_mirror_edge_gen.execute_all_unit_tests()
    Test_list_gen.execute_all_unit_tests()
except AssertionError as emsg:
    print("Unit Tests Failed")
    print(emsg)
