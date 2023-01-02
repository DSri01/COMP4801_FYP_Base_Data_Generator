# COMP4801_FYP_Base_Data_Generator

## FYP: 22013

### FYP Team

**Student:** SRIVASTAVA Dhruv (3035667792)

**Supervisor:** Dr. Heming Cui

## Description

Base Data Generator for HTAP Graph Database Benchmark.

## Usage Instructions

The following python modules are used in implementing the Base Data Generator
scripts (the version number in brackets is the one used during implementation):

- ```json```
- ```multiprocessing```
- ```numpy (1.19.2)```
- ```random```
- ```sys```
- ```threading```

(Optionally) Test the scripts by running:

```python BDG009_UnitTest_BDG.py```

Run the Base Data Generator by defining the configurations in
the ```BDG008_ConfigFile.json``` configuration file and then executing the
generator by running:

```python BDG000_ExecuteBaseDataGenerator.py BDG008_ConfigFile.json```

10 files will be generated storing the following data:

- Investor Names
- TradeBook Investment Amount
- Company Names
- Company List
- Follower List
- Leader List 1
- Leader List 2
- Friend Edges
- Mirror Edges
- Remove Mirror Edges



## Module Components Description

| File Name | Description |
|-----------|-------------|
|BDG000_ExecuteBaseDataGenerator.py|Script to generate all data for the benchmark and Query Drivers|
|vertex_generators/BDG001_VertexGenerator.py|Base Class that defines the functionality to generate data for vertices in multithreaded fashion|
|vertex_generators/BDG002_NamedVertexGenerator.py|Defines the functionality to generate vertex names|
|vertex_generators/BDG003_NumberedVertexGenerator.py|Defines the functionality to generate numbers for vertices|
|edge_generators/BDG004_FriendEdgeGenerator.py|Defines the functionality to generate 'friend' edges|
|edge_generators/BDG005_MirrorEdgeGenerator.py|Defines the functionality to generate 'mirror' edges and 'remove mirror' edges|
|list_generators/BDG006_PermutedListGenerator.py|Defines the functionality to generate permutation of a list and store the list in a file|
|BDG007_Configuration.py|Defines the functionality to load configurations from the config file to a configuration object used across BDG000_ExecuteBaseDataGenerator.py to share the configurations|
|BDG008_ConfigFile.json|Config File for the Base Data Generator. Uses the JSON format|
|BDG009_UnitTest_BDG.py|Running the scripts executes all the defined unit tests for the module components|
