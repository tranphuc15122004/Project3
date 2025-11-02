from algorithm.Object import *
import sys
import time
from typing import Dict , List
from algorithm.In_and_Out import *
from algorithm.engine import *
import algorithm.algorithm_config as Config
from algorithm.GAVND import GAVND_7
from src.conf.configs import Configs
import time

def main():
    id_to_factory , route_map ,  id_to_vehicle , id_to_unlocated_items ,  id_to_ongoing_items , id_to_allorder = Input()
    deal_old_solution_file(id_to_vehicle)

    vehicleid_to_plan: Dict[str , List[Node]]= {}
    vehicleid_to_destination : Dict[str , Node] = {}

    new_order_itemIDs : List[str] = []
    new_order_itemIDs = restore_scene_with_single_node(vehicleid_to_plan , id_to_ongoing_items, id_to_unlocated_items  , id_to_vehicle , id_to_factory ,id_to_allorder)

    new_order_itemIDs = [item for item in new_order_itemIDs if item]
    
    #Thuat toan
    print()
    
    dispatch_new_orders(vehicleid_to_plan , id_to_factory , route_map , id_to_vehicle , id_to_unlocated_items , new_order_itemIDs)
    
    
    Unongoing_super_nodes , Base_vehicleid_to_plan = get_UnongoingSuperNode(vehicleid_to_plan , id_to_vehicle)
    
    best_chromosome = Chromosome(vehicleid_to_plan , route_map , id_to_vehicle)
    
    copy_vehicleid_to_plan = copy.deepcopy(vehicleid_to_plan)
    best_chromosome : Chromosome = GAVND_7(copy_vehicleid_to_plan , route_map , id_to_vehicle , Unongoing_super_nodes , Base_vehicleid_to_plan)
    if best_chromosome is None or best_chromosome.fitness > total_cost(id_to_vehicle , route_map , vehicleid_to_plan):
        best_chromosome = Chromosome(vehicleid_to_plan , route_map , id_to_vehicle)
    
    
    used_time = time.time() - Config.BEGIN_TIME
    print('Thoi gian thuc hien thuat toan: ' , used_time)
    
    update_solution_json(id_to_ongoing_items , id_to_unlocated_items , id_to_vehicle , best_chromosome.solution , vehicleid_to_destination , route_map , used_time)
    merge_node(id_to_vehicle , best_chromosome.solution)   
    
    get_output_solution(id_to_vehicle , best_chromosome.solution , vehicleid_to_destination)
    
    write_destination_json_to_file(vehicleid_to_destination   , input_directory)    
    write_route_json_to_file(best_chromosome.solution  , input_directory) 
