import math
import random
import sys
import time
from typing import Dict, List, Optional, Tuple
from algorithm.Object import *
import algorithm.algorithm_config as config
from algorithm.engine import dispatch_nodePair, total_cost

class Chromosome:
    def __init__(self, vehicleid_to_plan: Dict[str, List[Node]], route_map: Dict[Tuple, Tuple], id_to_vehicle: Dict[str, Vehicle]):
        self.solution = vehicleid_to_plan
        self.route_map = route_map
        self.id_to_vehicle = id_to_vehicle
        self.cant_improved = False


    @property
    def fitness(self) -> float:
        return total_cost(self.id_to_vehicle, self.route_map, self.solution)

    def __repr__(self):
        return f'Chromosome(Fitness: {self.fitness}, Solution: {get_route_after(self.solution , {})})'
