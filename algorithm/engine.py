import copy
from datetime import datetime
import json
import math
import os
import random
import re
import sys
from typing import Dict , List, Optional, Tuple
from algorithm.Object import *
from src.conf.configs import Configs


input_directory = Configs.algorithm_data_interaction_folder_path   

def restore_scene_with_single_node(vehicleid_to_plan: Dict[str , List[Node]], id_to_ongoing_items: Dict[str , OrderItem], id_to_unlocated_items: Dict[str , OrderItem], id_to_vehicle: Dict[str , Vehicle] , id_to_factory: Dict[str , Factory], id_to_allorder: Dict[str , OrderItem]) -> List[str]:
    onVehicleOrderItems = ''
    unallocatedOrderItems = ''
    new_order_itemIDs = []
    
    for vehicleID in id_to_vehicle.keys():
        vehicleid_to_plan[vehicleID] = []
    
    for key in id_to_ongoing_items:
        onVehicleOrderItems += f"{key} "
    onVehicleOrderItems.strip()
    
    for key in id_to_unlocated_items:
        unallocatedOrderItems += f"{key} "
    unallocatedOrderItems.strip()
    
    solution_json_path = os.path.join(input_directory , 'solution.json')
    if os.path.exists(solution_json_path) :
        try:
            with open(solution_json_path , 'r') as file:
                before_solution = json.load(file)
                no = int(before_solution.get('no', 0))
                f = (no + 1) * 10
                t = (no + 1) * 10 + 10
                global delta_t
                delta_t = f"{f:04d}-{t:04d}"
                routeBefore = before_solution.get("route_after", "")
                splited_routeBefore : List[str] = routeBefore.split("V")
                
                last_on_vehicle_items= before_solution.get("onvehicle_order_items", "").split()
                curr_on_vehicle_items : List[str] = onVehicleOrderItems.split(" ")
                completeOrderItems = ' '.join([item for item in last_on_vehicle_items if item not in curr_on_vehicle_items]).strip()
                complete_item_array = completeOrderItems.split(" ")
                
                last_unallocated_items : List[str] = before_solution.get("unallocated_order_items", "").split()
                curr_unallocated_items : List[str] = unallocatedOrderItems.split(" ")
                newOrderItems = ' '.join([item for item in curr_unallocated_items if item not in last_unallocated_items]).strip()
                
                for route in splited_routeBefore:
                    if not route or len(route) < 3:
                        continue
                    
                    route = route.strip()
                    str_len : int = len(route.split(':')[1])
                    numstr = route.split(":")[0]
                    vehicleID = "V_" + numstr[1:]
                    if str_len < 3: 
                        vehicleid_to_plan[vehicleID] = []
                        continue
                    
                    route_nodes_str = route.split(":")[1]
                    route_nodes = route_nodes_str[1:len(route_nodes_str) - 1].split(" ")
                    node_list : List[str] = list(route_nodes)
                    
                    # bao gồm các node (đại diện bởi itemID) cò tới thời điểm của time interval hiện tại
                    node_list = [
                        node for node in node_list
                        if not (
                            (node.startswith("d") and node.split("_")[1] in complete_item_array) or
                            (node.startswith("p") and node.split("_")[1] in curr_on_vehicle_items)
                        )
                    ]
                    
                    if len(node_list) > 0:
                        planroute : List[Node] = []
                        
                        for node in node_list:
                            deliveryItemList : List[OrderItem] = []
                            pickupItemList : List[OrderItem] = []
                            temp : OrderItem = None
                            op = node[0][0:1]           #chỉ thị trạng thái của node (pickup / delivery) (p/d)
                            opNumstr = node.split("_")
                            opItemNum = int(opNumstr[0][1 :]) #p3 -> 3
                            orderItemID = node.split("_")[1]
                            idEndNumber = int(orderItemID.split("-")[1]) #số hiệu lớn nhất của đơn hàng
                            
                            # nếu là node giao
                            if op == 'd':
                                for i in range(opItemNum):
                                    temp = id_to_allorder[orderItemID]
                                    deliveryItemList.append(temp)
                                    
                                    idEndNumber -= 1
                                    orderItemID =  orderItemID.split("-")[0] + "-" + str(idEndNumber)
                            # nếu là node nhận
                            else:
                                for i in range(opItemNum):
                                    temp = id_to_allorder[orderItemID]
                                    pickupItemList.append(temp)
                                    
                                    idEndNumber += 1
                                    orderItemID =  orderItemID.split("-")[0] + "-" + str(idEndNumber)
                            
                            factoryID = ""
                            if op == 'd':
                                factoryID = temp.delivery_factory_id
                            else:
                                factoryID = temp.pickup_factory_id
                            factory = id_to_factory[factoryID]
                            
                            planroute.append(Node(factoryID , deliveryItemList , pickupItemList ,None ,None , factory.lng , factory.lat))
                            
                        if len(planroute) > 0:
                            vehicleid_to_plan[vehicleID] = planroute
        except Exception as e:
            print(f"Error: {e}" , file= sys.stderr)
    else:
        newOrderItems  = unallocatedOrderItems
        completeOrderItems = ""
        routeBefore = ""
        delta_t = "0000-0010"
        
    new_order_itemIDs = newOrderItems.split()
    
    return new_order_itemIDs

def get_output_solution(id_to_vehicle: Dict[str , Vehicle] , vehicleid_to_plan: Dict[str , list[Node]] , vehicleid_to_destination : Dict[str , Node]):
    for vehicleID , vehicle in id_to_vehicle.items():
        origin_plan : List[Node]= vehicleid_to_plan.get(vehicleID , [])
        destination : Node = None
        if vehicle.des:
            if (not origin_plan):
                print(f"Planned route of vehicle {vehicleID} is wrong", file=sys.stderr)
            else:
                destination = origin_plan[0]
                destination.arrive_time = vehicle.des.arrive_time
                origin_plan.pop(0)
            
            if destination and vehicle.des.id != destination.id:
                print(f"Vehicle {vehicleID} returned destination id is {vehicle.des.id} "
                    f"however the origin destination id is {destination.id}", file=sys.stderr)
        elif (origin_plan):
            destination = origin_plan[0]
            origin_plan.pop(0)
        if origin_plan and len(origin_plan) == 0:
            origin_plan = None
        vehicleid_to_plan[vehicleID] = origin_plan
        vehicleid_to_destination[vehicleID] = destination
        
def update_solution_json (id_to_ongoing_items: Dict[str , OrderItem] , id_to_unlocated_items: Dict[str , OrderItem] , id_to_vehicle: Dict[str , Vehicle] , vehicleid_to_plan: Dict[str , list[Node]] , vehicleid_to_destination : Dict[str , Node] , route_map: Dict[tuple , tuple] , used_time):
    order_items_json_path = os.path.join(input_directory, "solution.json")
    complete_order_items = ""
    on_vehicle_order_items = ""
    ongoing_order_items = ""
    unongoing_order_items = ""
    unallocated_order_items = ""
    new_order_items = ""
    route_before = ""
    route_after = ""
    solution_json_obj = {}

    on_vehicle_order_items = " ".join(id_to_ongoing_items.keys()).strip()

    unallocated_order_items = " ".join(id_to_unlocated_items.keys()).strip()

    os.makedirs(input_directory, exist_ok=True)
    
    pre_matching_item_ids = []
    for vehicle in id_to_vehicle.values():
        if (not vehicle.carrying_items) and  (vehicle.des) :
            pickup_item_list : List[OrderItem] = vehicle.des.pickup_item_list
            pre_matching_item_ids.extend([order_item.id for order_item in pickup_item_list])
    ongoing_order_items = " ".join(pre_matching_item_ids).strip()

    unallocated_items = unallocated_order_items.split()
    unongoing_order_items = " ".join([item for item in unallocated_items if item not in pre_matching_item_ids]).strip()

    if not os.path.exists(order_items_json_path):
        delta_t = "0000-0010"
        vehicle_num = len(vehicleid_to_plan)
        for i in range(vehicle_num):
            car_id = f"V_{i + 1}"
            route_before += f"{car_id}:[] "
        route_before = route_before.strip()

        route_after = get_route_after(vehicleid_to_plan , vehicleid_to_destination)
        
        solution_json_obj = {
            "no.": "0",
            "deltaT": delta_t,
            "complete_order_items": complete_order_items,
            "onvehicle_order_items": on_vehicle_order_items,
            "ongoing_order_items": ongoing_order_items,
            "unongoing_order_items": unongoing_order_items,
            "unallocated_order_items": unallocated_order_items,
            "new_order_items": unallocated_order_items,
            "used_time": used_time,
            "route_before": route_before,
            "route_after": route_after
        }
    else:
        try:
            with open(order_items_json_path, 'r', encoding='utf-8') as file:
                before_solution = json.load(file)
        except (IOError, json.JSONDecodeError) as e:
            print(f"Lỗi khi đọc file JSON: {e}", file = sys.stderr)
            return  # Ngăn lỗi tiếp tục chạy

        no = int(before_solution["no."]) + 1

        from_t = (no + 1) * 10
        to_t = (no + 1) * 10 + 10
        from_t_str = f"{from_t:04d}" if from_t < 10000 else str(from_t)
        to_t_str = f"{to_t:04d}" if to_t < 10000 else str(to_t)
        delta_t = f"{from_t_str}-{to_t_str}"

        last_onvehicle_order_item = before_solution["onvehicle_order_items"].split()
        curr_onvehicle_order_item = on_vehicle_order_items.split()
        complete_order_items = ' '.join([item for item in last_onvehicle_order_item if item not in curr_onvehicle_order_item]).split()
        
        last_unallocated_items = before_solution.get("unallocated_order_items", "").split()
        cur_unallocated_items = unallocated_order_items.split()
        new_order_items = " ".join([item for item in cur_unallocated_items if item not in last_unallocated_items]).split()


        route_before = before_solution["route_after"]
        route_after = get_route_after(vehicleid_to_plan , vehicleid_to_destination)

        solution_json_obj = {
            "no.": str(no),
            "deltaT": delta_t,
            "complete_order_items": complete_order_items,
            "onvehicle_order_items": on_vehicle_order_items,
            "ongoing_order_items": ongoing_order_items,
            "unongoing_order_items": unongoing_order_items,
            "unallocated_order_items": unallocated_order_items,
            "new_order_items": new_order_items,
            "used_time": used_time,
            "route_before": route_before,
            "route_after": route_after
        }

    # Ghi dữ liệu ra file JSON
    try:
        with open(order_items_json_path, 'w', encoding='utf-8') as file:
            json.dump(solution_json_obj, file, indent=4, ensure_ascii=False)
    except IOError as e:
        print(f"Lỗi khi ghi file JSON: {e}", file = sys.stderr)
        

def merge_node(id_to_vehicle: Dict[str , Vehicle], vehicleid_to_plan: Dict[str, list[Node]]):
    for vehicle_id, vehicle in id_to_vehicle.items():
        origin_planned_route = vehicleid_to_plan.get(vehicle_id, [])

        if origin_planned_route and len(origin_planned_route) > 1:
            before_node = origin_planned_route[0]
            i = 1  # Bắt đầu từ phần tử thứ 2
            while (i < len(origin_planned_route)):
                next_node = origin_planned_route[i]

                if before_node.id == next_node.id:
                    # Gộp danh sách pickupItemList
                    if next_node.pickup_item_list:
                        before_node.pickup_item_list.extend(next_node.pickup_item_list)  

                    # Gộp danh sách deliveryItemList (dùng extend thay vì vòng lặp)
                    if next_node.delivery_item_list:
                        before_node.delivery_item_list.extend(next_node.delivery_item_list) 
                    # Xóa phần tử trùng lặp
                    origin_planned_route.pop(i)
                else:
                    before_node = next_node
                    i += 1  # Chỉ tăng index khi không xóa phần tử
        vehicleid_to_plan[vehicle_id] = origin_planned_route
        
def get_route_after(vehicleid_to_plan: Dict[str , list[Node]], vehicleid_to_destination : Dict[str , Node]):
    
    route_str = ""
    vehicle_num = len(vehicleid_to_plan)
    vehicle_routes = [""] * vehicle_num
    index = 0
    
    if vehicleid_to_destination is None or len(vehicleid_to_destination) == 0:
        for i in range(vehicle_num):
            vehicle_routes[i] = "["
    for vehicle_id, first_node in vehicleid_to_destination.items():
        if first_node is not None:
            pickup_size = len(first_node.pickup_item_list) if first_node.pickup_item_list else 0
            delivery_size = len(first_node.delivery_item_list) if first_node.delivery_item_list else 0
            
            if delivery_size > 0:
                vehicle_routes[index] = f"[d{delivery_size}_{first_node.delivery_item_list[0].id} "
            if pickup_size > 0:
                if delivery_size == 0:
                    vehicle_routes[index] = f"[p{pickup_size}_{first_node.pickup_item_list[0].id} "
                else:
                    vehicle_routes[index] = vehicle_routes[index].strip()
                    vehicle_routes[index] += f"p{pickup_size}_{first_node.pickup_item_list[0].id} "
        else:
            vehicle_routes[index] = "["
        index += 1
    
    index = 0
    for vehicle_id, id2_node_list in vehicleid_to_plan.items():
        if id2_node_list and len(id2_node_list) > 0:
            for node in id2_node_list:
                pickup_size = len(node.pickup_item_list)
                delivery_size = len(node.delivery_item_list)
                
                if delivery_size > 0:
                    vehicle_routes[index] += f"d{delivery_size}_{node.delivery_item_list[0].id} "
                if pickup_size > 0:
                    if delivery_size > 0:
                        vehicle_routes[index] = vehicle_routes[index].strip()
                    vehicle_routes[index] += f"p{pickup_size}_{node.pickup_item_list[0].id} "
            
            vehicle_routes[index] = vehicle_routes[index].strip()
        vehicle_routes[index] += "]"
        index += 1

    for i in range(vehicle_num):
        car_id = f"V_{i + 1}"
        route_str += f"{car_id}:{vehicle_routes[i]} "
    
    route_str = route_str.strip()
    return route_str

        
def deal_old_solution_file(id2VehicleMap):
    # Xác định thời gian 00:00:00 của ngày hiện tại (UNIX timestamp, tính theo giây)
    now = datetime.now()
    initial_time = int(datetime(now.year, now.month, now.day, 0, 0, 0).timestamp())

    # Kiểm tra điều kiện thời gian cập nhật GPS
    if id2VehicleMap["V_1"].gps_update_time - 600 == initial_time:
        # Kiểm tra nếu bất kỳ xe nào có điểm đến thì thoát
        for vehicle in id2VehicleMap.values():
            if vehicle.des is not None:
                return

        # Xóa file solution.json nếu tồn tại
        file_path = "./algorithm/data_interaction/solution.json"
        if os.path.exists(file_path):
            os.remove(file_path)
            
            

def dispatch_new_orders(vehicleid_to_plan: Dict[str , list[Node]] ,  id_to_factory:Dict[str , Factory] , route_map: Dict[tuple , tuple] ,  id_to_vehicle: Dict[str , Vehicle] , id_to_unlocated_items:Dict[str , OrderItem], new_order_itemIDs: list[str]):
    pass