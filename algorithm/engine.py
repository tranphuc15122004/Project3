import copy
from datetime import datetime
import json
import math
import os
import random
import re
import sys
import time
from typing import Dict , List, Optional, Tuple
from algorithm.Object import *
import algorithm.algorithm_config as config
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
            

def isFeasible(route_node_list : List[Node] , carrying_items : List[OrderItem] , capacity : float ):
    unload_item_list = carrying_items[::-1] if carrying_items else []

    for node in route_node_list:
        delivery_items : List[OrderItem] = node.delivery_item_list
        pickup_items : List[OrderItem]= node.pickup_item_list

        if delivery_items:
            for order_item in delivery_items:
                if (not unload_item_list) or (unload_item_list[0] is None) or (unload_item_list[0].id != order_item.id):
                    #print("Violate FILO 1" , file= sys.stderr)
                    return False
                del unload_item_list[0]

        if pickup_items:
            for orderitem in pickup_items:
                unload_item_list.insert(0 , orderitem)
    
    if unload_item_list:
        #print("Violate FILO 2" ,file= sys.stderr)
        return False

    left_capacity = capacity
    if carrying_items:
        for order_item in carrying_items:
            left_capacity -= order_item.demand

    for node in route_node_list:
        delivery_items = node.delivery_item_list
        pickup_items = node.pickup_item_list

        if delivery_items:
            for order_item in delivery_items:
                left_capacity += order_item.demand
                if left_capacity > capacity:
                    #print("Violate capacity" , file= sys.stderr)
                    return False

        if pickup_items:
            for order_item in pickup_items:
                left_capacity -= order_item.demand
                if left_capacity < 0:
                    #print("Violate capacity" , file= sys.stderr)
                    return False

    return (not unload_item_list)


def cost_of_a_route (temp_route_node_list : List[Node] , vehicle: Vehicle , id_to_vehicle: Dict[str , Vehicle] , route_map: Dict[tuple , tuple] , vehicleid_to_plan: Dict[str , list[Node]] , mode = 'total') -> float:
    curr_factoryID = vehicle.cur_factory_id
    driving_dis  : float = 0.0
    overtime_Sum : float = 0.0
    objF : float = 0.0
    capacity = vehicle.board_capacity
    carrying_Items : List[OrderItem] = vehicle.carrying_items if vehicle.des else []
    
    if (temp_route_node_list) and (not isFeasible(temp_route_node_list , carrying_Items , capacity)):
        return math.inf
    
    
    dock_table: Dict[str, List[List[int]]] = {}
    n: int = 0
    vehicle_num: int = len(id_to_vehicle)

    curr_node: List[int] = [0] * vehicle_num
    curr_time: List[int] = [0] * vehicle_num
    leave_last_node_time: List[int] = [0] * vehicle_num

    n_node: List[int] = [0] * vehicle_num
    index = 0
    
    for vehicleID , otherVehicle in id_to_vehicle.items():
        distance = 0
        time  = 0
        
        if otherVehicle.cur_factory_id:
            if otherVehicle.leave_time_at_current_factory > otherVehicle.gps_update_time:
                tw: List[int] = [
                    otherVehicle.arrive_time_at_current_factory,
                    otherVehicle.leave_time_at_current_factory
                ]
                tw_list: Optional[List[List[int]]] = dock_table.get(otherVehicle.cur_factory_id , [])
                tw_list.append(tw)
                dock_table[otherVehicle.cur_factory_id] = tw_list
            leave_last_node_time[index] = otherVehicle.leave_time_at_current_factory
        else:
            leave_last_node_time[index] = otherVehicle.gps_update_time
        
        # Neu la xe dang xet
        if vehicleID == vehicle.id:
            if not temp_route_node_list or len(temp_route_node_list) == 0:
                curr_node[index] = math.inf
                curr_time[index] = math.inf
                n_node[index] = 0
            else:
                curr_node[index] = 0
                n_node[index] = len(temp_route_node_list)
                if not vehicle.des:
                    if vehicle.cur_factory_id == "":
                        print("cur factory have no value" , file= sys.stderr)
                    if len(temp_route_node_list) == 0:
                        print("tempRouteNodeList have no length" , file= sys.stderr)
                        
                    if vehicle.cur_factory_id == temp_route_node_list[0].id:
                        curr_time[index] = vehicle.leave_time_at_current_factory
                    else:
                        dis_and_time = route_map.get((vehicle.cur_factory_id , temp_route_node_list[0].id))
                        distance = float(dis_and_time[0])
                        time = int(dis_and_time[1])
                        curr_time[index] = vehicle.leave_time_at_current_factory + time
                        driving_dis += distance
                else:
                    if curr_factoryID:
                        if  curr_factoryID != temp_route_node_list[0].id:
                            curr_time[index] = vehicle.leave_time_at_current_factory
                            dis_and_time = route_map.get((curr_factoryID , temp_route_node_list[0].id))
                            distance = float(dis_and_time[0])
                            time = int(dis_and_time[1])
                            driving_dis += distance
                            curr_time[index] += time
                        else:
                            curr_time[index] = vehicle.leave_time_at_current_factory
                    else:
                        curr_time[index] = vehicle.des.arrive_time
                n += 1
        # Neu khong phai xe dang xet va co tuyen duong
        elif vehicleid_to_plan[vehicleID] and len(vehicleid_to_plan[vehicleID]) > 0:    
            curr_node[index] = 0
            n_node[index] = len(vehicleid_to_plan[vehicleID]) 
            
            if otherVehicle.des is None:
                if otherVehicle.cur_factory_id == vehicleid_to_plan[vehicleID][0].id:
                    curr_time[index] = otherVehicle.leave_time_at_current_factory
                else:
                    dis_and_time = route_map.get((otherVehicle.cur_factory_id , vehicleid_to_plan[vehicleID][0].id))
                    if dis_and_time is None:
                        print("no distance" , file= sys.stderr)
                    
                    distance = float(dis_and_time[0])
                    time = int(dis_and_time[1])
                    curr_time[index] = otherVehicle.leave_time_at_current_factory + time
                    driving_dis += distance
            else:
                if otherVehicle.cur_factory_id and len(otherVehicle.cur_factory_id) > 0:
                    if otherVehicle.cur_factory_id == vehicleid_to_plan[vehicleID][0].id:
                        curr_time[index]  = otherVehicle.leave_time_at_current_factory
                    else:
                        curr_time[index] = otherVehicle.leave_time_at_current_factory
                        dis_and_time = route_map.get((otherVehicle.cur_factory_id , vehicleid_to_plan[vehicleID][0].id))
                        distance = float(dis_and_time[0])
                        time = int(dis_and_time[1])
                        curr_time[index] += time
                        driving_dis += distance
                else: 
                    curr_time[index] = otherVehicle.des.arrive_time
            n+=1
        else:
            curr_time[index] = math.inf
            curr_time[index] = math.inf
            n_node[index] = 0
        index += 1
    
    while (n > 0):
        minT = math.inf
        minT2VehicleIndex = 0
        tTrue = minT
        idx = 0
        
        for i in range (vehicle_num):
            if curr_time[i] < minT:
                minT = curr_time[i]
                minT2VehicleIndex = i
        
        minT2VehicleIndex += 1
        minT2VehicleID = "V_" + str(minT2VehicleIndex)
        minT2VehicleIndex -= 1
        
        minTNodeList: List[Node] = []
        if minT2VehicleID == vehicle.id:
            minTNodeList = temp_route_node_list
        else:
            minTNodeList = vehicleid_to_plan[minT2VehicleID]
        minTNode = minTNodeList[curr_node[minT2VehicleIndex]]
        
        if minTNode.delivery_item_list and len(minTNode.delivery_item_list) > 0:
            beforeOrderID = ""
            nextOrderID = ""
            for order_item in minTNode.delivery_item_list:
                nextOrderID = order_item.id
                if beforeOrderID != nextOrderID:
                    commitCompleteTime = order_item.committed_completion_time
                    overtime_Sum += max(0 , curr_time[minT2VehicleIndex] - commitCompleteTime)
                beforeOrderID = nextOrderID
        
        usedEndTime : List[int] = []
        timeSlots : List[List[int]] =  dock_table.get(minTNode.id, [])
        if timeSlots:
            i = 0
            while i < len(timeSlots):
                time_slot = timeSlots[i]
                if time_slot[1] <= minT:
                    timeSlots.pop(i)  # Xóa phần tử nếu end_time <= minT
                elif time_slot[0] <= minT < time_slot[1]:
                    usedEndTime.append(time_slot[1])
                    i += 1
                else:
                    print("------------ timeslot.start > minT --------------", file = sys.stderr)
                    i += 1

        if len(usedEndTime) < 6:
            tTrue = minT
        else:
            idx = len(usedEndTime) - 6
            usedEndTime.sort()
            tTrue = usedEndTime[idx]


        service_time = minTNodeList[curr_node[minT2VehicleIndex]].service_time
        cur_factory_id = minTNodeList[curr_node[minT2VehicleIndex]].id
        curr_node[minT2VehicleIndex] += 1

        while (curr_node[minT2VehicleIndex] < n_node[minT2VehicleIndex] and cur_factory_id == minTNodeList[curr_node[minT2VehicleIndex]].id):

            delivery_item_list = minTNodeList[curr_node[minT2VehicleIndex]].delivery_item_list
            
            if delivery_item_list and len(delivery_item_list) > 0:
                before_order_id = ""
                next_order_id = ""

                for order_item in delivery_item_list:
                    next_order_id = order_item.order_id
                    if before_order_id != next_order_id:
                        commit_complete_time = order_item.committed_completion_time
                        overtime_Sum += max(0, curr_time[minT2VehicleIndex] - commit_complete_time)
                    before_order_id = next_order_id

            service_time += minTNodeList[curr_node[minT2VehicleIndex]].service_time
            curr_node[minT2VehicleIndex] += 1
            
        if curr_node[minT2VehicleIndex] >= n_node[minT2VehicleIndex]:
            n -= 1
            curr_node[minT2VehicleIndex] = math.inf
            curr_time[minT2VehicleIndex] = math.inf
            n_node[minT2VehicleIndex] = 0
        else:
            dis_and_time = route_map.get((cur_factory_id , minTNodeList[curr_node[minT2VehicleIndex]].id))
            if dis_and_time:
                distance = float(dis_and_time[0])
                time = int(dis_and_time[1])

                curr_time[minT2VehicleIndex] = tTrue + config.APPROACHING_DOCK_TIME + service_time + time
                leave_last_node_time[minT2VehicleIndex] = tTrue + config.APPROACHING_DOCK_TIME + service_time
                driving_dis += distance

        tw = [minT, tTrue + config.APPROACHING_DOCK_TIME + service_time]
        tw_list = dock_table.get(minTNode.id, [])

        tw_list.append(tw)
        dock_table[minTNode.id] = tw_list
    
    objF = (config.Delta * overtime_Sum) + (driving_dis / float(len(id_to_vehicle))) 
    if mode == 'overtime':
        return (config.Delta * overtime_Sum)
    elif mode  == 'distance':
        return (driving_dis / float(len(id_to_vehicle)))
    if objF < 0:
        print("the objective function less than 0" , file= sys.stderr)
    return objF


def total_cost(id_to_vehicle: Dict[str , Vehicle] , route_map: Dict[tuple , tuple] , vehicleid_to_plan: Dict[str , list[Node]]) -> float:
    driving_dis  : float = 0.0
    overtime_Sum : float = 0.0
    waiting_Sum  : float = 0.0
    objF : float  = 0.0
    dock_table: Dict[str, List[List[int]]] = {}
    n: int = 0
    vehicle_num: int = len(id_to_vehicle)
    curr_node: List[int] = [0] * vehicle_num
    curr_time: List[int] = [0] * vehicle_num
    leave_last_node_time: List[int] = [0] * vehicle_num

    n_node: List[int] = [0] * vehicle_num
    index = 0
    
    for vehicleID , otherVehicle in id_to_vehicle.items():
        distance = 0
        time  = 0
        
        if otherVehicle.cur_factory_id :
            if otherVehicle.leave_time_at_current_factory > otherVehicle.gps_update_time:
                tw: List[int] = [
                    otherVehicle.arrive_time_at_current_factory,
                    otherVehicle.leave_time_at_current_factory
                ]
                tw_list: Optional[List[List[int]]] = dock_table.get(otherVehicle.cur_factory_id)
                if tw_list is None:
                    tw_list = []
                tw_list.append(tw)
                dock_table[otherVehicle.cur_factory_id] = tw_list
            leave_last_node_time[index] = otherVehicle.leave_time_at_current_factory
        else:
            leave_last_node_time[index] = otherVehicle.gps_update_time
        
        if vehicleid_to_plan.get(vehicleID) and len(vehicleid_to_plan.get(vehicleID)) > 0:    
            curr_node[index] = 0
            n_node[index] = len(vehicleid_to_plan[vehicleID]) 
            
            if otherVehicle.des is None:
                if otherVehicle.cur_factory_id == vehicleid_to_plan[vehicleID][0].id:
                    curr_time[index] = otherVehicle.leave_time_at_current_factory
                else:
                    dis_and_time = route_map.get((otherVehicle.cur_factory_id , vehicleid_to_plan[vehicleID][0].id))
                    if dis_and_time is None:
                        print("no distance" , file= sys.stderr)
                    
                    distance = float(dis_and_time[0])
                    time = int(dis_and_time[1])
                    curr_time[index] = otherVehicle.leave_time_at_current_factory + time
                    driving_dis += distance
            else:
                if otherVehicle.cur_factory_id is not None and len(otherVehicle.cur_factory_id) > 0:
                    if otherVehicle.cur_factory_id == vehicleid_to_plan[vehicleID][0].id:
                        curr_time[index]  = otherVehicle.leave_time_at_current_factory
                    else:
                        curr_time[index] = otherVehicle.leave_time_at_current_factory
                        dis_and_time = route_map.get((otherVehicle.cur_factory_id , vehicleid_to_plan[vehicleID][0].id))
                        distance = float(dis_and_time[0])
                        time = int(dis_and_time[1])
                        curr_time[index] += time
                        driving_dis += distance
                else: 
                    curr_time[index] = otherVehicle.des.arrive_time
            n+=1
        else:
            curr_time[index] = math.inf
            curr_time[index] = math.inf
            n_node[index] = 0
        index += 1
    
    while n > 0:
        minT = math.inf
        minT2VehicleIndex = 0
        tTrue = minT
        idx = 0
        
        for i in range (vehicle_num):
            if curr_time[i] < minT:
                minT = curr_time[i]
                minT2VehicleIndex = i
        
        minT2VehicleIndex += 1
        minT2VehicleID = "V_" + str(minT2VehicleIndex)
        minT2VehicleIndex -= 1
        
        minTNodeList: List[Node] = []
        minTNodeList = vehicleid_to_plan.get(minT2VehicleID)
        minTNode = minTNodeList[curr_node[minT2VehicleIndex]]
        
        if minTNode.delivery_item_list and len(minTNode.delivery_item_list) > 0:
            beforeOrderID = ""
            nextOrderID = ""
            for order_item in minTNode.delivery_item_list:
                nextOrderID = order_item.id
                if beforeOrderID != nextOrderID:
                    commitCompleteTime = order_item.committed_completion_time
                    overtime_Sum += max(0 , curr_time[minT2VehicleIndex] - commitCompleteTime)
                beforeOrderID = nextOrderID
        
        usedEndTime : List[int] = []
        timeSlots : List[List[int]] =  dock_table.get(minTNode.id, [])
        if timeSlots:
            i = 0
            while i < len(timeSlots):
                time_slot = timeSlots[i]
                if time_slot[1] <= minT:
                    timeSlots.pop(i)  # Xóa phần tử nếu end_time <= minT
                elif time_slot[0] <= minT < time_slot[1]:
                    usedEndTime.append(time_slot[1])
                    i += 1
                else:
                    print("------------ timeslot.start > minT --------------", file = sys.stderr)
                    i += 1

        if len(usedEndTime) < 6:
            tTrue = minT
        else:
            idx = len(usedEndTime) - 6
            usedEndTime.sort()
            tTrue = usedEndTime[idx]

        # Accumulate waiting time at this factory (queueing delay before service starts)
        if minT != math.inf and tTrue != math.inf:
            waiting_Sum += max(0, tTrue - minT)

        service_time = minTNodeList[curr_node[minT2VehicleIndex]].service_time
        cur_factory_id = minTNodeList[curr_node[minT2VehicleIndex]].id
        curr_node[minT2VehicleIndex] += 1

        while (curr_node[minT2VehicleIndex] < n_node[minT2VehicleIndex] and
            cur_factory_id == minTNodeList[curr_node[minT2VehicleIndex]].id):

            delivery_item_list = minTNodeList[curr_node[minT2VehicleIndex]].delivery_item_list
            
            if delivery_item_list and len(delivery_item_list) > 0:
                before_order_id = ""
                next_order_id = ""

                for order_item in delivery_item_list:
                    next_order_id = order_item.order_id
                    if before_order_id != next_order_id:
                        commit_complete_time = order_item.committed_completion_time
                        overtime_Sum += max(0, curr_time[minT2VehicleIndex] - commit_complete_time)
                    before_order_id = next_order_id

            service_time += minTNodeList[curr_node[minT2VehicleIndex]].service_time
            curr_node[minT2VehicleIndex] += 1
            
        if curr_node[minT2VehicleIndex] >= n_node[minT2VehicleIndex]:
            n -= 1
            curr_node[minT2VehicleIndex] = math.inf
            curr_time[minT2VehicleIndex] = math.inf
            n_node[minT2VehicleIndex] = 0
        else:
            dis_and_time = route_map.get((cur_factory_id , minTNodeList[curr_node[minT2VehicleIndex]].id))
            if dis_and_time:
                distance = float(dis_and_time[0])
                time = int(dis_and_time[1])

                curr_time[minT2VehicleIndex] = tTrue + config.APPROACHING_DOCK_TIME + service_time + time
                leave_last_node_time[minT2VehicleIndex] = tTrue + config.APPROACHING_DOCK_TIME + service_time
                driving_dis += distance

        tw = [minT, tTrue + config.APPROACHING_DOCK_TIME + service_time]
        tw_list = dock_table.get(minTNode.id, [])

        tw_list.append(tw)
        dock_table[minTNode.id] = tw_list
    
    # Weight for waiting time; falls back to config.Delta if no dedicated setting is present
    waiting_weight = config.WAITING_WEIGHT
    objF = (config.Delta * overtime_Sum) + (driving_dis / float(len(id_to_vehicle))) + (waiting_weight * waiting_Sum)
    if objF < 0:
        print("the objective function less than 0" , file= sys.stderr)
    return objF




def create_Pickup_Delivery_nodes(tmp_itemList: list[OrderItem] , id_to_factory: Dict[str , Factory]) -> list[Node]:
    res: list[Node] = []
    if tmp_itemList:
        pickup_address =tmp_itemList[0].pickup_factory_id
        delivery_address =  tmp_itemList[0].delivery_factory_id
        for order_item in tmp_itemList:
            if order_item.pickup_factory_id != pickup_address:
                print("The pickup factory of these items is not the same" , file = sys.stderr)
                pickup_address = ""
                break

        for order_item in tmp_itemList:
            if order_item.delivery_factory_id != delivery_address:
                print("The delivery factory of these items is not the same" , file = sys.stderr)
                delivery_address = ""
                break
    else:
        return []

    if len(pickup_address) ==0 or len(delivery_address) == 0:
        return []
    
    pickup_factory = id_to_factory[pickup_address]
    delivery_factory = id_to_factory[delivery_address]

    pickup_item_list = []
    for item in tmp_itemList:
        pickup_item_list.append(item)
    pickup_node = Node(factory_id= pickup_factory.factory_id , delivery_item_list=[] , pickup_item_list= pickup_item_list , lng= pickup_factory.lng , lat= pickup_factory.lat)

    delivery_item_list = []
    for item in reversed(tmp_itemList):
        delivery_item_list.append(item)
    delivery_node = Node(delivery_factory.factory_id,delivery_item_list,[],delivery_factory.lng,delivery_factory.lat)

    res.extend([pickup_node, delivery_node])
    return res



def dispatch_nodePair(node_list: list[Node]  , id_to_vehicle: Dict[str , Vehicle] , vehicleid_to_plan: Dict[str, list[Node]], route_map: Dict[tuple , tuple]  , selected_vehicle: str= None , mode = 'total' ):
    bestInsertVehicleID: str = ''
    bestInsertPosI: int = 0
    bestInsertPosJ: int = 1
    bestNodeList : list[Node] = []
    isExhausive  = False
    new_pickup_node = node_list[0]
    new_delivery_node = node_list[1]
    minCostDelta = math.inf

    for vehicleID , vehicle in id_to_vehicle.items():
        if selected_vehicle is not None and vehicleID != selected_vehicle:
            continue
        
        vehicle_plan = vehicleid_to_plan[vehicleID]
        
        node_list_size = len(vehicle_plan) if vehicle_plan else 0

        insert_pos = 0 
        model_nodes_num = node_list_size + 2
        first_merge_node_num = 0

        if vehicle.des:
            if new_pickup_node.id != vehicle.des.id:
                insert_pos = 1
            
            if vehicle_plan is not None and vehicle_plan:
                for node in vehicle_plan:
                    if vehicle.des.id != node.id:
                        break
                    first_merge_node_num += 1

        model_nodes_num -= first_merge_node_num

        modle_node_list : List[Node]= [] # thêm các cặp node gửi và nhận theo thứ tự mới (tuần tự)
        exhaustive_route_node_list : List[Node]= [] # Dùng để lưu giữ kế hoạch của một xe trong quá trình duyệt tham lam
        cp_route_node_list : List[Node] = [] # Một copy của một kế hoạch hiện có 
        if vehicle_plan:
            for node in vehicle_plan:
                cp_route_node_list.append(node)

        empty_pos_num = 0
        
        if model_nodes_num <= 8:
            if first_merge_node_num > 0:
                while first_merge_node_num > 0:
                    exhaustive_route_node_list.append(cp_route_node_list.pop(0))
                    first_merge_node_num -= 1
            
            count = 0
            i = 0
            while True:
                if (not cp_route_node_list) or (len(cp_route_node_list) == 0) or (i >= len(cp_route_node_list)):
                    break
                pickup_node = None
                delivery_node = None
                order_item_id = ""
                
                # Kiểm tra pickup_item_list
                if (cp_route_node_list[i].pickup_item_list  and len(cp_route_node_list[i].pickup_item_list) > 0):
                    order_item_id = cp_route_node_list[i].pickup_item_list[0].id
                    pickup_node = cp_route_node_list[i]
                    del cp_route_node_list[i]  # Xóa phần tử tại i
                    
                    # Tìm delivery node
                    j = i
                    while j < len(cp_route_node_list):
                        if (cp_route_node_list[j].delivery_item_list is not None and len(cp_route_node_list[j].delivery_item_list) > 0):
                            item_id = cp_route_node_list[j].delivery_item_list[- 1].id
                            if order_item_id == item_id:  
                                delivery_node = cp_route_node_list[j]
                                del cp_route_node_list[j] 
                                break
                        j += 1
                    
                    # Thêm pickup_node và delivery_node vào modle_node_list tại vị trí count
                    modle_node_list.insert(count, pickup_node)
                    modle_node_list.insert(count + 1, delivery_node)
                    i -= 1  # Giảm i vì danh sách đã bị xóa phần tử
                    count += 1
                
                i += 1

            # Thêm new_order_pickup_node và new_order_delivery_node
            modle_node_list.insert(count, new_pickup_node)
            count += 1
            modle_node_list.insert(count, new_delivery_node)

            empty_pos_num = len(cp_route_node_list) if cp_route_node_list else 0 
            
            while cp_route_node_list:
                modle_node_list.append(cp_route_node_list.pop(0))

            model_nodes_num = len(modle_node_list) + empty_pos_num

        if model_nodes_num <= 8:
            if model_nodes_num == 2:
                exhaustive_route_node_list.append(new_pickup_node)
                exhaustive_route_node_list.append(new_delivery_node)
                tmp_cost = cost_of_a_route(exhaustive_route_node_list , vehicle , id_to_vehicle , route_map , vehicleid_to_plan , mode)

                if tmp_cost < minCostDelta: 
                    minCostDelta = tmp_cost
                    isExhausive = True
                    bestInsertVehicleID = vehicleID
                    bestNodeList = exhaustive_route_node_list[:]
            elif model_nodes_num == 4:
                for i in range(len(config.modle4)):
                    if empty_pos_num == 1:
                        if config.modle4[i][0] == 0:
                            for j in range(1, 4):
                                exhaustive_route_node_list.append(modle_node_list[config.modle4[i][j] - 1])

                            costValue = cost_of_a_route(exhaustive_route_node_list, vehicle , id_to_vehicle , route_map , vehicleid_to_plan , mode)

                            if costValue < minCostDelta:
                                minCostDelta = costValue
                                bestNodeList = exhaustive_route_node_list[:]
                                bestInsertVehicleID = vehicleID
                                isExhausive = True
                            del exhaustive_route_node_list[-3:]
                    else:
                        for j in range(4):
                            exhaustive_route_node_list.append(modle_node_list[config.modle4[i][j]])

                        costValue = cost_of_a_route(exhaustive_route_node_list, vehicle , id_to_vehicle , route_map , vehicleid_to_plan, mode)

                        if costValue < minCostDelta:
                            minCostDelta = costValue
                            bestNodeList = exhaustive_route_node_list[:]
                            bestInsertVehicleID = vehicleID
                            isExhausive = True
                        del exhaustive_route_node_list[-4:]
            elif model_nodes_num == 6:
                for i in range(len(config.modle6)):
                    if empty_pos_num == 1:
                        if config.modle6[i][0] == 0:
                            for j in range(1, 6):
                                exhaustive_route_node_list.append(modle_node_list[config.modle6[i][j] - 1])
                            costValue = cost_of_a_route(exhaustive_route_node_list, vehicle , id_to_vehicle , route_map , vehicleid_to_plan, mode)
                            if costValue < minCostDelta:
                                minCostDelta = costValue
                                bestNodeList = exhaustive_route_node_list[:]
                                bestInsertVehicleID = vehicleID
                                isExhausive = True
                            del exhaustive_route_node_list[-5:]
                    elif empty_pos_num == 2:
                        if config.modle6[i][0] == 0 and config.modle6[i][1] == 1:
                            for j in range(2, 6):
                                exhaustive_route_node_list.append(modle_node_list[config.modle6[i][j] - 2])
                            costValue = cost_of_a_route(exhaustive_route_node_list, vehicle ,id_to_vehicle ,route_map , vehicleid_to_plan , mode)
                            if costValue < minCostDelta:
                                minCostDelta = costValue
                                bestNodeList = exhaustive_route_node_list[:]
                                bestInsertVehicleID = vehicleID
                                isExhausive = True
                            del exhaustive_route_node_list[-4:]
                    else:
                        for j in range(6):
                            exhaustive_route_node_list.append(modle_node_list[config.modle6[i][j]])
                        costValue = cost_of_a_route(exhaustive_route_node_list, vehicle , id_to_vehicle , route_map, vehicleid_to_plan, mode)
                        if costValue < minCostDelta:
                            minCostDelta = costValue
                            bestNodeList = exhaustive_route_node_list[:]
                            bestInsertVehicleID = vehicleID
                            isExhausive = True
                        del exhaustive_route_node_list[-6:]
            elif model_nodes_num == 8:
                for i in range(len(config.modle8)):
                    if empty_pos_num == 1:
                        if config.modle8[i][0] == 0:
                            for j in range(1, 8):
                                exhaustive_route_node_list.append(modle_node_list[config.modle8[i][j] - 1])
                            costValue = cost_of_a_route(exhaustive_route_node_list, vehicle , id_to_vehicle , route_map, vehicleid_to_plan, mode)
                            if costValue < minCostDelta:
                                minCostDelta = costValue
                                bestNodeList = exhaustive_route_node_list[:]
                                bestInsertVehicleID = vehicleID
                                isExhausive = True
                            del exhaustive_route_node_list[-7:]
                    elif empty_pos_num == 2:
                        if config.modle8[i][0] == 0 and config.modle8[i][1] == 1:
                            for j in range(2, 8):
                                exhaustive_route_node_list.append(modle_node_list[config.modle8[i][j] - 2])
                            costValue = cost_of_a_route(exhaustive_route_node_list, vehicle , id_to_vehicle , route_map, vehicleid_to_plan, mode)
                            if costValue < minCostDelta:
                                minCostDelta = costValue
                                bestNodeList = exhaustive_route_node_list[:]
                                bestInsertVehicleID = vehicleID
                                isExhausive = True
                            del exhaustive_route_node_list[-6:]
                    elif empty_pos_num == 3:
                        if config.modle8[i][0] == 0 and config.modle8[i][1] == 1 and config.modle8[i][2] == 2:
                            for j in range(3, 8):
                                exhaustive_route_node_list.append(modle_node_list[config.modle8[i][j] - 3])
                            costValue = cost_of_a_route(exhaustive_route_node_list, vehicle , id_to_vehicle , route_map, vehicleid_to_plan, mode)
                            if costValue < minCostDelta:
                                minCostDelta = costValue
                                bestNodeList = exhaustive_route_node_list[:]
                                bestInsertVehicleID = vehicleID
                                isExhausive = True
                            del exhaustive_route_node_list[-5:]
                    else:
                        for j in range(8):
                            exhaustive_route_node_list.append(modle_node_list[config.modle8[i][j]])
                        costValue = cost_of_a_route(exhaustive_route_node_list, vehicle , id_to_vehicle , route_map, vehicleid_to_plan, mode)
                        if costValue < minCostDelta:
                            minCostDelta = costValue
                            bestNodeList = exhaustive_route_node_list[:]
                            bestInsertVehicleID = vehicleID
                            isExhausive = True
                        del exhaustive_route_node_list[-8:]
        else:
            for i in range(insert_pos, node_list_size + 1):
                if vehicle_plan is not None:
                    #tempRouteNodeList = copy.deepcopy(vehicle_plan)
                    tempRouteNodeList : List[Node]= []
                    for nnn in vehicle_plan: tempRouteNodeList.append(nnn)
                else:
                    tempRouteNodeList = []

                tempRouteNodeList.insert(i, new_pickup_node)

                for j in range(i + 1, node_list_size + 2):
                    if j != i + 1 and tempRouteNodeList[j - 1].pickup_item_list:
                        for k in range(j, node_list_size + 2):
                            if tempRouteNodeList[k].delivery_item_list:
                                if tempRouteNodeList[j - 1].pickup_item_list[0].id == tempRouteNodeList[k].delivery_item_list[- 1].id:
                                    j = k + 1
                                    break

                    elif tempRouteNodeList[j - 1].delivery_item_list :
                        is_terminal = True
                        for k in range(j - 2, -1, -1):
                            if tempRouteNodeList[k].pickup_item_list:
                                if tempRouteNodeList[j - 1].delivery_item_list[- 1].id == tempRouteNodeList[k].pickup_item_list[0].id:
                                    if k < i:
                                        is_terminal = True
                                        break
                                    elif k > i:
                                        is_terminal = False
                                        break
                        if is_terminal:
                            break

                    tempRouteNodeList.insert(j, new_delivery_node)

                    costValue = cost_of_a_route(tempRouteNodeList, vehicle, id_to_vehicle , route_map , vehicleid_to_plan, mode)
                    if costValue < minCostDelta:
                        minCostDelta = costValue
                        bestInsertPosI = i
                        bestInsertPosJ = j
                        bestInsertVehicleID = vehicleID
                        isExhausive = False

                    tempRouteNodeList.pop(j)
        #print(minCostDelta , , file = sys.stderr)
    #print(f"Best cost {minCostDelta}" , , file = sys.stderr)
    return isExhausive , bestInsertVehicleID, bestInsertPosI, bestInsertPosJ , bestNodeList



def dispatch_new_orders(vehicleid_to_plan: Dict[str , list[Node]] ,  id_to_factory:Dict[str , Factory] , route_map: Dict[tuple , tuple] ,  id_to_vehicle: Dict[str , Vehicle] , id_to_unlocated_items:Dict[str , OrderItem], new_order_itemIDs: list[str]):
    if new_order_itemIDs:
        orderId_to_Item : Dict[str , list[OrderItem]] = {}
        for new_order_item in new_order_itemIDs:
            new_item = id_to_unlocated_items.get(new_order_item)
            orderID  = new_item.order_id
            if orderID not in orderId_to_Item:
                orderId_to_Item[orderID] = []
            orderId_to_Item.get(orderID).append(new_item)
        
        for vehicle in id_to_vehicle.values():
            capacity = vehicle.board_capacity
            break
        
        for orderID , orderID_items in orderId_to_Item.items():
            order_demand = 0
            for item in orderID_items:
                order_demand += item.demand
            
            if order_demand > capacity:
                tmp_demand = 0
                tmp_itemList: list[OrderItem] = []
                
                for item in orderID_items:
                    if (tmp_demand + item.demand) > capacity:
                        node_list: list[Node] = create_Pickup_Delivery_nodes(tmp_itemList , id_to_factory)
                        isExhausive = False
                        route_node_list : List[Node] = []
                        
                        if node_list:
                            isExhausive , bestInsertVehicleID, bestInsertPosI, bestInsertPosJ , bestNodeList = dispatch_nodePair(node_list , id_to_vehicle , vehicleid_to_plan , route_map)
                        
                        route_node_list = vehicleid_to_plan.get(bestInsertVehicleID , [])

                        if isExhausive:
                            route_node_list = bestNodeList[:]
                        else:
                            if route_node_list is None:
                                route_node_list = []
                            
                            new_order_pickup_node = node_list[0]
                            new_order_delivery_node = node_list[1]
                            
                            route_node_list.insert(bestInsertPosI, new_order_pickup_node)
                            route_node_list.insert(bestInsertPosJ, new_order_delivery_node)
                        vehicleid_to_plan[bestInsertVehicleID] = route_node_list
                        
                        tmp_itemList.clear()
                        tmp_demand = 0
                    tmp_itemList.append(item)
                    tmp_demand += item.demand 

                if len(tmp_itemList) > 0:
                    node_list: list[Node] = create_Pickup_Delivery_nodes(tmp_itemList , id_to_factory)
                    isExhausive = False
                    
                    if node_list:
                        isExhausive , bestInsertVehicleID, bestInsertPosI, bestInsertPosJ , bestNodeList =  dispatch_nodePair(node_list , id_to_vehicle , vehicleid_to_plan, route_map)
                    route_node_list : List[Node] = vehicleid_to_plan.get(bestInsertVehicleID , [])
                    
                    if isExhausive:
                        route_node_list = bestNodeList[:]
                    else:
                        if route_node_list is None:
                            route_node_list = []
                        
                        new_order_pickup_node = node_list[0]
                        new_order_delivery_node = node_list[1]
                        
                        route_node_list.insert(bestInsertPosI, new_order_pickup_node)
                        route_node_list.insert(bestInsertPosJ, new_order_delivery_node)
                    vehicleid_to_plan[bestInsertVehicleID] = route_node_list
            else:
                node_list: list[Node] = create_Pickup_Delivery_nodes(orderID_items , id_to_factory)
                
                isExhausive = False
                if node_list:
                    isExhausive , bestInsertVehicleID, bestInsertPosI, bestInsertPosJ , bestNodeList = dispatch_nodePair(node_list , id_to_vehicle , vehicleid_to_plan , route_map)
                route_node_list : List[Node] = vehicleid_to_plan.get(bestInsertVehicleID , [])
                if isExhausive:
                    route_node_list = bestNodeList[:]
                else:
                    if route_node_list is None:
                        route_node_list = []
                    
                    new_order_pickup_node = node_list[0]
                    new_order_delivery_node = node_list[1]
                    
                    route_node_list.insert(bestInsertPosI, new_order_pickup_node)
                    route_node_list.insert(bestInsertPosJ, new_order_delivery_node)
                vehicleid_to_plan[bestInsertVehicleID] = route_node_list




def get_UnongoingSuperNode (vehicleid_to_plan: Dict[str , List[Node]] , id_to_vehicle: Dict[str , Vehicle] ) -> Tuple[Dict[int , Dict[str, Node]] , Dict[str , List[Node]]]:
    UnongoingSuperNodes : Dict[int , Dict[str , Node]] = {}

    Base_vehicleid_to_plan : Dict[str , List[Node]] = {}
    
    NodePairNum = 0 
    # xet từng kế hoạch di chuyển của phương tiện
    for vehicleID , vehicle_plan in vehicleid_to_plan.items():
        vehicle = id_to_vehicle[vehicleID]
        Base_vehicleid_to_plan[vehicleID] = []
        for node in vehicle_plan:
            Base_vehicleid_to_plan[vehicleID].append(node)
            
        if vehicle_plan and len(vehicle_plan) > 0:
            index = 1 if vehicle.des else 0
            
            # Để duy trì tính FILO 
            pickup_node_heap : List[Node] = []
            p_node_idx_heap : List[int] = []
            p_and_d_node_map : Dict[str , Node] = {}
            before_p_factory_id , before_d_factory_id = None , None
            before_p_node_idx , before_d_node_idx = 0 , 0
            
            node_to_remove = set()
            
            # đối với mỗi node của phương tiện
            for i in range (index , len(vehicle_plan)):
                curr = vehicle_plan[i]
                if curr.delivery_item_list  and curr.pickup_item_list :
                    print ("Exits combine node exception when Local search" , file= sys.stderr)
                
                heapTopOrderItemId = pickup_node_heap[0].pickup_item_list[0].id if pickup_node_heap else ""
                # Nếu node hiện tại là node giao
                if curr.delivery_item_list:
                    # Nó là node giao của node nhận đầu tiên trong heap
                    if curr.delivery_item_list[-1].id == heapTopOrderItemId:
                        pickup_node_key = f"{vehicleID},{p_node_idx_heap[0]}"
                        delivery_node_key = f"{vehicleID},{int(i)}"
                
                        if len(p_and_d_node_map) >= 2:
                            if ((pickup_node_heap[0].id != before_p_factory_id) or (p_node_idx_heap[0] + 1 != before_p_node_idx) or (curr.id != before_d_factory_id) or (i - 1 != before_d_node_idx)):

                                while p_and_d_node_map:
                                    temp = dict(list(p_and_d_node_map.items())[:2])
                                    UnongoingSuperNodes[NodePairNum] = temp
                                    NodePairNum += 1
                                    for key in temp.keys():
                                        del p_and_d_node_map[key]
                                        _ , idx = key.split(',')
                                        node_to_remove.add(int(idx))

                        p_and_d_node_map[pickup_node_key] = pickup_node_heap[0]
                        p_and_d_node_map[delivery_node_key] = curr

                        before_p_factory_id = pickup_node_heap[0].id
                        before_p_node_idx = p_node_idx_heap[0]
                        before_d_factory_id = curr.id
                        before_d_node_idx = i
                        pickup_node_heap.pop(0)
                        p_node_idx_heap.pop(0)
                
                # Nếu node hiện tại là node nhận
                if curr.pickup_item_list:
                    pickup_node_heap.insert(0 , curr)
                    p_node_idx_heap.insert(0 , i)
                    if p_and_d_node_map:
                        while p_and_d_node_map:
                            temp = dict(list(p_and_d_node_map.items())[:2])
                            UnongoingSuperNodes[NodePairNum] = temp
                            #print(UnongoingSuperNodes, file = sys.stderr)
                            NodePairNum += 1
                            for key in temp.keys():
                                del p_and_d_node_map[key]
                                _ , idx = key.split(',')
                                node_to_remove.add(int(idx))
            
            if len(p_and_d_node_map) >= 2:
                
                # Đã sửa phần này
                while p_and_d_node_map:
                    temp = dict(list(p_and_d_node_map.items())[:2])
                    UnongoingSuperNodes[NodePairNum] = temp
                    #print(UnongoingSuperNodes, file = sys.stderr)
                    NodePairNum += 1
                    for key in temp.keys():
                        del p_and_d_node_map[key]
                        _ , idx = key.split(',')
                        node_to_remove.add(int(idx))
                p_and_d_node_map = {}
                
            if node_to_remove:
                Base_vehicleid_to_plan[vehicleID] = [node for i , node in enumerate(vehicle_plan) if i not in node_to_remove]

    return UnongoingSuperNodes , Base_vehicleid_to_plan


def new_generate_random_chromosome(initial_vehicleid_to_plan : Dict[str , List[Node]],  route_map: Dict[Tuple, Tuple], id_to_vehicle: Dict[str, Vehicle], Unongoing_super_nodes : Dict[int , Dict[str, Node]]  ,Base_vehicleid_to_plan : Dict[str , List[Node]] , quantity : int):
    ls_node_pair_num = len(Unongoing_super_nodes)
    if ls_node_pair_num == 0:
        return None , None

    #Quan the
    population : List[Chromosome] = []
    number_of_node = 0
    for plan in initial_vehicleid_to_plan.values():
        number_of_node += len(plan)
    
    pdg_Map : Dict[str , List[Node]] = {}
    
    # tao Dict cac super node
    for idx, pdg in Unongoing_super_nodes.items():
        pickup_node = None
        delivery_node = None
        node_list: List[Node] = []
        pos_i = 0
        pos_j = 0
        d_num = len(pdg) // 2
        index = 0

        if pdg:
            vehicleID = ''
            for v_and_pos_str, node in (pdg.items()):
                vehicleID = v_and_pos_str.split(",")[0]
                if index % 2 == 0:
                    pos_i = int(v_and_pos_str.split(",")[1])
                    pickup_node = node
                    node_list.insert(0, pickup_node)
                    index += 1
                else:
                    pos_j = int(v_and_pos_str.split(",")[1])
                    delivery_node = node
                    node_list.append(delivery_node)
                    index += 1
                    pos_j = int(pos_j - d_num + 1)
            
            k : str = f"{vehicleID},{int(pos_i)}+{int(pos_j)}"
            pdg_Map[k] = node_list
    if len(pdg_Map) < 2:
        return None , None
    
    while len(population) < quantity:
        new_individual = disturbance_opt(initial_vehicleid_to_plan , id_to_vehicle , route_map , 0.5)
        
        if new_individual:
            population.append(new_individual)
    
    population.append(Chromosome(initial_vehicleid_to_plan , route_map , id_to_vehicle))
    return population , pdg_Map 



def disturbance_opt(vehicleid_to_plan: Dict[str , List[Node]], id_to_vehicle: Dict[str , Vehicle] , route_map: Dict[tuple , tuple] , relocate_rate = 0.3):
    begin = time.time()
    
    new_vehicle_to_plan : Dict[str , List[Node]] = {}
    for VID , plan in vehicleid_to_plan.items():
        new_vehicle_to_plan[VID] = []
        for node in plan:
            new_vehicle_to_plan[VID].append(copy.deepcopy(node))
            
    dis_order_super_node,  _ = get_UnongoingSuperNode(vehicleid_to_plan , id_to_vehicle)
    ls_node_pair_num = len(dis_order_super_node)
    if ls_node_pair_num == 0:
        return None
    
    pdg_Map : Dict[str , List[Node]] = {}
    
    for idx, pdg in dis_order_super_node.items():
        pickup_node = None
        delivery_node = None
        node_list: List[Node] = []
        pos_i = 0
        pos_j = 0
        d_num = len(pdg) // 2
        index = 0

        if pdg:
            for v_and_pos_str, node in pdg.items():
                if index % 2 == 0:
                    vehicleID = v_and_pos_str.split(",")[0]
                    pos_i = int(v_and_pos_str.split(",")[1])
                    pickup_node = node
                    node_list.insert(0, pickup_node)
                    index += 1
                else:
                    pos_j = int(v_and_pos_str.split(",")[1])
                    delivery_node = node
                    node_list.append(delivery_node)
                    index += 1
                    pos_j = pos_j - d_num + 1

            k : str = f"{vehicleID},{int(pos_i)}+{int(pos_j)}"
            pdg_Map[k] = node_list
            
    if len(pdg_Map) < 2:
        return None

    num_pairs_to_relocate = max(1, int(len(pdg_Map) * relocate_rate))
    pairs_to_relocate = random.sample(list(pdg_Map.keys()), num_pairs_to_relocate)
    
    # Lưu trữ các cặp node sẽ được gán lại
    relocated_pairs : Dict[str, List[Node]] = {}
    
    # Nhóm các cặp node cần xóa theo xe
    vehicle_removal_info : Dict[str, List[tuple]] = {}
    
    for key in pairs_to_relocate:
        relocated_pairs[key] = pdg_Map[key]
        
        # Lấy thông tin vị trí và xe
        vehicle_pos_info = key.split(',')
        vehicle_id = vehicle_pos_info[0]
        positions = vehicle_pos_info[1].split('+')
        pos_i = int(positions[0])
        pos_j = int(positions[1])
        
        node_list = pdg_Map[key]
        d_num = len(node_list) // 2
        
        # Thêm thông tin xóa vào dictionary theo xe
        if vehicle_id not in vehicle_removal_info:
            vehicle_removal_info[vehicle_id] = []
        vehicle_removal_info[vehicle_id].append((pos_i, pos_i + d_num, pos_j, pos_j + d_num))
    
    # Xây dựng lời giải sau khi bỏ những cặp sẽ được gán lại
    for vehicle_id, removal_list in vehicle_removal_info.items():
        route_node_list = new_vehicle_to_plan.get(vehicle_id, [])
        
        # Thu thập tất cả các chỉ số cần xóa
        indices_to_remove = set()
        for pos_i_start, pos_i_end, pos_j_start, pos_j_end in removal_list:
            # Thêm chỉ số pickup nodes
            for idx in range(pos_i_start, pos_i_end):
                if idx < len(route_node_list):
                    indices_to_remove.add(idx)
            # Thêm chỉ số delivery nodes
            for idx in range(pos_j_start, pos_j_end):
                if idx < len(route_node_list):
                    indices_to_remove.add(idx)
        
        # Sắp xếp chỉ số theo thứ tự giảm dần để xóa từ cuối lên đầu
        sorted_indices = sorted(indices_to_remove, reverse=True)
        
        # Xóa các node theo thứ tự từ cuối lên đầu
        for idx in sorted_indices:
            if idx < len(route_node_list):
                del route_node_list[idx]
        
        new_vehicle_to_plan[vehicle_id] = route_node_list
    
    # Gán lại các cặp node đã đánh dấu một cách ngẫu nhiên vào tuyến đường
    for key, node_list in relocated_pairs.items():
        # Sử dụng random_dispatch_nodePair để gán ngẫu nhiên cặp node
        random_dispatch_nodePair(node_list, id_to_vehicle, new_vehicle_to_plan)
    
    
    #print(time.time() - begin)
    return Chromosome(new_vehicle_to_plan , route_map , id_to_vehicle)



def random_dispatch_nodePair(node_list: list[Node], id_to_vehicle: Dict[str, Vehicle], vehicleid_to_plan: Dict[str, list[Node]]):
    """
    Randomly insert a pickup-delivery pair into a vehicle route without:
    - losing nodes,
    - duplicating nodes,
    - or altering the destination position (index 0) for vehicles that have a current destination.
    """
    if not node_list or len(node_list) < 2:
        return

    is_inserted = False
    attempts = 0
    MAX_ATTEMPTS = 500
    # Probability to try simple end-extension (append pickup & delivery at tail)
    p_extend_end = 1
    while not is_inserted:
        selected_vehicleID = random.choice(list(id_to_vehicle.keys()))
        selected_vehicle = id_to_vehicle[selected_vehicleID]

        # Ensure the plan list exists
        route = vehicleid_to_plan.get(selected_vehicleID)
        if route is None:
            route = []
            vehicleid_to_plan[selected_vehicleID] = route

        begin_pos = 1 if selected_vehicle.des else 0  # never insert before index 0 if a destination exists
        check_end = False
        old_len = len(route)

        # If route is empty but vehicle currently has a destination, put that destination at index 0 first
        if old_len == 0 and selected_vehicle.des is not None:
            route.append(selected_vehicle.des)
            old_len = 1  # destination preserved at index 0

        pickup_node = None
        delivery_node = None
        if node_list[0].pickup_item_list and not node_list[0].delivery_item_list:
            pickup_node , delivery_node = node_list[0] , node_list[-1]
        else: 
            delivery_node , pickup_node = node_list[0] , node_list[-1]
            

        # Helper: get current carrying items as Python list (bottom -> top)
        def _get_carrying_list(v: Vehicle) -> List[OrderItem]:
            ci = getattr(v, 'carrying_items', None)
            # Try to deep copy stack-like object
            try:
                ci_copy = copy.deepcopy(ci)
            except Exception:
                ci_copy = ci
            # Stack-like: has is_empty/pop
            try:
                items_top_first: List[OrderItem] = []
                while ci_copy is not None and hasattr(ci_copy, 'is_empty') and not ci_copy.is_empty():
                    items_top_first.append(ci_copy.pop())
                # convert top->bottom to bottom->top for isFeasible
                return list(reversed(items_top_first)) if items_top_first else (list(ci_copy) if isinstance(ci_copy, list) else [])
            except Exception:
                # list-like
                try:
                    return list(ci_copy) if ci_copy is not None else []
                except Exception:
                    return []

        if old_len == 0:
            # No destination and no existing route -> simply append pair (feasibility-checked)
            route.extend([pickup_node, delivery_node])
            carrying_items = _get_carrying_list(selected_vehicle)
            if isFeasible(route, carrying_items, selected_vehicle.board_capacity):
                is_inserted = True
            else:
                # revert and retry with another vehicle
                route.pop(); route.pop()
        else:
            # Branch 1: try end-extension with probability p_extend_end
            did_try_extend = False
            if random.random() < p_extend_end:
                did_try_extend = True
                route.append(pickup_node)
                route.append(delivery_node)
                carrying_items = _get_carrying_list(selected_vehicle)
                # Validate destination position & feasibility
                if (selected_vehicle.des and route[0].id != selected_vehicle.des.id) or not isFeasible(route, carrying_items, selected_vehicle.board_capacity):
                    # Revert
                    route.pop(); route.pop()
                else:
                    is_inserted = True

            # Branch 2: general random insertion search if not inserted yet
            if not is_inserted and not did_try_extend:
                # Try random feasible positions for pickup and delivery (delivery strictly after pickup)
                feasible_position1 = [i for i in range(begin_pos, len(route) + 1)]
                random.shuffle(feasible_position1)
                for insert_posI in feasible_position1:
                    feasible_position2 = [i for i in range(insert_posI + 1, len(route) + 2)]
                    random.shuffle(feasible_position2)
                    for insert_posJ in feasible_position2:
                        route.insert(insert_posI, pickup_node)
                        route.insert(insert_posJ, delivery_node)

                        # Do not allow changing destination position at index 0
                        if selected_vehicle.des and route and route[0].id != selected_vehicle.des.id:
                            # revert immediately if destination got shifted
                            route.pop(insert_posJ)
                            route.pop(insert_posI)
                            continue

                        carrying_items = _get_carrying_list(selected_vehicle)
                        if isFeasible(route, carrying_items, selected_vehicle.board_capacity):
                            check_end = True
                            break

                        # Revert failed attempt
                        route.pop(insert_posJ)
                        route.pop(insert_posI)
                    if check_end:
                        break

        # Confirm exactly two nodes were added and, if applicable, destination preserved
        if len(route) == old_len + 2:
            if not selected_vehicle.des or (route and route[0].id == selected_vehicle.des.id):
                is_inserted = True

        attempts += 1
        if not is_inserted and attempts >= MAX_ATTEMPTS:
            # Fallback: try exhaustive feasible insertion over all vehicles/positions deterministically
            for vID, v in id_to_vehicle.items():
                r = vehicleid_to_plan.get(vID)
                if r is None:
                    r = []
                    vehicleid_to_plan[vID] = r
                begin_pos2 = 1 if v.des else 0
                base_len = len(r)
                if base_len == 0 and v.des is not None:
                    r.append(v.des); base_len = 1
                for i in range(begin_pos2, len(r) + 1):
                    for j in range(i + 1, len(r) + 2):
                        r.insert(i, pickup_node)
                        r.insert(j, delivery_node)
                        # keep destination header
                        if v.des and r and r[0].id != v.des.id:
                            r.pop(j); r.pop(i)
                            continue
                        if isFeasible(r, _get_carrying_list(v), v.board_capacity):
                            return
                        r.pop(j); r.pop(i)
            # If still not inserted, log and return without modification (caller may handle)
            print("[random_dispatch_nodePair] Failed to insert PD pair after exhaustive search", file=sys.stderr)
            return
        
        
