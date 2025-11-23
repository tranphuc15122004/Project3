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
from algorithm.engine import *



def new_crossver2(parent1: Chromosome , parent2: Chromosome , Base_vehicleid_to_plan : Dict[str , List[Node]] , PDG_map: Dict[str , List[Node]] , static_single_pass: bool = False):
    begin = time.time()
    
    # Cac super node
    new_PDG_map : Dict[str , List[Node]] = {}
    for key , value in PDG_map.items():
        if len(value )!= 2: print("ERROR 1")
        first_node = value[0]
        if first_node.pickup_item_list and first_node.delivery_item_list:
            print ("ERROR 2")
        if first_node.pickup_item_list:
            key = f'{len(first_node.pickup_item_list)}_{first_node.pickup_item_list[0].id}'
        elif first_node.delivery_item_list:
            key = f'{len(first_node.delivery_item_list)}_{first_node.delivery_item_list[-1].id}'
        
        
        new_PDG_map[key] = value
        
    check_valid : Dict[str , int]= {key : 0 for key in new_PDG_map.keys()}
    
    blockmap_parent1 = extract_block_from_solution(parent1.solution , parent1.id_to_vehicle)
    blockmap_parent2 = extract_block_from_solution(parent2.solution , parent2.id_to_vehicle)


    # ========= Khởi tạo biến điều khiển vòng lặp =========
    DEBUG = False  # set True to enable verbose logging
    start_time = time.time()
    iteration = 0
    stagnation = 0  # số vòng không thu thêm signature mới
    total_blocks_target = len(new_PDG_map) if new_PDG_map else 0
    # Tham số dừng (có thể điều chỉnh / đưa ra ngoài nếu cần tinh chỉnh sau)
    MAX_ITER = max(5, 2 * total_blocks_target) if total_blocks_target > 0 else 20
    MAX_NO_GAIN = 5          # số vòng liên tiếp không có gain mới thì dừng
    TIME_BUDGET_SEC = 15   # ngân sách thời gian cho riêng crossover này
    MIN_GAIN_PER_BLOCK = 1   # yêu cầu tối thiểu signature mới / vòng

    # Ghi lại lý do dừng cuối cùng (debug)
    last_stop_reason = None

    def is_finished() -> bool:
        """Điều kiện dừng tổng hợp cho vòng lặp chọn block.

        Các tiêu chí:
        1. Timeout toàn cục (config.is_timeout()).
        2. Hết block (cả hai map rỗng hoặc None).
        3. Đủ coverage (số signature đã dùng >= tổng số super nhóm ban đầu).
        4. Quá số vòng lặp tối đa.
        5. Vượt ngân sách thời gian cục bộ.
        6. Stagnation (không có gain mới quá MAX_NO_GAIN).
        """
        nonlocal last_stop_reason
        # 1. Timeout toàn cục
        if hasattr(config, 'is_timeout') and config.is_timeout():
            last_stop_reason = 'global-timeout'
            return True
        # 2. Hết block
        empty1 = (not blockmap_parent1) or (len(blockmap_parent1) == 0)
        empty2 = (not blockmap_parent2) or (len(blockmap_parent2) == 0)
        if empty1 and empty2:
            last_stop_reason = 'no-blocks-left'
            return True
        # 3. Coverage đủ
        if total_blocks_target > 0 and len(used_signatures) >= total_blocks_target:
            last_stop_reason = 'coverage-complete'
            return True
        # 4. Quá số vòng
        if iteration >= MAX_ITER:
            last_stop_reason = 'max-iter'
            return True
        # 5. Hết ngân sách thời gian cục bộ
        if (time.time() - start_time) >= TIME_BUDGET_SEC:
            last_stop_reason = 'time-budget'
            return True
        # 6. Stagnation
        if stagnation >= MAX_NO_GAIN:
            last_stop_reason = 'stagnation'
            return True
        return False
    
    
    used_signatures : set[str] = set()

    child_vehicleid_to_plan : Dict[str , List[Node]] = copy.deepcopy(Base_vehicleid_to_plan)

    # Seed used_signatures from any existing nodes in base plan (avoid re-adding groups already present)
    for v_nodes in child_vehicleid_to_plan.values():
        used_signatures.update(extract_pickup_signatures(v_nodes))
    if DEBUG and used_signatures:
        print(f"[new_crossver2] Seeded {len(used_signatures)} signatures from base plan", file=sys.stderr)
    
    prev_block = None
    if DEBUG:
        print(f"[new_crossver2] START | total_target_blocks={len(new_PDG_map)}", file=sys.stderr)

    # Optional FAST PATH: compute scores once and insert in a single ordered pass
    if static_single_pass:
        # 1) Compute scores once
        score_map_p1 = block_scoring_func(blockmap_parent1, parent1.solution, parent2.solution, parent1.route_map)
        score_map_p2 = block_scoring_func(blockmap_parent2, parent1.solution, parent2.solution, parent1.route_map)

        # 2) Build candidate list
        candidates = []  # (origin, key, nodes, dis, time, demand)
        if blockmap_parent1:
            for k, nodes in blockmap_parent1.items():
                sc = score_map_p1.get(k)
                if not sc:
                    continue
                d, t, dem = sc
                if math.isinf(d) or math.isinf(t) or math.isinf(dem):
                    continue
                candidates.append(("P1", k, nodes, d, t, dem))
        if blockmap_parent2:
            for k, nodes in blockmap_parent2.items():
                sc = score_map_p2.get(k)
                if not sc:
                    continue
                d, t, dem = sc
                if math.isinf(d) or math.isinf(t) or math.isinf(dem):
                    continue
                candidates.append(("P2", k, nodes, d, t, dem))

        # Prefilter by demand desc then (d+t) asc to shrink candidate set
        if len(candidates) > PREFILTER_K:
            candidates.sort(key=lambda x: (-x[5], (x[3] + x[4])))
            candidates = candidates[:PREFILTER_K]

        # 3) Compute skyline front once
        def skyline_front(items):
            items_sorted = sorted(items, key=lambda x: (x[3], x[4]))  # by (distance, time)
            best_time = math.inf
            # Use (origin,key) as identity to avoid unhashable list in tuple
            front_identities = set()
            for it in items_sorted:
                if it[4] < best_time:
                    best_time = it[4]
                    front_identities.add((it[0], it[1]))  # (origin, key)
            return front_identities

        front_set = skyline_front(candidates)
        # 4) Static ranking: prefer skyline members, then demand desc, then (d+t)
        candidates.sort(key=lambda x: (0 if (x[0], x[1]) in front_set else 1, -x[5], (x[3] + x[4])))

        # 5) Single pass insertion
        for origin, key, nodes, d, t, dem in candidates:
            # If already fully covered all PD groups, stop
            if total_blocks_target > 0 and len(used_signatures) >= total_blocks_target:
                break

            # Skip if any of its pickup signatures already used
            def block_pickup_sigs(ns: List[Node]) -> set[str]:
                s = set()
                for nd in ns:
                    if nd.pickup_item_list:
                        s.add(f"{len(nd.pickup_item_list)}_{nd.pickup_item_list[0].id}")
                return s
            pick_sigs = block_pickup_sigs(nodes)
            if any(sig in used_signatures for sig in pick_sigs):
                continue

            # Determine target vehicle (currently not enforcing source vehicle mapping)
            selected_vehicle: Optional[str] = None

            # Try insertion at best position (append-based CI currently)
            bestInsertPos, bestInsertVehicle = cheapest_insertion_for_block(
                nodes,
                parent1.id_to_vehicle,
                child_vehicleid_to_plan,
                parent1.route_map,
                selected_vehicle=None,
            )
            if bestInsertVehicle is None:
                # Strict mode: if enforcing source vehicle and no feasible insertion, skip this block
                continue
            target_route = child_vehicleid_to_plan[bestInsertVehicle]
            target_route[bestInsertPos: bestInsertPos] = nodes
            target_vehicle = parent1.id_to_vehicle[bestInsertVehicle] 
            
            if not isFeasible(target_route , target_vehicle.carrying_items , target_vehicle.board_capacity):
                # rollback
                del target_route[bestInsertPos: bestInsertPos + len(nodes)]
                continue

            # Update signatures and drop overlaps in blockmaps (no salvage)
            used_signatures.update(pick_sigs)
            update_blockmap_drop_overlap(blockmap_parent1, blockmap_parent2, nodes, used_signatures)

    # Caches for block scores to avoid recomputation across iterations (dynamic mode)
    score_cache_p1: Dict[str, Tuple[float, float, float]] = {}
    score_cache_p2: Dict[str, Tuple[float, float, float]] = {}
    
    while not static_single_pass:
        begin  = time.time()
        if is_finished():
            if DEBUG:
                print(f"[new_crossver2] STOP before-iter reason={last_stop_reason} iter={iteration}", file=sys.stderr)
            break
        try:
            # Log trạng thái đầu vòng lặp (coverage hiện tại trước khi chọn block mới)
            if DEBUG:
                print(
                    f"[new_crossver2] Iter {iteration} | blocks_p1={len(blockmap_parent1) if blockmap_parent1 else 0} | "
                    f"blocks_p2={len(blockmap_parent2) if blockmap_parent2 else 0} | used_sig={len(used_signatures)}/{total_blocks_target} | "
                    f"stagn={stagnation}",
                    file=sys.stderr
                )

            # Cập nhật block map với block trước đó
            update_blockmap(blockmap_parent1, blockmap_parent2, prev_block, used_signatures)
            
            # Điểm các block
            block_score_parent1 = compute_block_scores_incremental(blockmap_parent1, parent1.route_map, score_cache_p1)
            block_score_parent2 = compute_block_scores_incremental(blockmap_parent2, parent1.route_map, score_cache_p2)
            candidate_count = (len(block_score_parent1) if block_score_parent1 else 0) + (len(block_score_parent2) if block_score_parent2 else 0)
            if DEBUG:
                print(f"[new_crossver2] Iter {iteration} | candidate_blocks={candidate_count}", file=sys.stderr)

            best_block = find_best_block(blockmap_parent1, blockmap_parent2, block_score_parent1, block_score_parent2)
            if not best_block:
                last_stop_reason = 'no-candidate'
                if DEBUG:
                    print(f"[new_crossver2] Iter {iteration} | no candidates -> stopping", file=sys.stderr)
                break

            # Lấy key block (nếu xác định được) để log
            found_key = None
            for k, v in (blockmap_parent1 or {}).items():
                if v is best_block:
                    found_key = f"P1:{k}"
                    break
            if not found_key:
                for k, v in (blockmap_parent2 or {}).items():
                    if v is best_block:
                        found_key = f"P2:{k}"
                        break
            
            new_sigs = block_signatures(best_block)
            # Skip block if any of its pickup signatures already used (to avoid duplicates & LIFO conflicts)
            pickup_sigs = {s for s in new_sigs if '_' in s}  # all are pickup style already
            if any(sig in used_signatures for sig in pickup_sigs):
                # Remove this exact block from its map to prevent reselection
                removed_from = None
                if blockmap_parent1:
                    for k, v in list(blockmap_parent1.items()):
                        if v is best_block:
                            blockmap_parent1.pop(k, None)
                            removed_from = 'P1'
                            break
                if removed_from is None and blockmap_parent2:
                    for k, v in list(blockmap_parent2.items()):
                        if v is best_block:
                            blockmap_parent2.pop(k, None)
                            removed_from = 'P2'
                            break
                if DEBUG:
                    print(f"[new_crossver2] Iter {iteration} | skip duplicate-signature block {found_key}", file=sys.stderr)
                stagnation += 1
                continue

            gain = len(pickup_sigs - used_signatures)
            if DEBUG:
                print(
                    f"[new_crossver2] Iter {iteration} | chosen_block_key={found_key} | block_len={len(best_block)} | "
                    f"new_pick_sig_gain={gain}",
                    file=sys.stderr
                )
            if gain < MIN_GAIN_PER_BLOCK:
                stagnation += 1
            else:
                stagnation = 0

            bestInsertPos, bestInsertVehicle = cheapest_insertion_for_block(
                best_block,
                parent1.id_to_vehicle,
                child_vehicleid_to_plan,
                parent1.route_map
            )
            if bestInsertVehicle is None:
                last_stop_reason = 'no-insertion-position'
                if DEBUG:
                    print(f"[new_crossver2] Iter {iteration} | insertion failed -> stopping", file=sys.stderr)
                break
            target_route = child_vehicleid_to_plan[bestInsertVehicle]
            target_route[bestInsertPos: bestInsertPos] = best_block
            target_vehicle = parent1.id_to_vehicle[bestInsertVehicle]

            # Validate only the modified vehicle route for LIFO + uniqueness
            if not isFeasible(target_route , target_vehicle.carrying_items , target_vehicle.board_capacity):
                # Rollback insertion
                del target_route[bestInsertPos: bestInsertPos + len(best_block)]
                # Remove this block from future consideration
                removed_from = None
                if blockmap_parent1:
                    for k, v in list(blockmap_parent1.items()):
                        if v is best_block:
                            blockmap_parent1.pop(k, None)
                            removed_from = 'P1'
                            break
                if removed_from is None and blockmap_parent2:
                    for k, v in list(blockmap_parent2.items()):
                        if v is best_block:
                            blockmap_parent2.pop(k, None)
                if DEBUG:
                    print(f"[new_crossver2] Iter {iteration} | rollback block {found_key} (violates LIFO/dup)", file=sys.stderr)
                stagnation += 1
                continue

            # Update coverage after successful insertion
            used_signatures.update(pickup_sigs)
            if DEBUG:
                print(
                    f"[new_crossver2] Iter {iteration} | inserted at vehicle={bestInsertVehicle} pos={bestInsertPos} | "
                    f"coverage={len(used_signatures)}/{total_blocks_target}",
                    file=sys.stderr
                )

            prev_block = best_block
            iteration += 1
        except Exception as e:
            last_stop_reason = 'exception'
            if DEBUG:
                print(f"[new_crossver2] Iter {iteration} | EXCEPTION: {e}", file=sys.stderr)
            import traceback
            if DEBUG:
                traceback.print_exc(file=sys.stderr)
            break
        #print(time.time() - begin)
    
    
    if DEBUG:
        print(
            f"[new_crossver2] END | iterations={iteration} | coverage={len(used_signatures)}/{total_blocks_target} | "
            f"used_signatures={len(used_signatures)} | final_reason={last_stop_reason}",
            file=sys.stderr
        )
    
    # kiểm tra lại lời giải con và xử lý các node thừa hoặc thiếu
    for vehicleID in parent1.id_to_vehicle.keys():
        redundant = []
        del_index = []
        # Duyệt ngược danh sách để tìm và xóa nút thừa    
        for i in range(len(child_vehicleid_to_plan[vehicleID]) - 1, -1, -1):  
            node = child_vehicleid_to_plan[vehicleID][i]
            
            if node.pickup_item_list:
                if redundant and node.pickup_item_list[0].id == redundant[-1]:
                    redundant.pop()  # Loại bỏ phần tử tương ứng trong danh sách `redundant`
                    del_index.append(i)
            else:
                key = f'{len(node.delivery_item_list)}_{node.delivery_item_list[-1].id}'
                
                if key in new_PDG_map:
                    check_valid[key] += 1
                    
                    # nếu tìm được một super node thừa
                    if check_valid[key] > 1:
                        first_itemID_of_redundant_supernode = key.split('_')[-1]
                        redundant.append(first_itemID_of_redundant_supernode)
                        #print(f"Redundant nodes: {redundant}" , file= sys.stderr)
                        # Xóa node giao của super node thừa
                        del_index.append(i)
                        #print('Đã xóa 1 super node thừa' , file= sys.stderr)
        for i in del_index:
            child_vehicleid_to_plan[vehicleID].pop(i)
    
    
    # Kiem tra lai và thêm các node còn thiếu vào con   
    for key, value in check_valid.items():
        if value == 0:
            
            # truong hop bi thieu 1 super node thi gan theo chien luoc CI vao solution hien tai
            node_list = new_PDG_map[key]
            
            if node_list:
                bestInsertPos, bestInsertVehicle = cheapest_insertion_for_block(node_list, parent1.id_to_vehicle, child_vehicleid_to_plan, parent1.route_map)
            
            if bestInsertVehicle is None:
                last_stop_reason = 'no-insertion-position'
                print(f"[new_crossver2] reinsert abandon block | insertion failed -> stopping", file=sys.stderr)
                break
            target_route = child_vehicleid_to_plan[bestInsertVehicle]
            target_route[bestInsertPos: bestInsertPos] = node_list
    
    #print (time.time() - begin)
    return Chromosome(child_vehicleid_to_plan , parent1.route_map , parent1.id_to_vehicle)


def cheapest_insertion_for_block(node_block: List[Node],
                                id_to_vehicle: Dict[str, Vehicle],
                                vehicleid_to_plan: Dict[str, list[Node]],
                                route_map: Dict[tuple, tuple],
                                selected_vehicle: str = None):
    """Append-at-end heuristic with cost evaluation, optimized to avoid deep copies.

    We override only the target vehicle's route in the shared mapping temporarily when
    calling cost_of_a_route, then restore it. This avoids copying the whole plan map.
    """
    minCost = math.inf
    bestInsertPos = 0
    bestInsertVehicleID: Optional[str] = None

    for vehicleID, vehicle in id_to_vehicle.items():
        if selected_vehicle is not None and vehicleID != selected_vehicle:
            continue

        vehicle_plan = vehicleid_to_plan.get(vehicleID) or []
        tempRouteNodeList = vehicle_plan + node_block  # append at end
        def _carrying_list2(v: Vehicle) -> List[OrderItem]:
            ci = getattr(v, 'carrying_items', None)
            try:
                ci_copy = copy.deepcopy(ci)
            except Exception:
                ci_copy = ci
            try:
                items_top_first: List[OrderItem] = []
                while ci_copy is not None and hasattr(ci_copy, 'is_empty') and not ci_copy.is_empty():
                    items_top_first.append(ci_copy.pop())
                return list(reversed(items_top_first)) if items_top_first else (list(ci_copy) if isinstance(ci_copy, list) else [])
            except Exception:
                try:
                    return list(ci_copy) if ci_copy is not None else []
                except Exception:
                    return []
        carrying_items = _carrying_list2(vehicle)
        if not isFeasible(tempRouteNodeList, carrying_items, vehicle.board_capacity):
            continue
        # Temporarily override this vehicle's route
        tmp_cost = cost_of_a_route(tempRouteNodeList, vehicle, id_to_vehicle, route_map, vehicleid_to_plan)
        
        if tmp_cost < minCost:
            minCost = tmp_cost
            bestInsertPos = len(vehicle_plan)
            bestInsertVehicleID = vehicleID

    return bestInsertPos, bestInsertVehicleID

def fix_duppication_nodes (inidividual : Chromosome , PDG_map: Dict[str , List[Node]] ):
    # Cac super node
    new_PDG_map : Dict[str , List[Node]] = {}
    for key , value in PDG_map.items():
        if len(value )!= 2: print("ERROR 1 _ fix ")
        first_node = value[0]
        if first_node.pickup_item_list and first_node.delivery_item_list:
            print ("ERROR 2 _ fix")
        if first_node.pickup_item_list:
            key = f'{len(first_node.pickup_item_list)}_{first_node.pickup_item_list[0].id}'
        elif first_node.delivery_item_list:
            key = f'{len(first_node.delivery_item_list)}_{first_node.delivery_item_list[-1].id}'
        
        
        new_PDG_map[key] = value
        
    check_valid : Dict[str , int]= {key : 0 for key in new_PDG_map.keys()}
    
    for vID, plan in inidividual.solution.items():
        veh = inidividual.id_to_vehicle[vID]
        carry = veh.carrying_items
        if plan:
            if isFeasible(plan ,carry ,veh.board_capacity) == False:
                print("Solution trien vao bi loi !!!!")
    
    # kiểm tra lại lời giải con và xử lý các node thừa hoặc thiếu
    for vehicleID in inidividual.id_to_vehicle.keys():
        redundant = []
        del_index = []
        # Duyệt ngược danh sách để tìm và xóa nút thừa    
        for i in range(len(inidividual.solution[vehicleID]) - 1, -1, -1):  
            node = inidividual.solution[vehicleID][i]
            
            if node.pickup_item_list:
                if redundant and node.pickup_item_list[0].id == redundant[-1]:
                    redundant.pop()  # Loại bỏ phần tử tương ứng trong danh sách `redundant`
                    del_index.append(i)
            else:
                key = f'{len(node.delivery_item_list)}_{node.delivery_item_list[-1].id}'
                
                if key in new_PDG_map:
                    check_valid[key] += 1
                    
                    # nếu tìm được một super node thừa
                    if check_valid[key] > 1:
                        first_itemID_of_redundant_supernode = key.split('_')[-1]
                        redundant.append(first_itemID_of_redundant_supernode)
                        #print(f"Redundant nodes: {redundant}" , file= sys.stderr)
                        # Xóa node giao của super node thừa
                        del_index.append(i)
                        #print('Đã xóa 1 super node thừa' , file= sys.stderr)
        for i in del_index:
            inidividual.solution[vehicleID].pop(i)
    
    
    # Kiem tra lai và thêm các node còn thiếu vào con   
    for key, value in check_valid.items():
        if value == 0:
            
            # truong hop bi thieu 1 super node thi gan theo chien luoc CI vao solution hien tai
            node_list = new_PDG_map[key]
            
            if node_list:
                bestInsertPos, bestInsertVehicle = cheapest_insertion_for_block(node_list, inidividual.id_to_vehicle, inidividual.solution, inidividual.route_map)
                
            if bestInsertVehicle is None:
                last_stop_reason = 'no-insertion-position'
                print(f"[new_crossver2] reinsert abandon block | insertion failed -> stopping", file=sys.stderr)
                break
            target_route = inidividual.solution[bestInsertVehicle]
            target_route[bestInsertPos: bestInsertPos] = node_list



# ================== Block map update with overlap removal & optional salvage (LIFO assumption) ==================
# Thay vì lưu toàn bộ item id, chỉ lưu chữ ký nhóm pickup-delivery: "{count}_{anchor_item_id}"
# anchor_item_id: pickup -> first item id; delivery -> last item id (theo quy ước new_PDG_map)
def update_blockmap(blockmap1 : Dict[str, List[Node]] , blockmap2 : Dict[str , List[Node]] , used_block_nodes: List[Node] , used_signatures : set[str]):
    """Cập nhật blockmap sau khi chèn một block.

    Tối ưu hiệu năng: chỉ đánh dấu các nhóm PD bằng chữ ký (len_pick_list + '_' + first_pick_id)
    tương thích với key của new_PDG_map. Delivery node có chữ ký (len_delivery_list + '_' + last_delivery_id) và
    giả định first pickup id == last delivery id cho cùng nhóm -> hai chữ ký trùng nhau.

    Các bước:
    1. Thu thập chữ ký pickup trong block vừa dùng -> thêm vào used_signatures.
    2. Với mỗi block còn lại: nếu giao chữ ký mới -> tách salvage (trái/phải) bỏ phần giao.
    3. Chỉ giữ segment hợp lệ (balanced LIFO, số nút chẵn >=2, không chứa chữ ký đã dùng).
    4. Xóa block gốc trùng/giao; thêm các segment mới (tối đa 2).
    5. Dọn dẹp block nào vẫn chứa chữ ký đã dùng.
    """
    if not used_block_nodes:
        return
    
    def pickup_signature(nd: Node) -> Optional[str]:
        if nd.pickup_item_list:
            return f"{len(nd.pickup_item_list)}_{nd.pickup_item_list[0].id}"
        return None

    def delivery_signature(nd: Node) -> Optional[str]:
        if nd.delivery_item_list:
            return f"{len(nd.delivery_item_list)}_{nd.delivery_item_list[-1].id}"
        return None

    # Tạo tập chữ ký mới từ block vừa dùng (dùng pickup đủ, nhưng thêm delivery để chắc chắn)
    new_used_set : set[str] = set()
    for nd in used_block_nodes:
        sig_p = pickup_signature(nd)
        if sig_p: new_used_set.add(sig_p)
        sig_d = delivery_signature(nd)
        if sig_d: new_used_set.add(sig_d)
    used_signatures.update(new_used_set)

    # Kiểm tra LIFO balanced nhanh dựa trên chữ ký (stack chữ ký pickup, delivery phải trùng)
    def is_lifo_balanced(nodes: List[Node]) -> bool:
        stack : List[str] = []
        for nd in nodes:
            sig_p = pickup_signature(nd)
            if sig_p:
                stack.append(sig_p)
            sig_d = delivery_signature(nd)
            if sig_d:
                if not stack or stack[-1] != sig_d:
                    return False
                stack.pop()
        return len(stack) == 0

    salvage_min_pairs = 1           # keep residual only if at least this many pickup-delivery pairs
    salvage_min_ratio = 0.0         # set >0 (e.g. 0.5) to enforce size ratio vs original, 0 disables
    max_new_segments_per_block = 2  # with LIFO we expect at most 2

    def process_blockmap(bmap: Dict[str, List[Node]]):
        if not bmap:
            return
        to_delete : List[str] = []
        to_add : List[Tuple[str, List[Node]]] = []
        # iterate over snapshot because we will modify after
        
        for key, nodes in list(bmap.items()):
            if not nodes:
                to_delete.append(key)
                continue
            # Thu thập chữ ký pickup của block
            block_pick_sigs : List[str] = []
            block_all_sigs : set[str] = set()
            for nd in nodes:
                sp = pickup_signature(nd)
                if sp:
                    block_pick_sigs.append(sp)
                    block_all_sigs.add(sp)
                sd = delivery_signature(nd)
                if sd:
                    block_all_sigs.add(sd)

            # Nếu không giao với chữ ký mới nhưng đã dùng trước đó -> xóa; nếu không thì giữ nguyên
            if block_all_sigs.isdisjoint(new_used_set):
                if not block_all_sigs.isdisjoint(used_signatures - new_used_set):
                    to_delete.append(key)
                continue

            # If this block is exactly the inserted block (same length & node identity/order) -> remove directly
            if len(nodes) == len(used_block_nodes) and all(a is b for a,b in zip(nodes, used_block_nodes)):
                to_delete.append(key)
                continue
            
            # Attempt salvage: split into contiguous segments whose nodes do NOT contain any newly used item ids
            segments : List[List[Node]] = []
            current : List[Node] = []
            def node_has_new_sig(nd: Node) -> bool:
                sp = pickup_signature(nd)
                if sp and sp in new_used_set: return True
                sd = delivery_signature(nd)
                if sd and sd in new_used_set: return True
                return False
            for nd in nodes:
                if node_has_new_sig(nd):
                    if current:
                        segments.append(current)
                        current = []
                else:
                    current.append(nd)
            if current:
                segments.append(current)

            # Evaluate & keep valid segments
            added_segments = 0
            for idx, seg in enumerate(segments):
                if added_segments >= max_new_segments_per_block:
                    break
                if len(seg) < 2 or len(seg) % 2 != 0:
                    continue
                if not is_lifo_balanced(seg):
                    continue
                # Tính số cặp = số pickup signature trong segment
                seg_pick_sigs = []
                seg_all_sigs : set[str] = set()
                for nd in seg:
                    sp = pickup_signature(nd)
                    if sp:
                        seg_pick_sigs.append(sp)
                        seg_all_sigs.add(sp)
                    sd = delivery_signature(nd)
                    if sd:
                        seg_all_sigs.add(sd)
                # Loại nếu có chữ ký đã dùng
                if not seg_all_sigs.isdisjoint(used_signatures):
                    continue
                pair_count = len(seg_pick_sigs)
                pair_count = len(seg) // 2
                if pair_count < salvage_min_pairs:
                    continue
                if salvage_min_ratio > 0 and (len(seg)/len(nodes)) < salvage_min_ratio:
                    continue
                # create new key
                new_key = f"{key}|r{idx}"
                # avoid key collision
                suffix = 0
                base_new_key = new_key
                while new_key in bmap or any(k == new_key for k,_ in to_add):
                    suffix += 1
                    new_key = f"{base_new_key}_{suffix}"
                
                to_add.append((new_key, seg))
                added_segments += 1

            # original block removed regardless once processed (overlapped)
            to_delete.append(key)

        # apply deletions
        for k in to_delete:
            bmap.pop(k, None)
        # add salvage blocks
        for k, seg in to_add:
            bmap[k] = seg

        # final safety: remove any block accidentally containing used signatures
        purge_keys = []
        for k, nodes in bmap.items():
            has_used = False
            for nd in nodes:
                sp = pickup_signature(nd)
                if sp and sp in used_signatures:
                    has_used = True
                    break
                sd = delivery_signature(nd)
                if sd and sd in used_signatures:
                    has_used = True
                    break
            if has_used:
                purge_keys.append(k)
        for k in purge_keys:
            bmap.pop(k, None)

    process_blockmap(blockmap1)
    process_blockmap(blockmap2)


# ================== Simple block map update (NO salvage) ==================
def update_blockmap_drop_overlap(blockmap1: Dict[str, List[Node]],
                                 blockmap2: Dict[str, List[Node]],
                                 used_block_nodes: List[Node],
                                 used_signatures: set[str]):
    """Cập nhật blockmap nhưng KHÔNG tái sử dụng (salvage) phần còn lại của các block overlap.

    Khác với `update_blockmap` (có salvage segment), hàm này chỉ đơn giản:
    1. Thu thập chữ ký (signature) của toàn bộ pickup & delivery trong block vừa dùng.
    2. Thêm các chữ ký đó vào `used_signatures` (tập toàn cục các nhóm đã chọn).
    3. Xóa mọi block chứa bất kỳ chữ ký nào đã dùng (bao gồm mới và cũ).
       => Loại bỏ triệt để overlap, không giữ lại phần còn lại để tránh rủi ro nhiễu cấu trúc.

    Ưu điểm:
    - Nhanh, đơn giản, tránh tạo nhiều block nhỏ gây phân mảnh.
    - Hữu ích nếu heuristic salvage làm giảm chất lượng hoặc gây quá nhiều block nhỏ.

    Nhược điểm:
    - Có thể bỏ lỡ cơ hội tái sử dụng phần “sạch” còn lại của block lớn.

    Tham số:
    - blockmap1, blockmap2: hai dict block của 2 parent.
    - used_block_nodes: list node của block vừa chèn (có thể None / rỗng nếu vòng đầu tiên).
    - used_signatures: set lưu chữ ký đã chọn qua các vòng trước (sẽ được cập nhật tại chỗ).
    """
    if not used_block_nodes:
        return

    def pickup_signature(nd: Node) -> Optional[str]:
        if nd.pickup_item_list:
            return f"{len(nd.pickup_item_list)}_{nd.pickup_item_list[0].id}"
        return None

    def delivery_signature(nd: Node) -> Optional[str]:
        if nd.delivery_item_list:
            return f"{len(nd.delivery_item_list)}_{nd.delivery_item_list[-1].id}"
        return None

    # 1. Thu thập chữ ký mới
    new_used_set: set[str] = set()
    for nd in used_block_nodes:
        sp = pickup_signature(nd)
        if sp:
            new_used_set.add(sp)
        sd = delivery_signature(nd)
        if sd:
            new_used_set.add(sd)
    used_signatures.update(new_used_set)

    def purge(bmap: Dict[str, List[Node]]):
        if not bmap:
            return
        to_delete: List[str] = []
        for key, nodes in bmap.items():
            if not nodes:
                to_delete.append(key)
                continue
            remove_block = False
            if len(nodes) == len(used_block_nodes) and all(a is b for a, b in zip(nodes, used_block_nodes)):
                # Chính là block vừa dùng
                remove_block = True
            else:
                for nd in nodes:
                    sp = pickup_signature(nd)
                    if sp and sp in used_signatures:
                        remove_block = True
                        break
                    sd = delivery_signature(nd)
                    if sd and sd in used_signatures:
                        remove_block = True
                        break
            if remove_block:
                to_delete.append(key)
        for k in to_delete:
            bmap.pop(k, None)

    purge(blockmap1)
    purge(blockmap2)



# ================= Helper functions for validation ==================
def pickup_signature_of(nd: Node) -> Optional[str]:
    if nd and nd.pickup_item_list:
        return f"{len(nd.pickup_item_list)}_{nd.pickup_item_list[0].id}"
    return None

def delivery_signature_of(nd: Node) -> Optional[str]:
    if nd and nd.delivery_item_list:
        return f"{len(nd.delivery_item_list)}_{nd.delivery_item_list[-1].id}"
    return None

def extract_pickup_signatures(nodes: List[Node]) -> set[str]:
    sigs = set()
    for nd in nodes:
        ps = pickup_signature_of(nd)
        if ps:
            sigs.add(ps)
    return sigs


def build_block_vehicle_map(blockmap: Dict[str, List[Node]], node2veh: Dict[int, str]) -> Dict[str, Optional[str]]:
    res: Dict[str, Optional[str]] = {}
    for k, nodes in (blockmap or {}).items():
        vid: Optional[str] = None
        # Pick the first node that matches a vehicle; blocks should be contiguous from one route
        for nd in (nodes or []):
            vid = node2veh.get(id(nd))
            if vid is not None:
                break
        res[k] = vid
    return res

# Map each node (by identity) to its vehicle in each parent, then map blocks -> source vehicle
def build_node_to_vehicle(solution: Dict[str, List[Node]]) -> Dict[int, str]:
    m: Dict[int, str] = {}
    for vid, route in (solution or {}).items():
        if not route:
            continue
        for nd in route:
            m[id(nd)] = vid
    return m

# Hàm tính chữ ký block
def block_signatures(nodes: List[Node]) -> set[str]:
    sigs: set[str] = set()
    for nd in nodes:
        if nd.pickup_item_list:
            sigs.add(f"{len(nd.pickup_item_list)}_{nd.pickup_item_list[0].id}")
        if nd.delivery_item_list:
            sigs.add(f"{len(nd.delivery_item_list)}_{nd.delivery_item_list[-1].id}")
    return sigs
            

PREFILTER_K = 1200

def find_best_block(blockmap1: Dict[str, List[Node]],
                    blockmap2: Dict[str, List[Node]],
                    blockscore1: Dict[str, Tuple[float, float, float]],
                    blockscore2: Dict[str, Tuple[float, float, float]]):
    """Chọn block tốt nhất với skyline O(B log B) thay vì O(B^2):
    1) Hợp nhất ứng viên từ cả hai parent: (key, nodes, avg_dis, avg_time, avg_demand).
    2) Lọc non-dominated bằng thuật toán skyline 2D (dis, time): sort theo dis tăng dần, giữ các điểm có time < best_time_so_far.
    3) Trong front, ưu tiên demand lớn (giữ hiệu quả) rồi tie-break bằng (dis+time) nhỏ.
    """
    candidates = []  # (key, nodes, avg_dis, avg_time, avg_demand)

    # Gom ứng viên từ parent 1
    for k, nodes in (blockmap1 or {}).items():
        if not nodes:
            continue
        score = blockscore1.get(k)
        if not score:
            continue
        avg_dis, avg_time, avg_demand = score
        if math.isinf(avg_dis) or math.isinf(avg_time) or math.isinf(avg_demand):
            continue
        candidates.append((k, nodes, avg_dis, avg_time, avg_demand))
    # Gom ứng viên từ parent 2
    for k, nodes in (blockmap2 or {}).items():
        if not nodes:
            continue
        score = blockscore2.get(k)
        if not score:
            continue
        avg_dis, avg_time, avg_demand = score
        if math.isinf(avg_dis) or math.isinf(avg_time) or math.isinf(avg_demand):
            continue
        candidates.append((k, nodes, avg_dis, avg_time, avg_demand))

    if not candidates:
        return None

    # Prefilter top-K by demand desc then (dis+time) asc to reduce set size
    if len(candidates) > PREFILTER_K:
        candidates.sort(key=lambda x: (-x[4], (x[2] + x[3])))
        candidates = candidates[:PREFILTER_K]

    # Skyline: O(B log B)
    candidates.sort(key=lambda x: (x[2], x[3]))  # sort by distance asc, time asc
    front = []
    best_time = math.inf
    for item in candidates:
        _, _, dis, tim, _ = item
        # Since sorted by (dis asc, time asc), any item with time < best_time is non-dominated
        if tim < best_time:
            front.append(item)
            best_time = tim

    if not front:
        return None

    # Ưu tiên demand lớn, tie-break theo (dis+time) nhỏ
    front.sort(key=lambda x: (-x[4], (x[2] + x[3])))
    best = front[0]
    return best[1]



def extract_block_from_solution (vehicleid_to_plan : Dict[str , List[Node]] , id_to_vehicle : Dict[str , Vehicle]) -> Dict[str , List[Node]]:
    dis_order_super_node , _ = get_UnongoingSuperNode(vehicleid_to_plan , id_to_vehicle)
    
    ls_node_pair_num = len(dis_order_super_node)
    if ls_node_pair_num == 0:
        return False
    
    vehicleID = None
    block_map : Dict[str , List[Node]] = {}
    for idx , pdg in dis_order_super_node.items():
        pickup_node : Node = None
        delivery_node : Node = None
        node_list :List[Node] = []
        posI :int =0 ; posJ : int= 0
        dNum : int= len(pdg) // 2
        index :int= 0
        if pdg:
            for v_and_pos_str, node in pdg.items():
                if index % 2 == 0:
                    vehicleID = v_and_pos_str.split(",")[0]
                    posI = int(v_and_pos_str.split(",")[1])
                    pickup_node = node
                    node_list.insert(0, pickup_node)
                    index += 1
                else:
                    posJ = int(v_and_pos_str.split(",")[1])
                    delivery_node = node
                    node_list.append(delivery_node)
                    index += 1
                    posJ = posJ - dNum + 1
            
            vehicle_node_route : List[Node] = vehicleid_to_plan.get(vehicleID , [])
            
            for i in range(posI + dNum , posJ):
                node_list.insert(i - posI , vehicle_node_route[i])

            k : str = f"{vehicleID},{posI}+{posJ + dNum - 1}"    
            block_map[k] = node_list
    
    return block_map

# chấm điểm cho dựa trên cấu trúc của block
def block_scoring_func(blockmap: Dict[str, List[Node]],
                      vehicleid_to_plan1: Dict[str, List[Node]],
                      vehicleid_to_plan2: Dict[str, List[Node]],
                      route_map: Dict[Tuple[str, str], Tuple[float, float]]) -> Dict[str, Tuple[float, float, float]]:
    """Tính điểm cho từng block và trả về 3 chỉ số:
    (avg_distance, avg_time, avg_demand)

    - avg_distance: trung bình quãng đường giữa các cặp node liên tiếp.
    - avg_time: trung bình thời gian di chuyển giữa các cặp node liên tiếp.
    - avg_demand: trung bình demand của tất cả item (pickup + delivery) xuất hiện trong block.

    Các block không hợp lệ (số node < 2 hoặc số node lẻ) trả về (inf, inf, inf).
    Nếu không có cạnh hợp lệ (edge_count == 0) thì avg_distance = avg_time = 0.0 nhưng vẫn tính avg_demand.
    """
    block_scores: Dict[str, Tuple[float, float, float]] = {}

    if not blockmap:
        return block_scores

    for block_key, node_list in blockmap.items():
        if not node_list or len(node_list) % 2 != 0 or len(node_list) < 2:
            block_scores[block_key] = (math.inf, math.inf, math.inf)
            continue

        total_distance = 0.0
        total_time = 0.0
        edge_count = 0
        total_demand = 0.0
        item_count = 0

        # Tính demand trung bình trước (không phụ thuộc vào cạnh hợp lệ)
        for nd in node_list:
            if nd.pickup_item_list:
                for it in nd.pickup_item_list:
                    if hasattr(it, 'demand'):
                        total_demand += it.demand
                        item_count += 1
            if nd.delivery_item_list:
                for it in nd.delivery_item_list:
                    if hasattr(it, 'demand'):
                        total_demand += it.demand
                        item_count += 1
        avg_demand = (total_demand / item_count) if item_count > 0 else 0.0

        # Duyệt các cặp node liên tiếp để lấy distance/time
        for i in range(len(node_list) - 1):
            n1 = node_list[i]
            n2 = node_list[i + 1]
            if not n1 or not n2:
                continue
            key = (n1.id, n2.id)
            dis_time = route_map.get(key)
            if dis_time is None:
                dis_time = route_map.get((n2.id, n1.id))
            if dis_time is None:
                continue
            try:
                distance_val = float(dis_time[0])
                time_val = float(dis_time[1])
            except (ValueError, TypeError, IndexError):
                continue
            total_distance += distance_val
            total_time += time_val
            edge_count += 1

        if edge_count == 0:
            avg_distance = 0.0
            avg_time = 0.0
        else:
            avg_distance = total_distance / edge_count
            avg_time = total_time / edge_count

        block_scores[block_key] = (avg_distance, avg_time, avg_demand)

    return block_scores

# Incremental scoring with cache to avoid recomputing unchanged blocks each iteration
def compute_block_scores_incremental(blockmap: Optional[Dict[str, List[Node]]],
                                     route_map: Dict[Tuple[str, str], Tuple[float, float]],
                                     score_cache: Dict[str, Tuple[float, float, float]]) -> Dict[str, Tuple[float, float, float]]:
    """Compute block scores using a cache.

    - blockmap: key -> node_list
    - route_map: distance/time matrix
    - score_cache: persisted across iterations, stores key -> (avg_distance, avg_time, avg_demand)

    Returns a dict of scores for keys present in blockmap. For any key not in cache, compute and store it.
    This mirrors block_scoring_func per-block logic but avoids recomputing existing keys.
    """
    if not blockmap:
        return {}

    result: Dict[str, Tuple[float, float, float]] = {}

    for block_key, node_list in blockmap.items():
        if block_key in score_cache:
            result[block_key] = score_cache[block_key]
            continue

        # Compute score for this block (same logic as block_scoring_func for a single block)
        if not node_list or len(node_list) % 2 != 0 or len(node_list) < 2:
            score_cache[block_key] = (math.inf, math.inf, math.inf)
            result[block_key] = score_cache[block_key]
            continue

        total_distance = 0.0
        total_time = 0.0
        edge_count = 0
        total_demand = 0.0
        item_count = 0

        for nd in node_list:
            if nd.pickup_item_list:
                for it in nd.pickup_item_list:
                    if hasattr(it, 'demand'):
                        total_demand += it.demand
                        item_count += 1
            if nd.delivery_item_list:
                for it in nd.delivery_item_list:
                    if hasattr(it, 'demand'):
                        total_demand += it.demand
                        item_count += 1
        avg_demand = (total_demand / item_count) if item_count > 0 else 0.0

        for i in range(len(node_list) - 1):
            n1 = node_list[i]
            n2 = node_list[i + 1]
            if not n1 or not n2:
                continue
            key = (n1.id, n2.id)
            dis_time = route_map.get(key)
            if dis_time is None:
                dis_time = route_map.get((n2.id, n1.id))
            if dis_time is None:
                continue
            try:
                distance_val = float(dis_time[0])
                time_val = float(dis_time[1])
            except (ValueError, TypeError, IndexError):
                continue
            total_distance += distance_val
            total_time += time_val
            edge_count += 1

        if edge_count == 0:
            avg_distance = 0.0
            avg_time = 0.0
        else:
            avg_distance = total_distance / edge_count
            avg_time = total_time / edge_count

        score = (avg_distance, avg_time, avg_demand)
        score_cache[block_key] = score
        result[block_key] = score

    return result

def cheapest_insertion_for_block(node_block: List[Node],
                                id_to_vehicle: Dict[str, Vehicle],
                                vehicleid_to_plan: Dict[str, list[Node]],
                                route_map: Dict[tuple, tuple],
                                selected_vehicle: str = None):
    """Append-at-end heuristic with cost evaluation, optimized to avoid deep copies.

    We override only the target vehicle's route in the shared mapping temporarily when
    calling cost_of_a_route, then restore it. This avoids copying the whole plan map.
    """
    minCost = math.inf
    bestInsertPos = 0
    bestInsertVehicleID: Optional[str] = None

    for vehicleID, vehicle in id_to_vehicle.items():
        if selected_vehicle is not None and vehicleID != selected_vehicle:
            continue

        vehicle_plan = vehicleid_to_plan.get(vehicleID) or []
        tempRouteNodeList = vehicle_plan + node_block  # append at end
        def _carrying_list2(v: Vehicle) -> List[OrderItem]:
            ci = getattr(v, 'carrying_items', None)
            try:
                ci_copy = copy.deepcopy(ci)
            except Exception:
                ci_copy = ci
            try:
                items_top_first: List[OrderItem] = []
                while ci_copy is not None and hasattr(ci_copy, 'is_empty') and not ci_copy.is_empty():
                    items_top_first.append(ci_copy.pop())
                return list(reversed(items_top_first)) if items_top_first else (list(ci_copy) if isinstance(ci_copy, list) else [])
            except Exception:
                try:
                    return list(ci_copy) if ci_copy is not None else []
                except Exception:
                    return []
        carrying_items = _carrying_list2(vehicle)
        if not isFeasible(tempRouteNodeList, carrying_items, vehicle.board_capacity):
            continue
        # Temporarily override this vehicle's route
        tmp_cost = cost_of_a_route(tempRouteNodeList, vehicle, id_to_vehicle, route_map, vehicleid_to_plan)
        
        if tmp_cost < minCost:
            minCost = tmp_cost
            bestInsertPos = len(vehicle_plan)
            bestInsertVehicleID = vehicleID

    return bestInsertPos, bestInsertVehicleID