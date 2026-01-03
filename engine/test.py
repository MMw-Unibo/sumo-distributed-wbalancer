import sumolib

if __name__ == "__main__":
    single = {}
    print("Processing single data...")
    for step in sumolib.xml.parse_fast_nested("dump_single.out", "timestep", ['time'], "vehicle", ["id"], optional=True):
        time = int(float(step[0].time))
        vehicle_id = int(step[1].id)

        if time not in single:
            single[time] = []

        single[time].append(vehicle_id)


    print("Processing partitioned data...")
    part = {}
    for num_part in range(0, 2):
        for step in sumolib.xml.parse_fast_nested("dump_{}.out".format(num_part), "timestep", ['time'], "vehicle", ["id"], optional=True):
            time = int(float(step[0].time))
            vehicle_id = int(step[1].id)

            if time not in part:
                part[time] = []

            part[time].append(vehicle_id)

    # print("Single: {}".format(single))
    # print("Part: {}".format(part))

    print("Comparing data...")
    for timestep in list(single.keys())[:800]:
        if timestep not in part:
            print("Missing timestep: {}".format(timestep))
            continue

        if len(single[timestep]) != len(part[timestep]):
            print("Different number of vehicles at timestep {}: {} vs {}".format(timestep, len(single[timestep]), len(part[timestep])))
            
            if len(part[timestep]) != len(set(part[timestep])):
                print("Duplicated in part")

            # for vehicle_id in single[timestep]:
            #     if vehicle_id not in part[timestep]:
            #         print("Vehicle {} not found in partitioned data at timestep {}".format(vehicle_id, timestep))

            # for vehicle_id in part[timestep]:
            #     if vehicle_id not in single[timestep]:
            #         print("Vehicle {} not found in single data at timestep {}".format(vehicle_id, timestep))
