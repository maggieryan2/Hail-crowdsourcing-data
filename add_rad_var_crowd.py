import get_archived_data_JKO as rad_tool #use prepare_gridded_radar_data_from_zip function

import numpy as np
import pandas as pd # type: ignore
import datetime
import os
import sys
import logging

username = "mryan"

# Define the new directory for saving unzipped files
output_directory = "/users/mryan/Code/unzipped_files"

# Ensure the directory exists
if not os.path.exists(output_directory):
    os.makedirs(output_directory)


#List of time steps to compute maximum of radar variables; m15/p15 means previous/next 15 minutes
time_1_list = ['Time_m5', 'Time_m10', 'Time_m15']
time_2_list = ['Time_p5', 'Time_p10', 'Time_p15']

rad_var = sys.argv[1]
date_from = sys.argv[2]
date_to = sys.argv[3]

RADIUS_LIST = [0, 2, 4]

y = np.linspace(255.5, 964.5, 710)*1000;
x = np.linspace(479.5, -159.5,  640)*1000;


# Create grid of x,y coordinates
Y,X = np.meshgrid(y,x)

# Load reports
#raw_data = pd.read_csv('crowd_reports_2015-05-12_2023-06-21.csv').drop(columns=['Unnamed: 0'])

raw_data = pd.read_csv('crowd_reports_%s_%s.csv' % (date_from, date_to)).drop(columns=['Unnamed: 0'])
#raw_data = pd.read_csv('sensor_mod_20230907.csv').drop(columns=['Unnamed: 0'])
#raw_data['Time'] = pd.to_datetime(raw_data['Time'])
raw_data['Time'] = pd.to_datetime(raw_data['Time'], format='%Y-%m-%d %H:%M:%S%z', errors='coerce')

matching = ["x","y", "Time", "Timestamp"] + time_1_list + time_2_list

# If too much data then split per period and change date_from and date_to below
data_sub = raw_data[(raw_data['Time']>= date_from) & (raw_data['Time']<= date_to)][matching].copy()
data_sub = data_sub.sort_values(by=['Time'])

# Function to get maximum of radar variable over present, previous and next n timesteps on full grid

import os
import numpy as np
import datetime


def get_combined_max_radar_grid_3ts(
    product, #type=str, 
    timestamps, #type=list[str],  # Assuming timestamps is a list of strings
    ts, #type= int,                # Assuming ts is an integer
    agg_method = 'max', # agg_method type = str     # Default value for agg_method
    output_directory='/users/mryan/Code/unzipped_files'  # Default output directory
):
    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    temp_date = datetime.datetime.strptime(timestamps[0], "%Y%m%d%H%M%S")
    time_first = datetime.datetime.strptime(timestamps[1], "%Y%m%d%H%M%S")
    time_last = datetime.datetime.strptime(timestamps[2], "%Y%m%d%H%M%S")
    save_name_first = f"{timestamps[0]}_{timestamps[1]}"
    save_name_last = f"{timestamps[0]}_{timestamps[2]}"
    
    print("temp_date", str(temp_date), " time_last: ", str(time_last))
    while temp_date <= time_last:
        try:
            if "grid" in locals():
                grid_new = rad_tool.prepare_gridded_radar_data_from_zip(product=product,timestamp=temp_date.strftime("%Y%m%d%H%M%S"))
                if agg_method == 'max':
                    grid = np.fmax(grid, grid_new)
                elif agg_method == 'sum':
                    grid = np.add(grid, grid_new)
            else:
                grid = rad_tool.prepare_gridded_radar_data_from_zip(
                    product=product,
                    timestamp=temp_date.strftime("%Y%m%d%H%M%S")
                )
                if ts == 1:
                    ##np.save(f"/scratch/{username}/temp/subdaily_npy/{product}_{timestamps[0]}.npy", grid)
                    np.save(f"{output_directory}/{product}_{timestamps[0]}.npy", grid) 
                    print(output_directory)
        
        except (AttributeError, FileNotFoundError, UnicodeDecodeError) as error:
            with open("error_log.txt", "a") as f:
                f.write(f"{temp_date}: {error}\n")
        
        temp_date += datetime.timedelta(minutes=5)

    if "grid" in locals():
        #np.save(f"/scratch/{username}/temp/subdaily_npy/{product}_{save_name_last}.npy", grid)
        np.save(f"{output_directory}/{product}_{save_name_last}.npy", grid) 
        del grid

    temp_date = datetime.datetime.strptime(timestamps[0], "%Y%m%d%H%M%S")

    while temp_date >= time_first:
        try:
            if "grid" in locals():
                grid_new = rad_tool.prepare_gridded_radar_data_from_zip(
                    product=product,
                    timestamp=temp_date.strftime("%Y%m%d%H%M%S")
                )
                if agg_method == 'max':
                    grid = np.fmax(grid, grid_new)
                elif agg_method == 'sum':
                    grid = np.add(grid, grid_new)
            else:
                grid = rad_tool.prepare_gridded_radar_data_from_zip(
                    product=product,
                    timestamp=temp_date.strftime("%Y%m%d%H%M%S")
                )
        
        except (AttributeError, FileNotFoundError, UnicodeDecodeError) as error:
            with open("error_log.txt", "a") as f:
                f.write(f"{temp_date}: {error}\n")

        temp_date -= datetime.timedelta(minutes=5)

    if "grid" in locals():
        #np.save(f"/scratch/{username}/temp/subdaily_npy/{product}_{save_name_first}.npy", grid)
        np.save(f"{output_directory}/{product}_{save_name_first}.npy", grid)
        del grid



# Distance between two grid points
def dist(x1,x2,y1,y2):
    d = np.sqrt((x1-x2)**2 + (y1-y2)**2)
    return d

# Get max of radar variables over radius_list
def add_rad_variables(x, y, timestamp, time_first, time_last, rad_var, radius_list, ts):  
    l_max = {}
    distance = dist(Y,float(x),X,float(y))
    if ts == 1:
         ls_time = ['ts', time_1[5:8], time_2[5:8]]
         ls_dt = [timestamp, time_first, time_last]
    else:
         ls_time = [time_1[5:8], time_2[5:8]]
         ls_dt = [time_first, time_last]

    for dt, txt in zip(ls_dt,ls_time):
        try:  
            if dt == timestamp:
                #file_c = "/scratch/%s/temp/subdaily_npy/%s_%s.npy" % (username, rad_var, timestamp)
                file_c = f"{output_directory}/{rad_var}_{timestamp}.npy" 
                rad_vals = np.load(file_c)
            else:
                #file_c = "/scratch/%s/temp/subdaily_npy/%s_%s_%s.npy" % (username, rad_var, timestamp, dt)
                file_c = f"{output_directory}/{rad_var}_{timestamp}_{dt}.npy"
                rad_vals = np.load(file_c)

            if rad_vals.ndim > 2:
                rad_vals = rad_vals[0,:,:]
        
            for radius in radius_list:
                key_var = 'max{:s}_rad{:d}_{:s}'.format(rad_var, radius, txt)
                if radius == 0:
                    closestpix = np.argmin(distance)
                    l_max.update({'x': x, 'y': y, 'Timestamp': timestamp, time_1: time_first,
                                time_2: time_last, key_var: rad_vals.ravel()[closestpix]})
                else:
                    area = distance <= radius * 1000
                    l_max.update({'x': x, 'y': y, 'Timestamp': timestamp, time_1: time_first,
                                time_2: time_last, key_var: np.nanmax(rad_vals[area])})
        except FileNotFoundError as error:
            f = open("error_log.txt", "a")
            f.write(str(file_c) + ': ' + str(error) + '\n')
            f.close()
            pass
    print(f"Inputs: {x}, {y}, {z}, {v}, {w}, rad_var: {rad_var}, RADIUS_LIST: {RADIUS_LIST}, ts: {ts}")
    print(f"Output: {l_max}")

    try:
        # Attempt to read file
        print(f"Attempting to open file: {file_c}")
        with open(file_c, 'r') as file:
            print(file.readlines()[:5])  # Check the file contents
    except Exception as e:
        print(f"Error reading file: {e}")
    # If valid output is not found, log the situation
    if not valid_output_found:
        print(f"No valid output for inputs: {x}, {y}, {z}, {v}, {w}")
    return l_max

# Identify first iteration
ts = 1

# Main loop which iterates over the timestamps range
# Call get_combined_max_radar_grid_3ts for all timestamps combinations (ls)
# Then call add_rad_variables to get max over radius_list


for time_1,time_2 in zip(time_1_list,time_2_list):
    data_sub[['Timestamp',time_1,time_2]] = data_sub[['Timestamp',time_1,time_2]].astype(str)
    [get_combined_max_radar_grid_3ts(rad_var,x,ts,'max') for x in data_sub[['Timestamp',time_1,time_2]].drop_duplicates().values.tolist()]
    ls = data_sub[['x','y','Timestamp',time_1,time_2]].drop_duplicates()


    l_temp = [add_rad_variables(x,y,z,v,w,rad_var,RADIUS_LIST,ts) for x,y,z,v,w in zip(ls['x'], ls['y'], ls['Timestamp'], ls[time_1], ls[time_2])]    

    l_del = data_sub[['Timestamp',time_1,time_2]].drop_duplicates()

    #Remove temp numpy grid
    for ts_,t1,t2 in zip(l_del['Timestamp'], l_del[time_1], l_del[time_2]):
        #file_t1 = "/scratch/%s/temp/subdaily_npy/%s_%s_%s.npy" % (username, rad_var, ts_, t1)
        file_t1 = f"{output_directory}/{rad_var}_{ts_}_{t1}.npy" 
        file_t2 = f"{output_directory}/{rad_var}_{ts_}_{t2}.npy" 
        #file_t2 = "/scratch/%s/temp/subdaily_npy/%s_%s_%s.npy" % (username, rad_var, ts_, t2)
        if os.path.isfile(file_t1):
             os.remove(file_t1)
        if os.path.isfile(file_t2):
             os.remove(file_t2)
        if ts == 1:
             #file_ts = "/scratch/%s/temp/subdaily_npy/%s_%s.npy" % (username, rad_var, ts_)
             file_ts = f"{output_directory}/{rad_var}_{ts_}.npy" 
             if os.path.isfile(file_ts):
                  os.remove(file_ts)

    ts = 0
    df = pd.DataFrame.from_dict(l_temp)
    data_sub = pd.merge(data_sub, df, how='left', on=['x', 'y', 'Timestamp', time_1, time_2])

data_sub.to_csv('crowd_%s_%s_%s.csv' % (rad_var, date_from, date_to))
