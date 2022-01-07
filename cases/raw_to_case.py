import csv
import pandas as pd
import random
import numpy as np

t4_up_p100 = (1.4, 2)
p100_up_v100 = (1.2, 1.9)
ddl_up_runtime = (1.2, 3)
def get(s):
    return s[0] + (s[1] - s[0]) * random.random()

if __name__ == "__main__":
    df = pd.read_csv("200.csv", header=0)
    df2 = pd.DataFrame(columns=["job_name", "norm_job_submit_time", "ddl", "T4", "P100", "V100"])
    for i in range(df.shape[0]):
        job_name = df.iloc[i]["job_name"]
        submit_time = 0
        t4_time = 0
        p100_time = 0
        v100_time = 0
        gpu_type = df.iloc[i]["gpu_type"]
        runtime = df.iloc[i]["runtime"]
        if gpu_type == "T4":
            t4_time = runtime
            p100_time = t4_time / get(t4_up_p100)
            v100_time = t4_time / get(t4_up_p100) / get(p100_up_v100)
        elif gpu_type == "P100":
            p100_time = runtime
            t4_time = p100_time * get(t4_up_p100)
            v100_time = p100_time / get(p100_up_v100)

        elif gpu_type == "V100" or gpu_type == "V100M32":
            v100_time = runtime
            p100_time = v100_time * get(p100_up_v100)
            t4_time = v100_time * get(p100_up_v100) * get(t4_up_p100)
        df2.loc[i] = [job_name, 0, 0, round(t4_time), round(p100_time), round(v100_time)]
    df2.sort_values("V100", ascending=False)
    for i in range(df2.shape[0]):
        if i < 40:
            ddl = round(runtime * get(ddl_up_runtime))
            print(ddl, runtime)
        else:
            ddl = np.inf
        df2.loc[i]["ddl"] = ddl
        
    print(df2)
    df2.to_csv("case_200_long_task_add_ddl.csv")

    
