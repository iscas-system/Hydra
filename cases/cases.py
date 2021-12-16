import pandas as pd

if __name__ == "__main__":
    df = pd.read_csv("case_2000.csv", header=0)
    print(df.head)
    for i in range(df.shape[0]):
        name = df.loc[i, "job_name"]
        submit_time = df.loc[i, "norm_job_submit_time"]
        ddl = df.loc[i, "ddl"]

        df.loc[i, "norm_job_submit_time"] = 1
        df.loc[i, "ddl"] -= submit_time - 1
    print(df)

    df.to_csv("case_2000_all.csv")