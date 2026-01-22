import pandas as pd

def build_features(input_path: str, output_path: str):
    df = pd.read_csv(input_path)

    df = pd.get_dummies(
        df[["Request_Type", "Protocol", "User_Agent"]],
        prefix=["req", "proto", "ua"],
        drop_first=True
    )
    
    df.to_parquet(output_path, index=False)
