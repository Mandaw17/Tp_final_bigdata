import pandas as pd

def clean_logs(input_path: str, output_path: str):
    df = pd.read_csv(input_path)

    df = df.dropna()


    df.to_csv(output_path, index=False)