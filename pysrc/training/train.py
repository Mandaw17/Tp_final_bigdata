from sklearn.ensemble import RandomForestClassifier
import pandas as pd
from sklearn.model_selection import train_test_split


def train_model(input_path: str, output_path: str):
    labelised_data = pd.read_csv(input_path)

    # set training data
    X = labelised_data.drop(columns=["Label"])
    y = labelised_data["Label"]

    # set test data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    # initialize model
    model = RandomForestClassifier()

    # train model
    model.fit(X_train, y_train)

    