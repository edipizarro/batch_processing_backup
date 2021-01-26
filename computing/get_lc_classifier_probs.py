import dask.dataframe as dd
import pandas as pd
import click
import os
from lc_classifier.classifier.models import HierarchicalRandomForest


@click.command()
@click.argument("features_dir", type=str)
@click.argument("xmatches_dir", type=str)
@click.argument("output_path", type=str)
def get_probs(features_dir, xmatches_dir, output_path):
    features = dd.read_parquet(os.path.join(features_dir, "*.parquet")).compute()
    xmatches = dd.read_parquet(os.path.join(xmatches_dir, "*.parquet")).compute()

    features = features.join(xmatches.set_index("objectId_2"), on="oid")
    features["Period_fit"] = features["PPE"]

    features["W1-W2"] = features["w1mpro"] - features["w2mpro"]
    features["W2-W3"] = features["w2mpro"] - features["w3mpro"]
    features["g-W2"] = features["mean_mag_1"] - features["w2mpro"]
    features["g-W3"] = features["mean_mag_2"] - features["w3mpro"]
    features["r-W2"] = features["mean_mag_2"] - features["w2mpro"]
    features["r-W3"] = features["mean_mag_2"] - features["w3mpro"]

    features.set_index("oid", inplace=True)
    model = HierarchicalRandomForest({})
    model.download_model()
    model.load_model(model.MODEL_PICKLE_PATH)

    probabilities = model.predict_in_pipeline(features)

    response = pd.concat([
        probabilities["hierarchical"]["top"],
        probabilities["hierarchical"]["children"]["Stochastic"],
        probabilities["hierarchical"]["children"]["Periodic"],
        probabilities["hierarchical"]["children"]["Transient"],
    ], axis=1)

    response.to_parquet(output_path)
    return


if __name__ == "__main__":
    get_probs()
