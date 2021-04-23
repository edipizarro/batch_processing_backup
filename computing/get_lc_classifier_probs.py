import dask.dataframe as dd
import click
import logging
import os

import pandas as pd
from lc_classifier.classifier.models import HierarchicalRandomForest


def compute_features(features_dir, xmatches_dir):
    features = dd.read_parquet(os.path.join(features_dir, "*.parquet")).compute()
    logging.info(f"Features opened ({features.shape[0]} objects with features)")

    if os.path.isdir(xmatches_dir):
        xmatches = dd.read_parquet(os.path.join(xmatches_dir, "*.parquet")).compute()
    else:
        xmatches = pd.read_parquet(xmatches_dir)
    logging.info(f"Xmatches opened ({xmatches.shape[0]} objects with xmatch)")

    logging.info("Joining data")
    features = features.join(xmatches.set_index("objectId_2"), on="oid")

    logging.info("Getting new features from xmatch")
    features["Period_fit"] = features["PPE"]
    features["W1-W2"] = features["w1mpro"] - features["w2mpro"]
    features["W2-W3"] = features["w2mpro"] - features["w3mpro"]
    features["g-W2"] = features["mean_mag_1"] - features["w2mpro"]
    features["g-W3"] = features["mean_mag_2"] - features["w3mpro"]
    features["r-W2"] = features["mean_mag_2"] - features["w2mpro"]
    features["r-W3"] = features["mean_mag_2"] - features["w3mpro"]

    features.set_index("oid", inplace=True)
    return features


def compute_probabilities(features, output_path=None):
    model = HierarchicalRandomForest({})
    model.download_model()
    model.load_model(model.MODEL_PICKLE_PATH)

    logging.info("Getting probabilities")
    probabilities = model.predict_in_pipeline(features)
    top = probabilities["hierarchical"]["top"]
    stochastic = probabilities["hierarchical"]["children"]["Stochastic"]
    periodic = probabilities["hierarchical"]["children"]["Periodic"]
    transient = probabilities["hierarchical"]["children"]["Transient"]
    leafs = probabilities["probabilities"]

    if output_path:
        top.to_parquet(os.path.join(output_path, "lc_classifier_top.parquet"))
        stochastic.to_parquet(os.path.join(output_path, "lc_classifier_stochastic.parquet"))
        periodic.to_parquet(os.path.join(output_path, "lc_classifier_periodic.parquet"))
        transient.to_parquet(os.path.join(output_path, "lc_classifier_transient.parquet"))
        leafs.to_parquet(os.path.join(output_path, "lc_classifier.parquet"))

    else:

        top.columns = [f"top_{x}" for x in top.columns]
        stochastic.columns = [f"stochastic_{x}" for x in stochastic.columns]
        periodic.columns = [f"periodic_{x}" for x in periodic.columns]
        transient.columns = [f"transient_{x}" for x in transient.columns]
        leafs.columns = [f"leaf_{x}" for x in leafs.columns]
        return pd.concat([top, stochastic, periodic, transient, leafs], axis=1)


@click.command()
@click.argument("features_dir", type=str)
@click.argument("xmatches_dir", type=str)
@click.argument("output_path", type=str)
def get_probs(features_dir, xmatches_dir, output_path):
    logging.basicConfig(level="INFO",
                        format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    features = compute_features(features_dir, xmatches_dir)

    if not os.path.isdir(output_path):
        os.mkdir(output_path)
    logging.info(f"Writing probabilities in {output_path}")

    compute_probabilities(features, output_path=output_path)
    return


if __name__ == "__main__":
    get_probs()
