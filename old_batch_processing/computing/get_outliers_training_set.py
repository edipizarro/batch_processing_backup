import click
import logging
import pandas as pd

from get_lc_classifier_probs import compute_probabilities, compute_features
from get_flags import compute_flags


@click.command()
@click.argument("features_dir", type=str)
@click.argument("xmatches_dir", type=str)
@click.argument("objects_dir", type=str)
@click.argument("magstats_dir", type=str)
@click.argument("output_path", type=str)
def get_training_set(features_dir, xmatches_dir, objects_dir, magstats_dir, output_path):
    logging.basicConfig(level="INFO",
                        format='%(asctime)s %(levelname)s %(name)s.%(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    features = compute_features(features_dir, xmatches_dir)
    probabilities = compute_probabilities(features)
    features = pd.concat([features, probabilities], axis=1)
    del probabilities

    flags = compute_flags(objects_dir, magstats_dir)
    flags = flags[flags["objectId"].isin(features.index)]
    flags.set_index("objectId", inplace=True)
    logging.info("Joining flags with features")
    features = features.join(flags, how="left")
    features.to_parquet(output_path)
    logging.info("End")
    return


if __name__ == "__main__":
    get_training_set()
