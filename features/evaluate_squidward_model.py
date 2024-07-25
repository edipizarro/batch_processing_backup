import os
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
import sys

from joblib import Parallel, delayed
import pandas as pd

from alerce_classifiers.classifiers.hierarchical_random_forest import (
    HierarchicalRandomForestClassifier,
)
import wget


url = os.getenv("TEST_SQUIDWARD_MODEL_PATH")
download_path = "/tmp/Squidward"
if not os.path.exists(download_path):
    os.makedirs(download_path)
filename = url.split("/")[-1]
model_dir = os.path.join(download_path, filename)
if not os.path.exists(model_dir):
    wget.download(url, model_dir)


def predict_over_batch(batch_filename: str, output_filename: str):
    classifier = HierarchicalRandomForestClassifier([])
    classifier.load_classifier(model_dir)

    consolidated_features = pd.read_parquet(batch_filename)

    shorten = consolidated_features["shorten"]
    consolidated_features = consolidated_features[
        [c for c in consolidated_features.columns if c != "shorten"]
    ]
    prediction_df = classifier.classify_batch(consolidated_features)
    prediction_df["shorten"] = shorten
    prediction_df.to_parquet(output_filename)


if __name__ == "__main__":
    dir_name = sys.argv[1]
    file_list = os.listdir(dir_name)
    output_dir = os.path.join(dir_name, 'probabilities_squidward_1_0_7')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_list = [filename for filename in file_list if "features_astro_objects_batch" in filename]
    file_list = sorted(file_list)

    tasks = []
    for batch_filename in file_list:
        full_filename = os.path.join(dir_name, batch_filename)
        output_filename = os.path.join(output_dir, batch_filename.replace("features", "probs_squidward"))
        if os.path.exists(output_filename):
            continue
        tasks.append(
            delayed(predict_over_batch)(full_filename, output_filename)
        )

    Parallel(n_jobs=50, verbose=11, backend="loky")(tasks)
