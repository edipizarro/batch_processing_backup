import os
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["CUDA_VISIBLE_DEVICES"] = ""
import sys

from joblib import Parallel, delayed
import pandas as pd

from alerce_classifiers.mbappe.mapper import MbappeMapper
from alerce_classifiers.mbappe.model import MbappeClassifier


def predict_over_batch(batch_filename: str, output_filename: str):
    classifier = MbappeClassifier(
        model_path=os.getenv("TEST_MBAPPE_MODEL_PATH"),
        metadata_quantiles_path=os.getenv("TEST_MBAPPE_METADATA_QUANTILES_PATH"),
        features_quantiles_path=os.getenv("TEST_MBAPPE_FEATURES_QUANTILES_PATH"),
        mapper=MbappeMapper(),
    )

    astro_object_list = pd.read_pickle(batch_filename)
    prediction_df = classifier.predict(astro_object_list)

    prediction_df["shorten"] = shorten
    prediction_df.to_parquet(output_filename)


if __name__ == "__main__":
    dir_name = sys.argv[1]
    file_list = os.listdir(dir_name)
    output_dir = os.path.join(dir_name, 'probabilities_mbappe')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_list = [filename for filename in file_list if "astro_objects_batch" in filename]
    file_list = sorted(file_list)

    tasks = []
    for batch_filename in file_list:
        full_filename = os.path.join(dir_name, batch_filename)
        output_filename = os.path.join(output_dir, 'probs_squidward_' + batch_filename.replace("pkl", "parquet"))
        tasks.append(
            delayed(predict_over_batch)(full_filename, output_filename)
        )

    Parallel(n_jobs=50, verbose=11, backend="loky")(tasks)
