import os
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["CUDA_VISIBLE_DEVICES"] = ""
import sys

from joblib import Parallel, delayed
import numpy as np
import pandas as pd

from mbappe_mapper_batch import MbappeMapperBatchProcessing
from alerce_classifiers.mbappe.model import MbappeClassifier


def predict_over_batch(batch_filename: str, output_filename: str):
    from lc_classifier.features.core.base import astro_object_from_dict
    classifier = MbappeClassifier(
        model_path=os.getenv("TEST_MBAPPE_MODEL_PATH"),
        metadata_quantiles_path=os.getenv("TEST_MBAPPE_METADATA_QUANTILES_PATH"),
        features_quantiles_path=os.getenv("TEST_MBAPPE_FEATURES_QUANTILES_PATH"),
        mapper=MbappeMapperBatchProcessing(),
    )

    astro_object_list = pd.read_pickle(batch_filename)
    astro_object_list = [astro_object_from_dict(d) for d in astro_object_list]
    n_aos = len(astro_object_list)
    batch_size = 100
    n_batches = int(np.ceil(n_aos / batch_size))
    prediction_list = []
    for i in range(n_batches):
        prediction_df = classifier.predict(astro_object_list[i*batch_size:(i+1)*batch_size])
        prediction_list.append(prediction_df)

    prediction_df = pd.concat(prediction_list)
    shorten = batch_filename.split('/')[-1].split('.')[0].split('_')[-2]
    prediction_df["shorten"] = shorten
    prediction_df.to_parquet(output_filename)


if __name__ == "__main__":
    # to cache model files
    classifier = MbappeClassifier(
        model_path=os.getenv("TEST_MBAPPE_MODEL_PATH"),
        metadata_quantiles_path=os.getenv("TEST_MBAPPE_METADATA_QUANTILES_PATH"),
        features_quantiles_path=os.getenv("TEST_MBAPPE_FEATURES_QUANTILES_PATH"),
        mapper=MbappeMapperBatchProcessing(),
    )


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
        output_filename = os.path.join(output_dir, 'probs_mbappe_' + batch_filename.replace("pkl", "parquet"))
        if os.path.exists(output_filename):
            continue
        tasks.append(
            delayed(predict_over_batch)(full_filename, output_filename)
        )

    Parallel(n_jobs=30, verbose=11, backend="loky")(tasks)
