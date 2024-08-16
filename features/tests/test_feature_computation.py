import pandas as pd
import os
from shutil import rmtree
from ..create_astro_objects import xmatch_df_to_astro_object_list
from ..compute_features import extract_features_many_files


script_path = os.path.dirname(os.path.abspath(__file__))


def test_feature_computation_from_spark():
    df_from_xmatch_step = pd.read_parquet(
        os.path.join(script_path, "data/sample_xmatch_test.snappy.parquet")
    )
    print(df_from_xmatch_step.columns)

    astro_objects = xmatch_df_to_astro_object_list(df_from_xmatch_step)
    print(astro_objects)


def test_feature_computation():
    # Create list of aos filenames
    aos_directory = os.path.join(script_path, "data/aos")
    output_directory = "/tmp/test_feature_computation_many_files"
    if os.path.exists(output_directory):
        rmtree(output_directory)
    os.makedirs(output_directory)
    assert len(os.listdir(output_directory)) == 0

    # Compute features
    extract_features_many_files(aos_directory, output_directory, 2)
    assert len(os.listdir(output_directory)) == len(os.listdir(aos_directory))
