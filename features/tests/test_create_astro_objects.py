import pandas as pd
import os
from lc_classifier.features.preprocess.ztf import ZTFLightcurvePreprocessor
from lc_classifier.features.composites.ztf import ZTFFeatureExtractor
from ..create_astro_objects_from_db import dataframes_to_astro_object_list
from lc_classifier.utils import all_features_from_astro_objects


script_path = os.path.dirname(os.path.abspath(__file__))


def test_check_input():
    detections = pd.read_parquet(os.path.join(script_path, "data/detections.parquet"))

    forced_photometry = pd.read_parquet(
        os.path.join(script_path, "data/forced_photometry.parquet")
    )

    xmatch = pd.read_parquet(os.path.join(script_path, "data/xmatch.parquet"))

    print(detections.columns)
    print(forced_photometry.columns)
    aos = dataframes_to_astro_object_list(detections, forced_photometry, xmatch)
    print(aos)


def test_feature_computation():
    detections = pd.read_parquet(os.path.join(script_path, "data/detections.parquet"))

    forced_photometry = pd.read_parquet(
        os.path.join(script_path, "data/forced_photometry.parquet")
    )

    xmatch = pd.read_parquet(os.path.join(script_path, "data/xmatch.parquet"))

    aos = dataframes_to_astro_object_list(detections, forced_photometry, xmatch)
    lightcurve_preprocessor = ZTFLightcurvePreprocessor()
    lightcurve_preprocessor.preprocess_batch(aos)

    feature_extractor = ZTFFeatureExtractor()
    feature_extractor.compute_features_batch(aos, progress_bar=True)
    print(aos)


def test_consolidate_features():
    detections = pd.read_parquet(os.path.join(script_path, "data/detections.parquet"))

    forced_photometry = pd.read_parquet(
        os.path.join(script_path, "data/forced_photometry.parquet")
    )

    xmatch = pd.read_parquet(os.path.join(script_path, "data/xmatch.parquet"))

    aos = dataframes_to_astro_object_list(detections, forced_photometry, xmatch)
    lightcurve_preprocessor = ZTFLightcurvePreprocessor()
    lightcurve_preprocessor.preprocess_batch(aos)

    feature_extractor = ZTFFeatureExtractor()
    feature_extractor.compute_features_batch(aos, progress_bar=True)
    all_features = all_features_from_astro_objects(aos)
    print(all_features)
