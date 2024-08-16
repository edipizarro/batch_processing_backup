import os
import pandas as pd
from create_astro_objects import dataframes_to_astro_object_list
from lc_classifier.features.core.base import save_astro_objects_batch


aos_folder = "data/aos"
test_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests")
aos_folder_full_path = os.path.join(test_path, aos_folder)

if not os.path.exists(aos_folder_full_path):
    os.makedirs(aos_folder_full_path)

# Load data
detections = pd.read_parquet(os.path.join(test_path, "data/detections.parquet"))

forced_photometry = pd.read_parquet(
    os.path.join(test_path, "data/forced_photometry.parquet")
)

xmatch = pd.read_parquet(os.path.join(test_path, "data/xmatch.parquet"))

aos = dataframes_to_astro_object_list(detections, forced_photometry, xmatch)

assert len(aos) == 4

save_astro_objects_batch(aos[:2], os.path.join(aos_folder_full_path, "aos_0.pkl"))
save_astro_objects_batch(aos[2:], os.path.join(aos_folder_full_path, "aos_1.pkl"))
