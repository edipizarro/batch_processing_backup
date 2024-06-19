import pickle

import numpy as np
import pandas as pd
from lc_classifier.features.core.base import AstroObject
from lc_classifier.utils import mag2flux, mag_err_2_flux_err
from typing import List


class NoDetections(Exception):
    pass


def create_astro_object(
    detections: pd.DataFrame, forced_photometry: pd.DataFrame, xmatch: pd.DataFrame
) -> AstroObject:

    detection_keys = [
        "oid",
        "candid",
        "pid",
        "ra",
        "dec",
        "mjd",
        "magpsf_corr",
        "sigmapsf_corr",
        "magpsf",
        "sigmapsf",
        "fid",
        "isdiffpos",
    ]

    detections = detections[detection_keys]
    detections["forced"] = False

    forced_photometry["candid"] = forced_photometry["oid"] + forced_photometry[
        "pid"
    ].astype(str)
    forced_photometry_keys = [
        "oid",
        "candid",
        "pid",
        "ra",
        "dec",
        "mjd",
        "mag_corr",
        "e_mag_corr",
        "mag",
        "e_mag",
        "fid",
        "isdiffpos",
    ]
    forced_photometry = forced_photometry[forced_photometry_keys]
    forced_photometry["forced"] = True
    forced_photometry.rename(
        columns={
            "mag_corr": "magpsf_corr",
            "e_mag_corr": "sigmapsf_corr",
            "mag": "magpsf",
            "e_mag": "sigmapsf",
        },
        inplace=True,
    )

    a = pd.concat([detections, forced_photometry])
    a["aid"] = "aid_" + a["oid"]
    a["tid"] = "ZTF"
    a["sid"] = "ZTF"
    a.fillna(value=np.nan, inplace=True)
    a.rename(
        columns={"magpsf_corr": "brightness", "sigmapsf_corr": "e_brightness"},
        inplace=True,
    )
    a["unit"] = "magnitude"
    a_flux = a.copy()
    a_flux["brightness"] = mag2flux(a["magpsf"]) * a["isdiffpos"]
    a_flux["e_brightness"] = mag_err_2_flux_err(a["sigmapsf"], a["magpsf"])
    a_flux["unit"] = "diff_flux"
    a = pd.concat([a, a_flux], axis=0)
    a.set_index("aid", inplace=True)
    a["fid"] = a["fid"].map({1: "g", 2: "r", 3: "i"})
    a = a[a["fid"].isin(["g", "r"])]

    aid = a.index.values[0]
    oid = a["oid"].iloc[0]

    aid_forced = a[a["forced"]]
    aid_detections = a[~a["forced"]]

    metadata = pd.DataFrame(
        [
            ["aid", aid],
            ["oid", oid],
            ["W1", xmatch["w1mpro"]],
            ["W2", xmatch["w2mpro"]],
            ["W3", xmatch["w3mpro"]],
            ["W4", xmatch["w4mpro"]],
            ["sgscore1", xmatch["sgscore1"]],
            ["sgmag1", xmatch["sgmag1"]],
            ["srmag1", xmatch["srmag1"]],
            ["distpsnr1", xmatch["distpsnr1"]],
        ],
        columns=["name", "value"],
    ).fillna(value=np.nan)

    astro_object = AstroObject(
        detections=aid_detections, forced_photometry=aid_forced, metadata=metadata
    )
    return astro_object


def dataframes_to_astro_object_list(detections, forced_photometry, xmatch):

    oids = detections["oid"].unique()
    print(oids)
    astro_objects_list = []
    for oid in oids:
        xmatch_oid = xmatch[xmatch["oid"] == oid]
        assert len(xmatch_oid) == 1
        xmatch_oid = xmatch_oid.iloc[0]
        ao = create_astro_object(
            detections[detections["oid"] == oid],
            forced_photometry[forced_photometry["oid"] == oid],
            xmatch_oid,
        )
        astro_objects_list.append(ao)
    return astro_objects_list


def save_batch(astro_objects: List[AstroObject], filename: str):
    with open(filename, "wb") as f:
        pickle.dump(astro_objects, f)
