from alerce_classifiers.base.dto import InputDTO
from alerce_classifiers.base.dto import OutputDTO
from alerce_classifiers.base.mapper import Mapper
from alerce_classifiers.utils.dataframe.mbappe_utils import DataframeUtils
from alerce_classifiers.mbappe.utils import magdiff2flux_uJy, fluxerr

import pandas as pd
import numpy as np
import torch
import yaml
import os
from alerce_classifiers.mbappe import configs
from lc_classifier.utils import AstroObject
from typing import List


class MbappeMapperBatchProcessing(Mapper):
    config_file_path = os.path.join(configs.__path__[0], "dict_info.yaml")
    with open(config_file_path, "r") as file:
        dict_info = yaml.load(file, Loader=yaml.FullLoader)

    _rename_cols = {
        "fluxdiff_uJy": "flux",
        "fluxerrdiff_uJy": "flux_error",
        "fid": "fid",
        "mjd": "time",
    }

    def _preprocess_detections(self, detections: pd.DataFrame):
        detections["fluxdiff_uJy"] = magdiff2flux_uJy(df=detections, col_mag="mag")
        detections["fluxerrdiff_uJy"] = fluxerr(
            df=detections, col_magerr="e_mag", col_flux="fluxdiff_uJy"
        )

        # Las curvas vienen ordenadas de menor a mayor?
        detections.replace({None: 0}, inplace=True)
        detections.replace({np.nan: 0}, inplace=True)

        detections = detections.rename(columns=self._rename_cols)

        #######################################################################
        # Generate windows
        detections["window_id"] = detections.groupby("oid").cumcount() // 200
        detections = detections.groupby(["oid", "window_id"]).agg(lambda x: list(x))

        # Transform features that are time series to matrices
        detections["time"], detections["flux"] = zip(
            *detections.apply(
                lambda x: DataframeUtils.separate_by_filter(
                    x["time"], x["flux"], x["fid"], self.dict_info
                ),
                axis=1,
            )
        )

        detections["time"] = detections.apply(
            lambda x: DataframeUtils.normalizing_time(x["time"]), axis=1
        )

        detections["mask"] = detections.apply(
            lambda x: DataframeUtils.create_mask(x["flux"]), axis=1
        )

        return detections

    def _preprocess_features(self, features: pd.DataFrame, quantiles: dict):
        features = features.apply(pd.to_numeric, errors="coerce").fillna(-9999)
        features = features[self.dict_info["md_cols"] + self.dict_info["feat_cols"]]

        features.loc[:, self.dict_info["md_cols"]] = quantiles["quantile_md"].transform(
            features[self.dict_info["md_cols"]].values
        )
        features.loc[:, self.dict_info["feat_cols"]] = quantiles[
            "quantile_feat"
        ].transform(features[self.dict_info["feat_cols"]].values)

        return features

    def _to_tensor_dict(self, df_lc_feat: pd.DataFrame, feat_columns: list) -> dict:
        df_feat = df_lc_feat[feat_columns]
        torch_input = {
            "time": torch.from_numpy(np.stack(df_lc_feat["time"].values, 0)).float(),
            "mask": torch.from_numpy(np.stack(df_lc_feat["mask"].values, 0)).float(),
            "data": torch.from_numpy(np.stack(df_lc_feat["flux"].values, 0)).float(),
            "tabular_feat": torch.from_numpy(df_feat.values).unsqueeze(2).float(),
        }
        return torch_input

    def preprocess(self, astro_objects: List[AstroObject], **kwargs) -> tuple:
        preprocessed_light_curve = self._preprocess_detections(data_input.detections)
        preprocessed_features = self._preprocess_features(
            data_input.features, kwargs["quantiles"]
        )

        df_lc_feat = pd.merge(
            preprocessed_light_curve.reset_index(level="window_id"),
            preprocessed_features,
            left_index=True,
            right_index=True,
        )

        feat_columns = preprocessed_features.columns
        torch_input = self._to_tensor_dict(df_lc_feat, feat_columns)

        return torch_input, df_lc_feat.index

    def postprocess(self, model_output, **kwargs) -> OutputDTO:
        probs = model_output.detach().numpy()
        probs = pd.DataFrame(probs, columns=kwargs["taxonomy"], index=kwargs["index"])
        probs = probs.groupby(level="oid").mean()

        return OutputDTO(probs, {"children": pd.DataFrame([]), "top": {}})
