det_col = [
    "objectId",
    "candid",
    "mjd",
    "fid",
    "pid",
    "diffmaglim",
    "isdiffpos",
    "nid",
    "ra",
    "dec",
    "magpsf",
    "sigmapsf",
    "magap",
    "sigmagap",
    "distnr",
    "rb",
    "rbversion",
    "drb",
    "drbversion",
    "magapbig",
    "sigmagapbig",
    "rfid",
    "magpsf_corr",
    "sigmapsf_corr",
    "sigmapsf_corr_ext",
    "corrected",
    "dubious",
    "parent_candid",
    "has_stamp",
    "step_id_corr",
]
obj_col = [
    "objectId",
    "ndethist",
    "ncovhist",
    "mjdstarthist",
    "mjdendhist",
    "corrected",
    "stellar",
    "ndet",
    "g-r_max",
    "g-r_max_corr",
    "g-r_mean",
    "g-r_mean_corr",
    "meanra",
    "meandec",
    "sigmara",
    "sigmadec",
    "deltamjd",
    "firstmjd",
    "lastmjd",
    "step_id_corr",
    "diffpos",
    "reference_change",
]
non_col = ["objectId", "mjd", "fid", "diffmaglim"]
ss_col = ["objectId", "candid", "ssdistnr", "ssmagnr", "ssnamenr"]
qua_col = [
    "objectId",
    "candid",
    "fid",
    "xpos",
    "ypos",
    "chipsf",
    "sky",
    "fwhm",
    "classtar",
    "mindtoedge",
    "seeratio",
    "aimage",
    "bimage",
    "aimagerat",
    "bimagerat",
    "nneg",
    "nbad",
    "sumrat",
    "scorr",
    "dsnrms",
    "ssnrms",
    "magzpsci",
    "magzpsciunc",
    "magzpscirms",
    "nmatches",
    "clrcoeff",
    "clrcounc",
    "zpclrcov",
    "zpmed",
    "clrmed",
    "clrrms",
    "exptime",
]
mag_col = [
    "objectId",
    "fid",
    "stellar",
    "corrected",
    "ndet",
    "ndubious",
    "dmdt_first",
    "dm_first",
    "sigmadm_first",
    "dt_first",
    "magpsf_mean",
    "magpsf_median",
    "magpsf_max",
    "magpsf_min",
    "magsigma",
    "magpsf_last",
    "magpsf_first",
    "magpsf_corr_mean",
    "magpsf_corr_median",
    "magpsf_corr_max",
    "magpsf_corr_min",
    "magsigma_corr",
    "magpsf_corr_last",
    "magpsf_corr_first",
    "first_mjd",
    "last_mjd",
    "step_id_corr",
    "saturation_rate",
]
ps1_col = [
    "objectId",
    "candid",
    "objectidps1",
    "sgmag1",
    "srmag1",
    "simag1",
    "szmag1",
    "sgscore1",
    "distpsnr1",
    "objectidps2",
    "sgmag2",
    "srmag2",
    "simag2",
    "szmag2",
    "sgscore2",
    "distpsnr2",
    "objectidps3",
    "sgmag3",
    "srmag3",
    "simag3",
    "szmag3",
    "sgscore3",
    "distpsnr3",
    "nmtchps",
    "unique1",
    "unique2",
    "unique3",
]
gaia_col = [
    "objectId",
    "candid",
    "neargaia",
    "neargaiabright",
    "maggaia",
    "maggaiabright",
    "unique1",
]
ref_col = [
    "rfid",
    "objectId",
    "candid",
    "fid",
    "rcid",
    "field",
    "magnr",
    "sigmagnr",
    "chinr",
    "sharpnr",
    "ranr",
    "decnr",
    "mjdstartref",
    "mjdendref",
    "nframesref",
]


xmatch_col = ["objectId_2", "catid", "designation", "distance"]

allwise_col = [
    "designation",
    "ra",
    "dec",
    "w1mpro",
    "w2mpro",
    "w3mpro",
    "w4mpro",
    "w1sigmpro",
    "w2sigmpro",
    "w3sigmpro",
    "w4sigmpro",
    "j_m_2mass",
    "h_m_2mass",
    "k_m_2mass",
    "j_msig_2mass",
    "h_msig_2mass",
    "k_msig_2mass",
]

fea_col = ["oid", "name", "value", "fid", "version"]
