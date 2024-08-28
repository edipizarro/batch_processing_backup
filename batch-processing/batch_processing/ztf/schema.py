from collections import OrderedDict

from polars import Float64, Int64, List, String, Struct

schema = OrderedDict(
    [
        ("schemavsn", String),
        ("publisher", String),
        ("objectId", String),
        (
            "candidate",
            Struct(
                {
                    "jd": Float64,
                    "fid": Int64,
                    "pid": Int64,
                    "diffmaglim": Float64,
                    "pdiffimfilename": String,
                    "programpi": String,
                    "programid": Int64,
                    "candid": Int64,
                    "isdiffpos": String,
                    "tblid": Int64,
                    "nid": Int64,
                    "rcid": Int64,
                    "field": Int64,
                    "xpos": Float64,
                    "ypos": Float64,
                    "ra": Float64,
                    "dec": Float64,
                    "magpsf": Float64,
                    "sigmapsf": Float64,
                    "chipsf": Float64,
                    "magap": Float64,
                    "sigmagap": Float64,
                    "distnr": Float64,
                    "magnr": Float64,
                    "sigmagnr": Float64,
                    "chinr": Float64,
                    "sharpnr": Float64,
                    "sky": Float64,
                    "magdiff": Float64,
                    "fwhm": Float64,
                    "classtar": Float64,
                    "mindtoedge": Float64,
                    "magfromlim": Float64,
                    "seeratio": Float64,
                    "aimage": Float64,
                    "bimage": Float64,
                    "aimagerat": Float64,
                    "bimagerat": Float64,
                    "elong": Float64,
                    "nneg": Int64,
                    "nbad": Int64,
                    "rb": Float64,
                    "ssdistnr": Float64,
                    "ssmagnr": Float64,
                    "ssnamenr": String,
                    "sumrat": Float64,
                    "magapbig": Float64,
                    "sigmagapbig": Float64,
                    "ranr": Float64,
                    "decnr": Float64,
                    "sgmag1": Float64,
                    "srmag1": Float64,
                    "simag1": Float64,
                    "szmag1": Float64,
                    "sgscore1": Float64,
                    "distpsnr1": Float64,
                    "ndethist": Int64,
                    "ncovhist": Int64,
                    "jdstarthist": Float64,
                    "jdendhist": Float64,
                    "scorr": Float64,
                    "tooflag": Int64,
                    "objectidps1": Int64,
                    "objectidps2": Int64,
                    "sgmag2": Float64,
                    "srmag2": Float64,
                    "simag2": Float64,
                    "szmag2": Float64,
                    "sgscore2": Float64,
                    "distpsnr2": Float64,
                    "objectidps3": Int64,
                    "sgmag3": Float64,
                    "srmag3": Float64,
                    "simag3": Float64,
                    "szmag3": Float64,
                    "sgscore3": Float64,
                    "distpsnr3": Float64,
                    "nmtchps": Int64,
                    "rfid": Int64,
                    "jdstartref": Float64,
                    "jdendref": Float64,
                    "nframesref": Int64,
                    "rbversion": String,
                    "dsnrms": Float64,
                    "ssnrms": Float64,
                    "dsdiff": Float64,
                    "magzpsci": Float64,
                    "magzpsciunc": Float64,
                    "magzpscirms": Float64,
                    "nmatches": Int64,
                    "clrcoeff": Float64,
                    "clrcounc": Float64,
                    "zpclrcov": Float64,
                    "zpmed": Float64,
                    "clrmed": Float64,
                    "clrrms": Float64,
                    "neargaia": Float64,
                    "neargaiabright": Float64,
                    "maggaia": Float64,
                    "maggaiabright": Float64,
                    "exptime": Float64,
                    "drb": Float64,
                    "drbversion": String,
                }
            ),
        ),
        (
            "prv_candidates",
            List(
                Struct(
                    {
                        "jd": Float64,
                        "fid": Int64,
                        "pid": Int64,
                        "diffmaglim": Float64,
                        "pdiffimfilename": String,
                        "programpi": String,
                        "programid": Int64,
                        "candid": Int64,
                        "isdiffpos": String,
                        "tblid": Int64,
                        "nid": Int64,
                        "rcid": Int64,
                        "field": Int64,
                        "xpos": Float64,
                        "ypos": Float64,
                        "ra": Float64,
                        "dec": Float64,
                        "magpsf": Float64,
                        "sigmapsf": Float64,
                        "chipsf": Float64,
                        "magap": Float64,
                        "sigmagap": Float64,
                        "distnr": Float64,
                        "magnr": Float64,
                        "sigmagnr": Float64,
                        "chinr": Float64,
                        "sharpnr": Float64,
                        "sky": Float64,
                        "magdiff": Float64,
                        "fwhm": Float64,
                        "classtar": Float64,
                        "mindtoedge": Float64,
                        "magfromlim": Float64,
                        "seeratio": Float64,
                        "aimage": Float64,
                        "bimage": Float64,
                        "aimagerat": Float64,
                        "bimagerat": Float64,
                        "elong": Float64,
                        "nneg": Int64,
                        "nbad": Int64,
                        "rb": Float64,
                        "ssdistnr": Float64,
                        "ssmagnr": Float64,
                        "ssnamenr": String,
                        "sumrat": Float64,
                        "magapbig": Float64,
                        "sigmagapbig": Float64,
                        "ranr": Float64,
                        "decnr": Float64,
                        "scorr": Float64,
                        "magzpsci": Float64,
                        "magzpsciunc": Float64,
                        "magzpscirms": Float64,
                        "clrcoeff": Float64,
                        "clrcounc": Float64,
                        "rbversion": String,
                    }
                )
            ),
        ),
        (
            "fp_hists",
            List(
                Struct(
                    {
                        "field": Int64,
                        "rcid": Int64,
                        "fid": Int64,
                        "pid": Int64,
                        "rfid": Int64,
                        "sciinpseeing": Float64,
                        "scibckgnd": Float64,
                        "scisigpix": Float64,
                        "magzpsci": Float64,
                        "magzpsciunc": Float64,
                        "magzpscirms": Float64,
                        "clrcoeff": Float64,
                        "clrcounc": Float64,
                        "exptime": Float64,
                        "adpctdif1": Float64,
                        "adpctdif2": Float64,
                        "diffmaglim": Float64,
                        "programid": Int64,
                        "jd": Float64,
                        "forcediffimflux": Float64,
                        "forcediffimfluxunc": Float64,
                        "procstatus": String,
                        "distnr": Float64,
                        "ranr": Float64,
                        "decnr": Float64,
                        "magnr": Float64,
                        "sigmagnr": Float64,
                        "chinr": Float64,
                        "sharpnr": Float64,
                    }
                )
            ),
        ),
    ]
)

schema_without_fp = OrderedDict(
    [
        ("schemavsn", String),
        ("publisher", String),
        ("objectId", String),
        (
            "candidate",
            Struct(
                {
                    "jd": Float64,
                    "fid": Int64,
                    "pid": Int64,
                    "diffmaglim": Float64,
                    "pdiffimfilename": String,
                    "programpi": String,
                    "programid": Int64,
                    "candid": Int64,
                    "isdiffpos": String,
                    "tblid": Int64,
                    "nid": Int64,
                    "rcid": Int64,
                    "field": Int64,
                    "xpos": Float64,
                    "ypos": Float64,
                    "ra": Float64,
                    "dec": Float64,
                    "magpsf": Float64,
                    "sigmapsf": Float64,
                    "chipsf": Float64,
                    "magap": Float64,
                    "sigmagap": Float64,
                    "distnr": Float64,
                    "magnr": Float64,
                    "sigmagnr": Float64,
                    "chinr": Float64,
                    "sharpnr": Float64,
                    "sky": Float64,
                    "magdiff": Float64,
                    "fwhm": Float64,
                    "classtar": Float64,
                    "mindtoedge": Float64,
                    "magfromlim": Float64,
                    "seeratio": Float64,
                    "aimage": Float64,
                    "bimage": Float64,
                    "aimagerat": Float64,
                    "bimagerat": Float64,
                    "elong": Float64,
                    "nneg": Int64,
                    "nbad": Int64,
                    "rb": Float64,
                    "ssdistnr": Float64,
                    "ssmagnr": Float64,
                    "ssnamenr": String,
                    "sumrat": Float64,
                    "magapbig": Float64,
                    "sigmagapbig": Float64,
                    "ranr": Float64,
                    "decnr": Float64,
                    "sgmag1": Float64,
                    "srmag1": Float64,
                    "simag1": Float64,
                    "szmag1": Float64,
                    "sgscore1": Float64,
                    "distpsnr1": Float64,
                    "ndethist": Int64,
                    "ncovhist": Int64,
                    "jdstarthist": Float64,
                    "jdendhist": Float64,
                    "scorr": Float64,
                    "tooflag": Int64,
                    "objectidps1": Int64,
                    "objectidps2": Int64,
                    "sgmag2": Float64,
                    "srmag2": Float64,
                    "simag2": Float64,
                    "szmag2": Float64,
                    "sgscore2": Float64,
                    "distpsnr2": Float64,
                    "objectidps3": Int64,
                    "sgmag3": Float64,
                    "srmag3": Float64,
                    "simag3": Float64,
                    "szmag3": Float64,
                    "sgscore3": Float64,
                    "distpsnr3": Float64,
                    "nmtchps": Int64,
                    "rfid": Int64,
                    "jdstartref": Float64,
                    "jdendref": Float64,
                    "nframesref": Int64,
                    "rbversion": String,
                    "dsnrms": Float64,
                    "ssnrms": Float64,
                    "dsdiff": Float64,
                    "magzpsci": Float64,
                    "magzpsciunc": Float64,
                    "magzpscirms": Float64,
                    "nmatches": Int64,
                    "clrcoeff": Float64,
                    "clrcounc": Float64,
                    "zpclrcov": Float64,
                    "zpmed": Float64,
                    "clrmed": Float64,
                    "clrrms": Float64,
                    "neargaia": Float64,
                    "neargaiabright": Float64,
                    "maggaia": Float64,
                    "maggaiabright": Float64,
                    "exptime": Float64,
                    "drb": Float64,
                    "drbversion": String,
                }
            ),
        ),
        (
            "prv_candidates",
            List(
                Struct(
                    {
                        "jd": Float64,
                        "fid": Int64,
                        "pid": Int64,
                        "diffmaglim": Float64,
                        "pdiffimfilename": String,
                        "programpi": String,
                        "programid": Int64,
                        "candid": Int64,
                        "isdiffpos": String,
                        "tblid": Int64,
                        "nid": Int64,
                        "rcid": Int64,
                        "field": Int64,
                        "xpos": Float64,
                        "ypos": Float64,
                        "ra": Float64,
                        "dec": Float64,
                        "magpsf": Float64,
                        "sigmapsf": Float64,
                        "chipsf": Float64,
                        "magap": Float64,
                        "sigmagap": Float64,
                        "distnr": Float64,
                        "magnr": Float64,
                        "sigmagnr": Float64,
                        "chinr": Float64,
                        "sharpnr": Float64,
                        "sky": Float64,
                        "magdiff": Float64,
                        "fwhm": Float64,
                        "classtar": Float64,
                        "mindtoedge": Float64,
                        "magfromlim": Float64,
                        "seeratio": Float64,
                        "aimage": Float64,
                        "bimage": Float64,
                        "aimagerat": Float64,
                        "bimagerat": Float64,
                        "elong": Float64,
                        "nneg": Int64,
                        "nbad": Int64,
                        "rb": Float64,
                        "ssdistnr": Float64,
                        "ssmagnr": Float64,
                        "ssnamenr": String,
                        "sumrat": Float64,
                        "magapbig": Float64,
                        "sigmagapbig": Float64,
                        "ranr": Float64,
                        "decnr": Float64,
                        "scorr": Float64,
                        "magzpsci": Float64,
                        "magzpsciunc": Float64,
                        "magzpscirms": Float64,
                        "clrcoeff": Float64,
                        "clrcounc": Float64,
                        "rbversion": String,
                    }
                )
            ),
        ),
    ]
)
