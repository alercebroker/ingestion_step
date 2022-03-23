DET_KEYS = [
    "aid",
    "tid",
    "oid",
    "candid",
    "mjd",
    "fid",
    "ra",
    "dec",
    "rb",
    "rbversion",
    "mag",
    "e_mag",
    "rfid",
    "e_ra",
    "e_dec",
    "isdiffpos",
    "has_stamp",
    "parent_candid",
    "corrected",
    "step_id_corr",
    "extra_fields",
]

OBJ_KEYS = [
    "aid",
    "tid",
    "oid",
    "lastmjd",
    "firstmjd",
    "meanra",
    "meandec",
    "e_ra",
    "e_dec",
]
DATAQUALITY_KEYS = [
    "oid",
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
    "magzpsci",
    "magzpsciunc",
    "magzpscirms",
    "clrcoeff",
    "clrcounc",
    "dsnrms",
    "ssnrms",
    "nmatches",
    "zpclrcov",
    "zpmed",
    "clrmed",
    "clrrms",
    "exptime",
]
OLD_DET_KEYS = [
    "oid",
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
NON_DET_KEYS = ["aid", "tid", "mjd", "diffmaglim", "fid", "oid"]
COR_KEYS = ["magpsf_corr", "sigmapsf_corr", "sigmapsf_corr_ext"]
SS_KEYS = ["oid", "candid", "ssdistnr", "ssmagnr", "ssnamenr"]

REFERENCE_KEYS = [
    "oid",
    "rfid",
    "candid",
    "fid",
    "rcid",
    "field",
    "magnr",
    "sigmagnr",
    "chinr",
    "sharpnr",
    "chinr",
    "ranr",
    "decnr",
    "nframesref",
    "mjdstartref",
    "mjdendref",
]
PS1_MultKey = ["objectidps", "sgmag", "srmag", "simag", "szmag", "sgscore", "distpsnr"]
PS1_KEYS = ["oid", "candid", "nmtchps"]
for i in range(1, 4):
    PS1_KEYS = PS1_KEYS + [f"{key}{i}" for key in PS1_MultKey]
