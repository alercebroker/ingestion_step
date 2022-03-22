import random
import pickle

from typing import List

random.seed(8798, version=2)


def random_ensure_choices(choices, data):
    for choice in choices:
        if choice not in data:
            return choice
        else:
            return random.choice(choices)


def get_ztf_prv_candidates(num_prv_candidates):
    if not num_prv_candidates:
        return None
    prv_candidates = []
    for i in range(num_prv_candidates):
        prv_candidates.append(
            {
                "jd": random.randrange(2458000, 2459000),
                "fid": random.choice([1, 2]),
                "pid": random.randint(1, 999999),
                "diffmaglim": random.uniform(15, 21),
                "pdiffimfilename": f"test{i}",
                "programpi": "Kulkarni",
                "programid": 2,
                "candid": random_ensure_choices(
                    [None, random.randint(1000000, 9999999)],
                    [c["candid"] for c in prv_candidates],
                ),
                "isdiffpos": random.choice([-1, 1]),
                "tblid": random.choice([None, random.randint(1, 9999999)]),
                "nid": random.choice([None, random.randint(1, 999999)]),
                "rcid": random.choice([None, random.randint(0, 63)]),
                "field": random.choice([None, random.randint(1, 1000)]),
                "xpos": random.choice([None, random.randrange(0, 64)]),
                "ypos": random.choice([None, random.randrange(0, 64)]),
                "ra": random.uniform(0, 360),
                "dec": random.uniform(-90, 90),
                "magpsf": random.uniform(15, 21),
                "sigmapsf": random.random(),
                "chipsf": random.choice([None, random.random()]),
                "magap": random.choice([None, random.random()]),
                "sigmagap": random.choice([None, random.random()]),
                "distnr": random.choice([None, random.random()]),
                "magnr": random.choice([None, random.random()]),
                "sigmagnr": random.choice([None, random.random()]),
                "chinr": random.choice([None, random.random()]),
                "sharpnr": random.choice([None, random.random()]),
                "sky": random.choice([None, random.random()]),
                "magdiff": random.choice([None, random.random()]),
                "fwhm": random.choice([None, random.random()]),
                "classtar": random.choice([None, random.random()]),
                "mindtoedge": random.choice([None, random.random()]),
                "magfromlim": random.choice([None, random.random()]),
                "seeratio": random.choice([None, random.random()]),
                "aimage": random.choice([None, random.random()]),
                "bimage": random.choice([None, random.random()]),
                "aimagerat": random.choice([None, random.random()]),
                "bimagerat": random.choice([None, random.random()]),
                "elong": random.choice([None, random.random()]),
                "nneg": random.choice([None, random.random()]),
                "nbad": random.choice([None, random.random()]),
                "rb": random.choice([None, random.random()]),
                "ssdistnr": random.choice([None, random.random()]),
                "ssmagnr": random.choice([None, random.random()]),
                "ssnamenr": random.choice([None, random.random()]),
                "sumrat": random.choice([None, random.random()]),
                "magapbig": random.choice([None, random.random()]),
                "sigmagapbig": random.choice([None, random.random()]),
                "ranr": random.random(),
                "decnr": random.random(),
                "scorr": random.choice([None, random.random()]),
                "magzpsci": random.choice([None, random.random()]),
                "magzpsciunc": random.choice([None, random.random()]),
                "magzpscirms": random.choice([None, random.random()]),
                "clrcoeff": random.choice([None, random.random()]),
                "clrcounc": random.choice([None, random.random()]),
                "rbversion": "t7_f4_c3",
            }
        )
    return pickle.dumps(prv_candidates)


def get_extra_fields(telescope: str, **kwargs):
    if telescope == "ATLAS":
        return {}
    elif telescope == "ZTF":
        return {
            "distnr": random.random(),
            "magnr": random.random(),
            "sigmagnr": random.random(),
            "ndethist": random.random(),
            "ncovhist": random.random(),
            "jdstarthist": random.uniform(59000, 60000),
            "jdendhist": random.uniform(59000, 60000),
            "prv_candidates": get_ztf_prv_candidates(**kwargs),
        }


def generate_message(num_messages: int, **kwargs):
    alerts = []
    telescopes = ["ATLAS", "ZTF"]
    for i in range(num_messages):
        alert = {
            "oid": f"{telescopes[i%2]}oid{i}",
            "tid": telescopes[i % 2],
            "candid": random.randint(1000000, 9000000),
            "mjd": random.uniform(59000, 60000),
            "fid": random.randint(1, 2),
            "ra": random.uniform(0, 360),
            "dec": random.uniform(-90, 90),
            "e_ra": random.random(),
            "e_dec": random.random(),
            "mag": random.uniform(15, 20),
            "e_mag": random.random(),
            "isdiffpos": random.choice([-1, 1]),
            "rb": random.random(),
            "rbversion": f"v{i}",
            "aid": random.randint(1000000, 9000000),
            "extra_fields": get_extra_fields(telescopes[i % 2], **kwargs),
        }
        alerts.append(alert)
    return alerts


def generate_message_atlas(num_messages):
    alerts = []
    for i in range(num_messages):
        alert = {
            "oid": f"ATLASoid{i}",
            "tid": "ATLAS",
            "candid": random.randint(1000000, 9000000),
            "mjd": random.uniform(59000, 60000),
            "fid": random.randint(1, 2),
            "ra": random.uniform(0, 360),
            "dec": random.uniform(-90, 90),
            "e_ra": random.random(),
            "e_dec": random.random(),
            "mag": random.uniform(15, 20),
            "e_mag": random.random(),
            "isdiffpos": random.choice([-1, 1]),
            "rb": random.random(),
            "rbversion": f"v{i}",
            "aid": f"AL2X{random.randint(1000, 9990)}",
            "extra_fields": get_extra_fields("ATLAS"),
        }
        alerts.append(alert)
    return alerts


def generate_message_ztf(num_messages: int):
    alerts = []
    for i in range(num_messages):
        alert = {
            "oid": f"ZTFoid{i}",
            "tid": "ZTF",
            "candid": random.randint(1000000, 9000000),
            "mjd": random.uniform(59000, 60000),
            "fid": random.randint(1, 2),
            "ra": random.uniform(0, 360),
            "dec": random.uniform(-90, 90),
            "e_ra": random.random(),
            "e_dec": random.random(),
            "mag": random.uniform(15, 20),
            "e_mag": random.random(),
            "isdiffpos": random.choice([-1, 1]),
            "rb": random.random(),
            "rbversion": f"v{i}",
            "aid": f"AL2X{random.randint(1000, 9990)}",
            "extra_fields": get_extra_fields(
                "ZTF", num_prv_candidates=random.randint(0, 20)
            ),
        }
        alerts.append(alert)
    return alerts


def random_sub_samples(samples: int, size: int):
    sequence = []
    if samples == 0:
        sequence = [0 for _ in range(size)]
    else:
        for _ in range(size):
            if _ == size - 1:
                val = samples - sum(sequence)
            else:
                remain = samples - sum(sequence)
                val = random.randrange(0, remain)
            sequence.append(val)
    return sequence


def generate_alerts_batch(n: int, same_objects: int = 10) -> List[dict]:
    """

    Parameters
    ----------
    n: number of alerts
    same_objects: number of objects in n alerts
    Returns a batch of generic alerts
    -------

    """
    generators = [generate_message_ztf, generate_message_atlas]
    sub_samples = random_sub_samples(n, len(generators))  # samples by generator
    batch = []
    for generator, m in zip(generators, sub_samples):
        alert = generator(m)
        batch.append(alert)
    batch = sum(batch, [])
    # adjust to repeat some identifiers
    for al in range(n):
        batch[al]["aid"] = f"AL2X{str(al % same_objects).zfill(5)}"
        batch[al]["oid"] = f"{batch[al]['tid']}2X{str(al % same_objects).zfill(5)}"
        batch[al]["candid"] = int(str(al + 1).ljust(8, "0"))
    random.shuffle(batch, lambda: 0.1)
    return batch


def generate_random_detections(n):
    batch = []
    for i in range(n):
        ex = {
            "tid": "EXAMPLE",
            "aid": f"ALERCE{i}",
            "oid": f"EX{i}",
            "candid": random.randint(1000000, 9000000),
            "mjd": random.uniform(59000, 60000),
            "fid": random.randint(1, 2),
            "ra": random.uniform(0, 360),
            "dec": random.uniform(-90, 90),
            "e_ra": random.random(),
            "e_dec": random.random(),
            "mag": random.uniform(15, 20),
            "e_mag": random.random(),
            "isdiffpos": random.choice([-1, 1]),
            "rb": random.random(),
            "rfid": random.randint(1000000, 9000000),
            "corrected": random.choice([False, True]),
            "parent_candid": None,
            "has_stamp": True,
            "step_id_corr": "example",
            "rbversion": "version123121",
            "extra_fields": {}
        }
        batch.append(ex)
    return batch


def generate_random_objects(n):
    batch = []
    for i in range(n):
        ex = {
            "_id": f"ALERCE{i}",
            "aid": f"ALERCE{i}",
            "oid": f"EX{i}",
            "meanra": random.uniform(0, 360),
            "meandec": random.uniform(-90, 90),
            "ndet": random.randint(1, 20),
            "firstmjd":  random.uniform(59000, 60000),
            "lastmjd":  random.uniform(59000, 60000) + random.uniform(1, 20),
            "extra_fields": {
                "e_ra": 0.,
                "e_dec": 0.,
                "tid": "EXAMPLE"
            }
        }
        batch.append(ex)
    return batch
