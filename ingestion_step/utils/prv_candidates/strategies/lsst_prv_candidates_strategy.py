from .base_prv_candidates_strategy import BasePrvCandidatesStrategy
from typing import List
from survey_parser_plugins.core import SurveyParser

import pandas as pd
import pickle

# Keys used on non detections for ZTF
NON_DET_KEYS = ["aid", "tid", "oid", "mjd", "diffmaglim", "fid"]


class LSSTPreviousCandidatesParser(SurveyParser):
    _source = "LSST"
    _generic_alert_message_key_mapping = {
        "candid": "diaSourceId",
        "mjd": "midPointTai",
        "fid": None,
        "rfid": None,
        "isdiffpos": None,
        "pid": None,
        "ra": "ra",
        "dec": "decl",
        "rb": None,
        "rbversion": None,
        "mag": "psFlux",
        "e_mag": "psFluxErr",
    }

    _fid_mapper = {  # u, g, r, i, z, Y
        "u": 0,
        "g": 1,
        "r": 2,
        "i": 3,
        "z": 4,
        "Y": 5,
    }

    @classmethod
    def parse_message(cls, message: dict) -> dict:
        if not cls.can_parse(message):
            raise KeyError("This parser can't parse message")
        oid = message["diaObjectId"]
        prv_candidate = message["diaSource"]
        prv_content = cls._generic_alert_message(
            prv_candidate, cls._generic_alert_message_key_mapping
        )
        # inclusion of extra attributes
        prv_content["oid"] = oid
        prv_content["aid"] = message["aid"]
        prv_content["tid"] = cls._source
        # attributes modification
        prv_content["isdiffpos"] = 0
        prv_content["parent_candid"] = message["parent_candid"]
        prv_content["e_ra"] = 0.001
        prv_content["e_dec"] = 0.001
        prv_content["pid"] = 0
        prv_content["fid"] = cls._fid_mapper[prv_candidate["filterName"]]
        return prv_content

    @classmethod
    def can_parse(cls, message: dict) -> bool:
        return "diaSource" in message.keys()

    @classmethod
    def parse(cls, messages: List[dict]) -> List[dict]:
        return list(map(cls.parse_message, messages))


class LSSTPrvCandidatesStrategy(BasePrvCandidatesStrategy):
    def process_prv_candidates(self, alerts: pd.DataFrame):
        detections = {}
        for index, alert in alerts.iterrows():
            oid = alert["oid"]
            tid = alert["tid"]
            aid = alert["aid"]
            candid = alert["candid"]
            if alert["extra_fields"]["prvDiaSources"] is not None:
                prv_candidates = pickle.loads(
                    alert["extra_fields"]["prvDiaSources"]
                )
                for prv in prv_candidates:
                    detections.update(
                        {
                            prv["diaSourceId"]: {
                                "diaObjectId": oid,
                                "publisher": tid,
                                "aid": aid,
                                "diaSource": prv,
                                "parent_candid": candid,
                            }
                        }
                    )
                del alert["extra_fields"]["prvDiaSources"]
        detections = LSSTPreviousCandidatesParser.parse(
            list(detections.values())
        )
        detections = pd.DataFrame(detections)
        non_detections = pd.DataFrame(columns=NON_DET_KEYS)

        return detections, non_detections
