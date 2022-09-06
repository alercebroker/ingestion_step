import pickle
from typing import List

import pandas as pd
from survey_parser_plugins.core import SurveyParser

from .base_prv_candidates_strategy import BasePrvCandidatesStrategy

# Keys used on non detections for ZTF
NON_DET_KEYS = ["aid", "tid", "oid", "mjd", "diffmaglim", "fid"]
FORCED_PHOT_TO_NON_DET = {
    "filterName": "fid",
    "diaObjectId": "oid",
    "midPointTai": "mjd",
    "psFlux": "diffmaglim",
}


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
        prv_content["alertId"] = message["alertId"]
        return prv_content

    @classmethod
    def can_parse(cls, message: dict) -> bool:
        return "diaSource" in message.keys()

    @classmethod
    def parse(cls, messages: List[dict]) -> List[dict]:
        return list(map(cls.parse_message, messages))


class LSSTPrvCandidatesStrategy(BasePrvCandidatesStrategy):
    _source = "LSST"
    _factor = 10 ** (-3.9 / 2.5)
    _fid_mapper = {
        "u": 0,
        "g": 1,
        "r": 2,
        "i": 3,
        "z": 4,
        "Y": 5,
    }
    _extra_fields = [
        "diaForcedSourceId",
        "ccdVisitId",
        "psFluxErr",
        "totFlux",
        "totFluxErr",
    ]

    def process_prv_candidates(self, alerts: pd.DataFrame):
        detections = {}
        forced_phot_sources = []
        for index, alert in alerts.iterrows():
            oid = alert["oid"]
            tid = alert["tid"]
            aid = alert["aid"]
            alert_id = alert["alertId"]
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
                                "alertId": alert_id,
                            }
                        }
                    )
                del alert["extra_fields"]["prvDiaSources"]
            if alert["extra_fields"]["prvDiaForcedSources"] is not None:
                data = alert["extra_fields"]["prvDiaForcedSources"]
                forced_phot = pickle.loads(data)
                forced_phot_sources += forced_phot
                del alert["extra_fields"]["prvDiaForcedSources"]

        detections = LSSTPreviousCandidatesParser.parse(
            list(detections.values())
        )
        detections = pd.DataFrame(detections)
        # The forced photometry is carried in non_detections fields
        forced_phot_sources = (
            pd.DataFrame(forced_phot_sources).rename(
                columns=FORCED_PHOT_TO_NON_DET
            )
            if len(forced_phot_sources)
            else pd.DataFrame(columns=NON_DET_KEYS)
        )
        # Process some fields of forced photometry
        forced_phot_sources["fid"] = forced_phot_sources["fid"].apply(
            lambda x: self._fid_mapper[x]
        )
        forced_phot_sources["tid"] = self._source
        forced_phot_sources["oid"] = forced_phot_sources["oid"].astype(str)
        forced_phot_sources["aid"] = forced_phot_sources["oid"]

        forced_phot_sources["diffmaglim"] = (
            forced_phot_sources["diffmaglim"] * self._factor
        )
        forced_phot_sources["psFluxErr"] = (
            forced_phot_sources["psFluxErr"] * self._factor
        )
        forced_phot_sources["diaForcedSourceId"] = forced_phot_sources[
            "diaForcedSourceId"
        ].astype(int)
        forced_phot_sources["extra_fields"] = forced_phot_sources[
            self._extra_fields
        ].to_dict("records")
        forced_phot_sources.drop(columns=self._extra_fields, inplace=True)
        return detections, forced_phot_sources
