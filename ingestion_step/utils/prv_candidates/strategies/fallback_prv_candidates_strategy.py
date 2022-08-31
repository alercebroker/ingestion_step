import pandas as pd

from .base_prv_candidates_strategy import BasePrvCandidatesStrategy

# Keys used on non detections for ZTF
NON_DET_KEYS = ["aid", "tid", "oid", "mjd", "diffmaglim", "fid"]


class FallbackPrvCandidatesStrategy(BasePrvCandidatesStrategy):
    def process_prv_candidates(self, alerts: pd.DataFrame):
        return pd.DataFrame(), pd.DataFrame(columns=NON_DET_KEYS)
