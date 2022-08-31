import pandas as pd

from .base_correction_strategy import BaseCorrectionStrategy


class LSSTCorrectionStrategy(BaseCorrectionStrategy):
    _factor = 10 ** (-3.9 / 2.5)

    def do_correction(self, detections: pd.DataFrame) -> pd.DataFrame:
        detections["corrected"] = True
        detections["mag"] = detections["mag"] * self._factor
        detections["e_mag"] = detections["e_mag"] * self._factor
        return detections
