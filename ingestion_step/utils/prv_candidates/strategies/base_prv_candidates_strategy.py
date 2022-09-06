from abc import ABC, abstractmethod

import pandas as pd


class BasePrvCandidatesStrategy(ABC):
    """
    The Strategy interface declares operations common to all supported versions of some algorithm.

    The Context uses this interface to call the algorithm defined by Concrete Strategies.
    """

    @abstractmethod
    def process_prv_candidates(
        self, data: pd.DataFrame
    ) -> (pd.DataFrame, pd.DataFrame):
        pass
