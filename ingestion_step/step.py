import logging
import pickle
import sys
from typing import List, Tuple

import numpy as np
import pandas as pd
from apf.core.step import GenericStep
from apf.producers import KafkaProducer

from ingestion_step.utils.multi_driver.connection import MultiDriverConnection

from .utils.constants import DET_KEYS, NON_DET_KEYS, OBJ_KEYS, OLD_DET_KEYS
from .utils.correction.corrector import Corrector
from .utils.correction.strategies import (FallbackCorrectionStrategy,
                                          LSSTCorrectionStrategy,
                                          ZTFCorrectionStrategy)
from .utils.old_preprocess import (compute_dmdt, do_flags, do_magstats,
                                   get_catalog, insert_dataquality,
                                   insert_gaia, insert_magstats, insert_ps1,
                                   insert_reference, insert_ss,
                                   preprocess_dataquality, preprocess_gaia,
                                   preprocess_objects_, preprocess_ps1,
                                   preprocess_reference, preprocess_ss)
from .utils.prv_candidates.processor import Processor
from .utils.prv_candidates.strategies import (FallbackPrvCandidatesStrategy,
                                              LSSTPrvCandidatesStrategy,
                                              ZTFPrvCandidatesStrategy)

sys.path.insert(0, "../../../../")
pd.options.mode.chained_assignment = None
logging.getLogger("GP").setLevel(logging.WARNING)
np.seterr(divide="ignore")


def parse_metadata(df: pd.DataFrame, name) -> pd.DataFrame:
    df["candid"] = df["candid"].astype("int")
    df.sort_values(
        by="candid", inplace=True, ignore_index=True, ascending=True
    )
    df.drop_duplicates(subset="oid", keep="last", inplace=True)
    index = df["oid"].values
    new_df = pd.DataFrame({name: df.to_dict("records")}, index=index)
    return new_df


class IngestionStep(GenericStep):
    """IngestionStep Description

    Parameters
    ----------
    consumer : GenericConsumer
        Description of parameter `consumer`.
    **step_args : type
        Other args passed to step (DB connections, API requests, etc.)

    """

    def __init__(
        self,
        consumer=None,
        config=None,
        level=logging.INFO,
        producer=None,
        db_connection=None,
        **step_args,
    ):
        super().__init__(consumer, config=config, level=level)
        self.version = config["STEP_METADATA"]["STEP_VERSION"]
        self.prv_candidates_processor = Processor(
            ZTFPrvCandidatesStrategy()
        )  # initial strategy (can change)
        self.detections_corrector = Corrector(
            ZTFCorrectionStrategy()
        )  # initial strategy (can change)
        self.producer = producer
        if config.get("PRODUCER_CONFIG", False):
            self.producer = KafkaProducer(config["PRODUCER_CONFIG"])

        self.driver = db_connection or MultiDriverConnection(
            config["DB_CONFIG"]
        )
        self.driver.connect()

    def get_objects(self, aids: List[str or int], engine="mongo"):
        """

        Parameters
        ----------
        aids
        engine
        Returns
        -------

        """
        filter_by = {"aid": {"$in": aids}}
        objects = self.driver.query("Object", engine=engine).find_all(
            filter_by=filter_by, paginate=False
        )
        if len(objects) == 0 or engine == "mongo":
            return pd.DataFrame(objects, columns=OBJ_KEYS)
        return pd.DataFrame(objects)

    def get_detections(self, aids: List[str or int], engine="mongo"):
        """

        Parameters
        ----------
        aids
        engine
        Returns
        -------

        """
        filter_by = {"aid": {"$in": aids}}
        detections = self.driver.query("Detection", engine=engine).find_all(
            filter_by=filter_by, paginate=False
        )
        if len(detections) == 0 or engine == "mongo":
            return pd.DataFrame(detections, columns=DET_KEYS)
        return pd.DataFrame(detections)

    def get_non_detections(self, aids: List[str or int], engine="mongo"):
        """

        Parameters
        ----------
        aids
        engine
        Returns
        -------

        """
        filter_by = {"aid": {"$in": aids}}
        non_detections = self.driver.query(
            "NonDetection", engine=engine
        ).find_all(filter_by=filter_by, paginate=False)
        if len(non_detections) == 0 or engine == "mongo":
            return pd.DataFrame(non_detections, columns=NON_DET_KEYS)
        return pd.DataFrame(non_detections)

    def insert_objects(self, objects: pd.DataFrame, engine="mongo") -> None:
        """
        Insert or update records in database. Insert new objects. Update old objects.

        Parameters
        ----------
        objects: Dataframe of astronomical objects.
        engine
        Returns
        -------

        """
        if engine == "mongo":
            objects.drop_duplicates(["aid"], inplace=True)
        else:
            objects.drop_duplicates(["oid"], inplace=True)
        new_objects = objects["new"]
        objects.drop(columns=["new"], inplace=True)

        to_insert = objects[new_objects]
        to_update = objects[~new_objects]
        self.logger.info(
            f"Inserting {len(to_insert)} and updating {len(to_update)} object(s)"
        )
        if len(to_insert) > 0:
            if engine == "mongo":
                to_insert["_id"] = to_insert["aid"].values
            to_insert.replace({np.nan: None}, inplace=True)
            dict_to_insert = to_insert.to_dict("records")
            self.driver.query("Object", engine=engine).bulk_insert(
                dict_to_insert
            )
            del to_insert

        if len(to_update) > 0:
            to_update.replace({np.nan: None}, inplace=True)
            dict_to_update = to_update.to_dict("records")
            filters = []
            for obj in dict_to_update:
                if engine == "mongo":
                    filters.append({"_id": obj["aid"]})
                else:
                    filters.append({"_id": obj["oid"]})
            self.driver.query("Object", engine=engine).bulk_update(
                dict_to_update, filter_by=filters
            )

    def insert_detections(self, detections: pd.DataFrame, engine="mongo"):
        """

        Parameters
        ----------
        detections
        engine
        Returns
        -------

        """
        self.logger.info(
            f"Inserting {len(detections)} new detections {engine}"
        )
        detections = detections.where(detections.notnull(), None)
        dict_detections = detections.to_dict("records")
        self.driver.query("Detection", engine=engine).bulk_insert(
            dict_detections
        )

    def insert_non_detections(
        self, non_detections: pd.DataFrame, engine="mongo"
    ):
        """

        Parameters
        ----------
        non_detections
        engine
        Returns
        -------

        """
        self.logger.info(
            f"Inserting {len(non_detections)} new non_detections {engine}"
        )
        non_detections.replace({np.nan: None}, inplace=True)
        dict_non_detections = non_detections.to_dict("records")
        self.driver.query("NonDetection", engine=engine).bulk_insert(
            dict_non_detections
        )

    @classmethod
    def calculate_stats_coordinates(cls, coordinates, e_coordinates):
        e_coordinates = e_coordinates / 3600
        num_coordinate = np.sum(coordinates / e_coordinates**2)
        den_coordinate = np.sum(1 / e_coordinates**2)
        mean_coordinate = num_coordinate / den_coordinate
        e_coord = np.sqrt(1 / den_coordinate) * 3600
        return mean_coordinate, e_coord

    def compute_meanra(self, ras, e_ras):
        mean_ra, e_ra = self.calculate_stats_coordinates(ras, e_ras)
        if 0.0 <= mean_ra <= 360.0:
            return mean_ra, e_ra
        else:
            raise ValueError(
                f"Mean ra must be between 0 and 360 (given {mean_ra})"
            )

    def compute_meandec(self, decs, e_decs):
        mean_dec, e_dec = self.calculate_stats_coordinates(decs, e_decs)
        if -90.0 <= mean_dec <= 90.0:
            return mean_dec, e_dec
        else:
            raise ValueError(
                f"Mean dec must be between -90 and 90 (given {mean_dec})"
            )

    def apply_objs_stats_from_correction(self, df: pd.DataFrame) -> pd.Series:
        response = {}
        df_mjd = df.mjd
        idx_min = df_mjd.values.argmin()
        df_min = df.iloc[idx_min]
        df_ra = np.abs(df["ra"])
        df_dec = df["dec"]
        df_e_ra = df["e_ra"]
        df_e_dec = df["e_dec"]

        response["meanra"], response["e_ra"] = self.compute_meanra(
            df_ra, df_e_ra
        )
        response["meandec"], response["e_dec"] = self.compute_meandec(
            df_dec, df_e_dec
        )
        response["firstmjd"] = df_mjd.min()
        response["lastmjd"] = df_mjd.max()
        response["tid"] = list(df["tid"].unique())
        response["oid"] = list(df["oid"].unique())
        response["ndet"] = len(df)
        return pd.Series(response)

    def preprocess_objects(self, objects: pd.DataFrame, light_curves: dict):
        """

        Parameters
        ----------
        objects
        light_curves

        Returns
        -------

        """
        # Keep existing objects
        aids = objects["aid"].unique()
        detections = light_curves["detections"]
        detections.drop_duplicates(
            ["candid", "aid"], inplace=True, keep="first"
        )
        detections.reset_index(inplace=True, drop=True)
        # New objects referer to: empirical new objects
        # (without detections in the past) and modified objects
        # (I mean existing objects in database)
        new_objects = detections.groupby("aid").apply(
            self.apply_objs_stats_from_correction
        )
        new_objects.reset_index(inplace=True)
        new_objects["new"] = ~new_objects["aid"].isin(aids)
        return new_objects

    def obj_stats(self, df: pd.DataFrame):
        response = {}
        df_mjd = df["mjd"]
        idxmax = df_mjd.values.argmax()
        df_max = df.iloc[idxmax]
        df_ra = df["ra"]
        df_dec = df["dec"]
        response["ndethist"] = df_max["ndethist"]
        response["ncovhist"] = df_max["ncovhist"]
        response["mjdstarthist"] = df_max["jdstarthist"] - 2400000.5
        response["mjdendhist"] = df_max["jdendhist"] - 2400000.5
        response["meanra"] = df_ra.mean()
        response["meandec"] = df_dec.mean()
        response["sigmara"] = df_ra.std()
        response["sigmadec"] = df_dec.std()
        response["firstmjd"] = df_mjd.min()
        response["lastmjd"] = df_max.mjd
        response["deltamjd"] = response["lastmjd"] - response["firstmjd"]
        response["diffpos"] = df["isdiffpos"].min() > 0
        response["reference_change"] = (
            response["mjdendhist"] > response["firstmjd"]
        )
        return pd.Series(response)

    def preprocess_objects_psql(
        self, objects: pd.DataFrame, light_curves: dict
    ):
        """

        Parameters
        ----------
        objects
        light_curves

        Returns
        -------

        """
        # Keep existing objects
        oids = objects["oid"].unique()
        detections = light_curves["detections"]
        detections.drop_duplicates(
            ["candid", "oid"], inplace=True, keep="first"
        )
        detections.reset_index(inplace=True, drop=True)
        # New objects referer to: empirical new objects
        # (without detections in the past) and modified objects
        # (I mean existing objects in database)
        new_objects = detections.groupby("oid").apply(self.obj_stats)
        new_objects.reset_index(inplace=True)
        new_objects["new"] = ~new_objects["oid"].isin(oids)
        return new_objects

    def get_lightcurves(self, oids, engine="mongo"):
        """

        Parameters
        ----------
        oids
        engine
        Returns
        -------

        """
        light_curves = {
            "detections": self.get_detections(oids, engine=engine),
            "non_detections": self.get_non_detections(oids, engine=engine),
        }
        self.logger.info(
            f"Light Curves ({len(oids)} objects) of this batch: "
            + f"{len(light_curves['detections'])} detections,"
            + f" {len(light_curves['non_detections'])}"
            + " non_detections in database"
        )
        return light_curves

    def preprocess_lightcurves(
        self,
        detections: pd.DataFrame,
        non_detections: pd.DataFrame,
        engine="mongo",
    ) -> dict:
        """

        Parameters
        ----------
        detections
        non_detections
        engine
        Returns
        -------

        """
        # TODO: remove index in psql iteration
        # Assign a label to difference new detections
        detections["new"] = True
        # Get unique oids from new alerts
        if engine == "psql":
            aids = detections["oid"].unique().tolist()
        else:
            aids = detections["aid"].unique().tolist()
        # Retrieve old detections and non_detections from database
        # and put new label to false
        light_curves = self.get_lightcurves(aids, engine=engine)
        light_curves["detections"]["new"] = False
        light_curves["non_detections"]["new"] = False
        old_detections = light_curves["detections"]

        # Remove tuple of [aid, candid] that are new detections and
        # old detections. This is a mask that retrieve
        # existing tuples on db.
        if engine == "mongo":
            unique_keys_detections = ["aid", "candid"]
        else:
            unique_keys_detections = ["oid", "candid"]
        # Checking if already on the database
        index_detections = pd.MultiIndex.from_frame(
            detections[unique_keys_detections]
        )
        old_index_detections = pd.MultiIndex.from_frame(
            old_detections[unique_keys_detections]
        )
        detections_already_on_db = index_detections.isin(old_index_detections)
        # Apply mask and get only new detections on detections from stream.
        new_detections = detections[~detections_already_on_db]
        # Get all light curve: only detections since beginning of time
        light_curves["detections"] = pd.concat(
            [old_detections, new_detections], ignore_index=True
        )

        non_detections["new"] = True
        old_non_detections = light_curves["non_detections"]
        if len(non_detections):
            # Using round 5 to have 5 decimals of precision to
            # delete duplicates non_detections
            non_detections["round_mjd"] = non_detections["mjd"].round(5)
            old_non_detections["round_mjd"] = old_non_detections["mjd"].round(
                5
            )
            # Remove [aid, fid, round_mjd] that are new non_dets
            # and old non_dets.
            if engine == "mongo":
                unique_keys_non_detections = ["aid", "fid", "round_mjd"]
            else:
                unique_keys_non_detections = ["oid", "fid", "round_mjd"]
            # Checking if already on the database
            index_non_detections = pd.MultiIndex.from_frame(
                non_detections[unique_keys_non_detections]
            )
            old_index_non_detections = pd.MultiIndex.from_frame(
                old_non_detections[unique_keys_non_detections]
            )
            non_dets_already_on_db = index_non_detections.isin(
                old_index_non_detections
            )
            # Apply mask and get only new non detections on
            # non detections from stream.
            new_non_detections = non_detections[~non_dets_already_on_db]
            # Get all light curve: only detections since beginning of time
            non_detections = pd.concat(
                [old_non_detections, new_non_detections], ignore_index=True
            )
            non_detections.drop(columns=["round_mjd"], inplace=True)
            light_curves["non_detections"] = non_detections
        return light_curves

    def process_prv_candidates(
        self, alerts: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Separate previous candidates from alerts.

        The input must be a DataFrame created from a list of GenericAlert.

        Parameters
        ----------
        alerts: A pandas DataFrame created from a list of GenericAlerts.

        Returns A Tuple with detections a non_detections from previous candidates
        -------

        """
        # dicto = {
        #     "ZTF": ZTFPrvCandidatesStrategy()
        # }
        data = alerts.copy()
        detections = []
        non_detections = []
        for tid, subset_data in data.groupby("tid"):
            if tid == "ZTF":
                self.prv_candidates_processor.strategy = (
                    ZTFPrvCandidatesStrategy()
                )
            elif tid == "LSST":
                self.prv_candidates_processor.strategy = (
                    LSSTPrvCandidatesStrategy()
                )
            else:
                self.prv_candidates_processor.strategy = (
                    FallbackPrvCandidatesStrategy()
                )
            det, non_det = self.prv_candidates_processor.compute(subset_data)
            detections.append(det)
            non_detections.append(non_det)
        detections = pd.concat(detections, ignore_index=True)
        non_detections = pd.concat(non_detections, ignore_index=True)
        return detections, non_detections

    def correct(self, detections: pd.DataFrame) -> pd.DataFrame:
        """Correct Detections.

        Parameters
        ----------
        detections

        Returns
        -------

        """
        response = []
        for idx, gdf in detections.groupby("tid"):
            if "ZTF" == idx:
                self.detections_corrector.strategy = ZTFCorrectionStrategy()
            elif "LSST" == idx:
                self.detections_corrector.strategy = LSSTCorrectionStrategy()
            else:
                self.detections_corrector.strategy = (
                    FallbackCorrectionStrategy()
                )
            corrected = self.detections_corrector.compute(gdf)
            response.append(corrected)
        response = pd.concat(response, ignore_index=True)
        return response

    def produce(
        self,
        alerts: pd.DataFrame,
        objects: pd.DataFrame,
        light_curves: dict,
        metadata: pd.DataFrame,
        key: str = "aid",
    ) -> None:
        """Produce light curves to topic configured on settings.py

        Parameters
        ----------
        alerts
        objects
        light_curves
        metadata
        key
        Returns
        -------

        """
        if self.producer is None:
            raise Exception("Kafka producer not configured in settings.py")
        # remove unused columns
        light_curves["detections"].drop(columns=["new"], inplace=True)
        light_curves["non_detections"].drop(columns=["new"], inplace=True)
        # sort by ascending mjd
        objects.sort_values("lastmjd", inplace=True, ascending=True)
        self.logger.info(f"Checking {len(objects)} messages (key={key})")
        n_messages = 0
        objects.set_index(key, inplace=True)
        for index, row in alerts.iterrows():
            _key = row[key]
            aid = row["aid"]
            oid = row["oid"]

            metadata_ = None
            if metadata is not None:
                if oid in metadata.index:
                    metadata_ = metadata.loc[oid].to_dict()

            publish_timestamp = row["elasticcPublishTimestamp"]
            ingest_timestamp = row["brokerIngestTimestamp"]

            mask_detections = light_curves["detections"][key] == _key
            detections = light_curves["detections"].loc[mask_detections]
            detections.replace({np.nan: None}, inplace=True)
            detections = detections.to_dict("records")
            mask_non_detections = light_curves["non_detections"][key] == _key
            non_detections = light_curves["non_detections"].loc[
                mask_non_detections
            ]
            non_detections = non_detections.to_dict("records")
            the_object = objects.loc[_key]
            output_message = {
                "aid": str(aid),
                "alertId": str(row["alertId"]),
                "meanra": the_object["meanra"],
                "meandec": the_object["meandec"],
                "ndet": the_object["ndet"],
                "candid": row["candid"],
                "detections": detections,
                "non_detections": non_detections,
                "metadata": metadata_,
                "elasticcPublishTimestamp": publish_timestamp,
                "brokerIngestTimestamp": ingest_timestamp,
            }
            self.producer.produce(output_message, key=aid)
            n_messages += 1
        self.logger.info(f"{n_messages} messages produced")

    # TEMPORAL CODE
    def execute_psql(
        self,
        alerts: pd.DataFrame,
        detections: pd.DataFrame,
        non_detections_prv_candidates: pd.DataFrame,
    ):
        # Get just ZTF objects
        alerts = alerts[alerts["tid"] == "ZTF"]
        # No alerts of ZTF on the batch, continue
        if len(alerts) == 0:
            return
        self.logger.info("Working on PSQL")
        detections = detections[detections["tid"] == "ZTF"]
        non_detections_prv_candidates = non_detections_prv_candidates[
            non_detections_prv_candidates["tid"] == "ZTF"
        ]

        # Reset all indexes
        alerts.reset_index(inplace=True)
        detections.reset_index(inplace=True)
        non_detections_prv_candidates.reset_index(inplace=True)
        # Get unique oids for ZTF
        unique_oids = alerts["oid"].unique().tolist()
        # Create a new dataframe with extra fields and remove it from detections
        extra_fields = list(detections["extra_fields"].values)
        extra_fields = pd.DataFrame(extra_fields)
        for c in ["prv_candidates", "pid"]:
            if c in extra_fields.columns:
                extra_fields.drop(columns=[c], inplace=True)
        del detections["extra_fields"]
        # Join detections with extra fields (old format of detections)
        detections = detections.join(extra_fields)
        detections["magpsf"] = detections["mag"]
        detections["sigmapsf"] = detections["e_mag"]
        # Get catalogs data and combined it with historic data
        # Dataquality
        dataquality = preprocess_dataquality(detections)
        # SS
        ss = get_catalog(unique_oids, "Ss_ztf", self.driver)
        ss = preprocess_ss(ss, detections)
        # Reference
        reference = get_catalog(unique_oids, "Reference", self.driver)
        reference = preprocess_reference(reference, detections)
        # PS1
        ps1 = get_catalog(unique_oids, "Ps1_ztf", self.driver)
        ps1 = preprocess_ps1(ps1, detections)
        # GAIA
        gaia = get_catalog(unique_oids, "Gaia_ztf", self.driver)
        gaia = preprocess_gaia(gaia, detections)
        # Get historic
        light_curves = self.preprocess_lightcurves(
            detections, non_detections_prv_candidates, engine="psql"
        )
        # compute magstats with historic catalogs
        old_magstats = get_catalog(unique_oids, "MagStats", self.driver)
        new_magstats = do_magstats(
            light_curves, old_magstats, ps1, reference, self.version
        )
        # Compute flags
        obj_flags, magstat_flags = do_flags(
            light_curves["detections"], reference
        )
        dmdt = compute_dmdt(light_curves, new_magstats)
        if len(dmdt) > 0:
            new_stats = new_magstats.set_index(["oid", "fid"]).join(
                dmdt.set_index(["oid", "fid"])
            )
            new_stats.reset_index(inplace=True)
        else:
            empty_dmdt = [
                "dmdt_first",
                "dm_first",
                "sigmadm_first",
                "dt_first",
            ]
            new_stats = new_magstats.reindex(
                columns=new_magstats.columns.tolist() + empty_dmdt
            )

        new_stats.set_index(["oid", "fid"], inplace=True)
        new_stats.loc[magstat_flags.index, "saturation_rate"] = magstat_flags
        new_stats.reset_index(inplace=True)
        # Get objects and store it
        objects = self.get_objects(unique_oids, engine="psql")
        objects = preprocess_objects_(
            objects, light_curves, alerts, new_stats, self.version
        )
        #         objects = self.preprocess_objects_psql(objects, light_curves)
        objects.set_index("oid", inplace=True)
        objects.loc[obj_flags.index, "diffpos"] = obj_flags["diffpos"]
        objects.loc[obj_flags.index, "reference_change"] = obj_flags[
            "reference_change"
        ]
        objects.reset_index(inplace=True)
        objects.drop(
            columns=["nearPS1", "nearZTF", "deltamjd", "ndubious"],
            inplace=True,
        )
        self.insert_objects(objects, engine="psql")

        # Store new detections
        new_detections = light_curves["detections"]["new"]
        new_detections = light_curves["detections"][new_detections]
        new_detections["step_id_corr"] = self.version
        new_detections.drop(columns=["new"], inplace=True)
        new_detections = new_detections[OLD_DET_KEYS]
        new_detections = new_detections.replace({np.nan: None})
        self.insert_detections(new_detections, engine="psql")
        # Store new non detections
        new_non_detections = light_curves["non_detections"]["new"]
        new_non_detections = light_curves["non_detections"][new_non_detections]
        new_non_detections.drop(columns=["new"], inplace=True)
        self.insert_non_detections(new_non_detections, engine="psql")
        # Store catalogs
        insert_reference(reference, self.driver)
        insert_ps1(ps1, self.driver)
        insert_magstats(new_stats, self.driver)
        insert_gaia(gaia, self.driver)
        insert_ss(ss, self.driver)
        insert_dataquality(dataquality, self.driver)

        reference = parse_metadata(reference, "reference")
        ps1 = parse_metadata(ps1, "ps1")
        gaia = parse_metadata(gaia, "gaia")
        ss = parse_metadata(ss, "ss")
        metadata = (
            reference.join(ps1, how="outer")
            .join(gaia, how="outer")
            .join(ss, how="outer")
        )
        return metadata

    def execute_mongo(
        self,
        alerts: pd.DataFrame,
        detections: pd.DataFrame,
        non_detections_prv_candidates: pd.DataFrame,
        metadata: pd.DataFrame,
    ):
        # Get unique alerce ids for get objects from database
        unique_aids = alerts["aid"].unique().tolist()
        # Concat new and old detections and non detections.
        light_curves = self.preprocess_lightcurves(
            detections, non_detections_prv_candidates
        )

        # Getting other tables: retrieve existing objects
        # and create new objects
        objects = self.get_objects(unique_aids)
        objects = self.preprocess_objects(objects, light_curves)
        # Insert new objects and update old objects on database
        self.insert_objects(objects)
        # Insert new detections and put step_version
        new_detections = light_curves["detections"]["new"]
        new_detections = light_curves["detections"][new_detections]
        new_detections["step_id_corr"] = self.version
        new_detections.drop(columns=["new"], inplace=True)
        self.insert_detections(new_detections)
        # Insert new now detections
        new_non_detections = light_curves["non_detections"]["new"]
        new_non_detections = light_curves["non_detections"][new_non_detections]
        new_non_detections.drop(columns=["new"], inplace=True)
        self.insert_non_detections(new_non_detections)
        # produce to some topic
        if self.producer:
            self.produce(alerts, objects, light_curves, metadata)
        del light_curves["detections"]
        del light_curves["non_detections"]
        del light_curves
        del objects
        del new_detections
        del new_non_detections

    def execute(self, messages):
        self.logger.info(f"Processing {len(messages)} alerts")
        alerts = pd.DataFrame(messages)
        # If is an empiric alert must has stamp
        alerts["has_stamp"] = True
        alerts["alertId"] = alerts["extra_fields"].map(lambda x: x["alertId"])
        # Process previous candidates of each alert
        (
            dets_from_prv_candidates,
            non_dets_from_prv_candidates,
        ) = self.process_prv_candidates(alerts)
        # If is an alert from previous candidate hasn't stamps
        # Concat detections from alerts and detections from previous candidates
        if dets_from_prv_candidates.empty:
            detections = alerts.copy()
            detections["parent_candid"] = np.nan
        else:
            dets_from_prv_candidates["has_stamp"] = False
            detections = pd.concat(
                [alerts, dets_from_prv_candidates], ignore_index=True
            )
        # Remove alerts with the same candid duplicated.
        # It may be the case that some candids are repeated or some
        # detections from prv_candidates share the candid.
        # We use keep='first' for maintain the candid of empiric detections.
        detections.drop_duplicates(
            "candid", inplace=True, keep="first", ignore_index=True
        )
        # Do correction to detections from stream
        detections = self.correct(detections)
        # Insert/update data on mongo and get metadata
        metadata = self.execute_psql(
            alerts.copy(),
            detections.copy(),
            non_dets_from_prv_candidates.copy(),
        )
        self.execute_mongo(
            alerts, detections, non_dets_from_prv_candidates, metadata
        )

        self.logger.info(f"Clean batch of data\n")
        del alerts
