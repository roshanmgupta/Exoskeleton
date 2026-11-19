# fatigue_analyzer.py
import time
import numpy as np
from scipy.stats import linregress
import logging
import json
import statistics

try:
    from kafka import errors as kafka_errors
    _KAFKA_TIMEOUT_ERR = kafka_errors.KafkaTimeoutError
except ImportError:
    _KAFKA_TIMEOUT_ERR = TimeoutError

from config_live import (
    MIN_MDF_POINTS_FOR_TREND,
    MDF_SLOPE_FATIGUE_THRESHOLD,
    REGRESSION_R_SQUARED_THRESHOLD,
    ALERT_COOLDOWN_SEC,
    FATIGUE_ALERT_TOPIC
)

logger = logging.getLogger(__name__)


def perform_fatigue_analysis_and_alert(
    key_tuple,
    mdf_history_data_copy,
    current_mdf_trigger_timestamp_ms,  # Timestamp of the MDF point that triggered analysis
    kafka_producer_ref,
    last_alert_dict_ref,
    lock_ref
):
    thingid, muscle_name = key_tuple

    try:
        if not mdf_history_data_copy or len(mdf_history_data_copy) < MIN_MDF_POINTS_FOR_TREND:
            logger.debug(
                f"Worker {key_tuple}: Not enough data ({len(mdf_history_data_copy)}) for trend. "
                f"Min: {MIN_MDF_POINTS_FOR_TREND}"
            )
            return None

        logger.debug(f"Worker {key_tuple}: Performing trend analysis on {len(mdf_history_data_copy)} MDF points.")

        first_mdf_ts_in_window = mdf_history_data_copy[0][0]
        last_mdf_ts_in_this_slice = mdf_history_data_copy[-1][0]
        actual_trend_window_duration_ms = last_mdf_ts_in_this_slice - first_mdf_ts_in_window

        if actual_trend_window_duration_ms < 0:
            logger.warning(
                f"Worker {key_tuple}: Trend duration negative ({actual_trend_window_duration_ms}ms). "
                f"Timestamps: {first_mdf_ts_in_window} to {last_mdf_ts_in_this_slice}. "
                f"History len: {len(mdf_history_data_copy)}. Skipping."
            )
            return None

        times_rel_sec_trend = np.array([(item[0] - first_mdf_ts_in_window) / 1000.0 for item in mdf_history_data_copy])
        mdfs_values_trend = np.array([item[1] for item in mdf_history_data_copy])

        if len(times_rel_sec_trend) < 2:
            logger.debug(f"Worker {key_tuple}: Less than 2 points for linregress.")
            return None

        slope, intercept, r_value, p_value, std_err = linregress(times_rel_sec_trend, mdfs_values_trend)

        if np.isnan(slope) or np.isnan(r_value):
            logger.warning(
                f"Worker {key_tuple}: Linreg NaN. Slope={slope}, R={r_value}. "
                f"X_data: {times_rel_sec_trend}, Y_data: {mdfs_values_trend}"
            )
            return None
        r_squared = r_value ** 2

        logger.info(
            f"Worker {key_tuple}: Trend: Slope={slope:.4f} Hz/s, R^2={r_squared:.3f}, "
            f"Pts={len(mdfs_values_trend)}, WinDur={actual_trend_window_duration_ms / 1000.0:.2f}s"
        )

        if slope < MDF_SLOPE_FATIGUE_THRESHOLD and r_squared >= REGRESSION_R_SQUARED_THRESHOLD:
            logger.info(
                f"Worker {key_tuple}: Fatigue MET for {muscle_name}! "
                f"Slope: {slope:.4f} (Thresh: {MDF_SLOPE_FATIGUE_THRESHOLD}), "
                f"R^2: {r_squared:.3f} (Thresh: {REGRESSION_R_SQUARED_THRESHOLD})"
            )
            worker_current_time_s = time.time()
            with lock_ref:
                if worker_current_time_s < last_alert_dict_ref.get(key_tuple, 0) + ALERT_COOLDOWN_SEC:
                    logger.debug(f"Worker {key_tuple}: Alert cooldown active.")
                    return None
                last_alert_dict_ref[key_tuple] = worker_current_time_s

            python_alert_emission_time_ms = int(worker_current_time_s * 1000)

            source_timestamps_of_mdf_data_in_trend = [item[3] for item in mdf_history_data_copy]
            flink_sent_timestamps_of_mdf_data_in_trend = [item[2] for item in mdf_history_data_copy]

            avg_source_ts_of_data_in_trend_overall = (
                statistics.mean(source_timestamps_of_mdf_data_in_trend)
                if source_timestamps_of_mdf_data_in_trend
                else current_mdf_trigger_timestamp_ms
            )
            avg_mdf_data_age_in_trend_ms_py = int(
                current_mdf_trigger_timestamp_ms - avg_source_ts_of_data_in_trend_overall
            )
            avg_flink_sent_ts_for_trend_mds_overall = (
                statistics.mean(flink_sent_timestamps_of_mdf_data_in_trend)
                if flink_sent_timestamps_of_mdf_data_in_trend
                else 0
            )
            flink_pipeline_delay_for_trend_data_ms = 0
            if avg_flink_sent_ts_for_trend_mds_overall > 0 and avg_source_ts_of_data_in_trend_overall > 0:
                flink_pipeline_delay_for_trend_data_ms = int(
                    avg_flink_sent_ts_for_trend_mds_overall - avg_source_ts_of_data_in_trend_overall
                )
            python_internal_processing_delay_ms = python_alert_emission_time_ms - current_mdf_trigger_timestamp_ms

            alert_payload = {
                "thingId": thingid,
                "alertTriggerTimestamp": current_mdf_trigger_timestamp_ms,
                "feedbackType": "emgMdfFatigueAlert",
                "muscle": muscle_name,
                "severity": "HIGH",
                "reason": "Decreasing MDF Trend Detected (Python Worker)",
                "mdfSlopeHzPerSec": round(float(slope), 4),
                "regressionRSquared": round(float(r_squared), 3),
                "trendWindowSecondsUsed": round(actual_trend_window_duration_ms / 1000.0, 2),
                "numberOfMdfPointsInTrend": len(mdf_history_data_copy),
                "mdfSlopeThreshold": MDF_SLOPE_FATIGUE_THRESHOLD,
                "rSquaredThreshold": REGRESSION_R_SQUARED_THRESHOLD,
                "lastMdfValueInTrend": round(float(mdfs_values_trend[-1]), 2),
                "averageMdfDataAgeInTrendMs": avg_mdf_data_age_in_trend_ms_py,
                "estimatedFlinkPipelineDelayMs": flink_pipeline_delay_for_trend_data_ms,
                "pythonProcessingTimeForAlertMs": python_internal_processing_delay_ms,
                "pythonAlertSentTimestampMs": python_alert_emission_time_ms
            }
            if kafka_producer_ref:
                kafka_producer_ref.send(FATIGUE_ALERT_TOPIC, value=alert_payload)
                kafka_producer_ref.flush(timeout=20)
                logger.warning(f"WORKER {key_tuple}: MDF FATIGUE ALERT SENT | Alert: {json.dumps(alert_payload, indent=2)}")
            else:
                logger.warning(
                    f"WORKER {key_tuple}: MDF FATIGUE ALERT (dry-run / no Kafka producer) | "
                    f"Alert: {json.dumps(alert_payload, indent=2)}"
                )
            return alert_payload

    except _KAFKA_TIMEOUT_ERR:
        logger.error(f"Worker {key_tuple}: Timeout flushing alert message to Kafka.")
    except ValueError as ve:
        logger.warning(
            f"Worker {key_tuple}: Linreg failed for {key_tuple}: {ve}. "
            f"X_data: {times_rel_sec_trend if 'times_rel_sec_trend' in locals() else 'not available'}, "
            f"Y_data: {mdfs_values_trend if 'mdfs_values_trend' in locals() else 'not available'}"
        )
    except Exception as ex_trend_worker:
        logger.error(f"Worker {key_tuple}: Unexpected error in trend analysis for {key_tuple}: {ex_trend_worker}", exc_info=True)
    return None
