import threading
import unittest
from unittest.mock import MagicMock

import numpy as np

import config_live as config
from fatigue_analyzer import perform_fatigue_analysis_and_alert
from mock_ble_streamer import generate_emg_sample


class TestConfigSanity(unittest.TestCase):
    def test_device_configs_have_consistent_channels(self):
        self.assertGreater(len(config.DEVICE_STREAMERS_CONFIG), 0)
        for dev in config.DEVICE_STREAMERS_CONFIG:
            n = dev["num_channels_expected"]
            self.assertEqual(len(dev["channel_names_from_header"]), n)
            self.assertEqual(len(dev["fatigue_channel_map"]), n)

    def test_kafka_brokers_are_non_empty_valid_strings(self):
        self.assertGreater(len(config.KAFKA_BROKERS), 0)
        for broker in config.KAFKA_BROKERS:
            self.assertNotIn("xxx", broker.lower())
            self.assertIn(":", broker)


class TestFatigueAnalyzer(unittest.TestCase):
    def _build_mdf_history(self, mdf_values, step_ms=500, start_ms=1_700_000_000_000):
        return [
            (start_ms + i * step_ms, float(val), start_ms + i * step_ms, start_ms + i * step_ms)
            for i, val in enumerate(mdf_values)
        ]

    def test_triggers_alert_on_declining_mdf_trend(self):
        # 25 points over 12s, MDF dropping from 40 Hz -> 34 Hz (-0.5 Hz/s, R^2 ~ 1.0)
        mdfs = np.linspace(40.0, 34.0, 25)
        history = self._build_mdf_history(mdfs)
        producer = MagicMock()
        last_alerts = {}
        lock = threading.Lock()

        payload = perform_fatigue_analysis_and_alert(
            ("Shirt01-Right", "deltoids_right"),
            history,
            history[-1][0],
            producer,
            last_alerts,
            lock,
        )

        self.assertIsNotNone(payload)
        self.assertEqual(payload["muscle"], "deltoids_right")
        self.assertEqual(payload["severity"], "HIGH")
        self.assertLess(payload["mdfSlopeHzPerSec"], config.MDF_SLOPE_FATIGUE_THRESHOLD)
        self.assertGreaterEqual(payload["regressionRSquared"], config.REGRESSION_R_SQUARED_THRESHOLD)
        producer.send.assert_called_once()
        producer.flush.assert_called_once()

    def test_respects_alert_cooldown(self):
        mdfs = np.linspace(40.0, 34.0, 25)
        history = self._build_mdf_history(mdfs)
        producer = MagicMock()
        last_alerts = {}
        lock = threading.Lock()

        first = perform_fatigue_analysis_and_alert(
            ("Shirt01-Right", "biceps_right"), history, history[-1][0], producer, last_alerts, lock
        )
        second = perform_fatigue_analysis_and_alert(
            ("Shirt01-Right", "biceps_right"), history, history[-1][0], producer, last_alerts, lock
        )

        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertEqual(producer.send.call_count, 1)

    def test_no_alert_on_stable_mdf(self):
        mdfs = np.full(25, 35.0)
        history = self._build_mdf_history(mdfs)
        producer = MagicMock()

        payload = perform_fatigue_analysis_and_alert(
            ("Shirt01-Right", "triceps_right"), history, history[-1][0], producer, {}, threading.Lock()
        )

        self.assertIsNone(payload)
        producer.send.assert_not_called()

    def test_no_alert_when_insufficient_points(self):
        mdfs = np.linspace(40.0, 30.0, config.MIN_MDF_POINTS_FOR_TREND - 1)
        history = self._build_mdf_history(mdfs)
        producer = MagicMock()

        payload = perform_fatigue_analysis_and_alert(
            ("Shirt01-Right", "deltoids_right"), history, history[-1][0], producer, {}, threading.Lock()
        )

        self.assertIsNone(payload)
        producer.send.assert_not_called()

    def test_dry_run_without_kafka_producer(self):
        mdfs = np.linspace(40.0, 34.0, 25)
        history = self._build_mdf_history(mdfs)

        payload = perform_fatigue_analysis_and_alert(
            ("Shirt01-Right", "deltoids_right"), history, history[-1][0], None, {}, threading.Lock()
        )

        self.assertIsNotNone(payload)
        self.assertEqual(payload["feedbackType"], "emgMdfFatigueAlert")


class TestMockEMGStreamer(unittest.TestCase):
    def _estimate_median_frequency(self, signal_1d, fs=100.0):
        centered = signal_1d - np.mean(signal_1d)
        freqs = np.fft.rfftfreq(len(centered), d=1.0 / fs)
        psd = np.abs(np.fft.rfft(centered)) ** 2
        cumulative = np.cumsum(psd)
        idx = int(np.searchsorted(cumulative, cumulative[-1] * 0.5))
        return float(freqs[min(idx, len(freqs) - 1)])

    def test_generate_emg_sample_shape_and_spectral_fatigue_shift(self):
        fs = 100.0
        rng = np.random.default_rng(42)
        samples = [
            generate_emg_sample(i, num_channels=5, sampling_rate_hz=fs, fatigue_duration_sec=20.0, rng=rng)
            for i in range(int(fs * 20))
        ]
        arr = np.asarray(samples, dtype=np.float64)
        self.assertEqual(arr.shape, (2000, 5))
        self.assertTrue(np.all(np.isfinite(arr)))

        early_mdf = self._estimate_median_frequency(arr[:200, 0], fs=fs)
        late_mdf = self._estimate_median_frequency(arr[-200:, 0], fs=fs)
        self.assertGreater(early_mdf, late_mdf + 10.0)


if __name__ == "__main__":
    unittest.main()
