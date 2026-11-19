# mock_ble_streamer.py
"""
Synthetic multi-channel EMG UDP streamer for hardware-free testing and demos.

Produces pickled per-sample float lists over UDP matching the exact packet
contract of `mcell_ble_udp_streamer.py`. Channels configured for fatigue
monitoring exhibit a realistic downward Median Frequency (MDF) shift and
slight RMS amplitude growth over time so end-to-end fatigue alerts can be
verified without physical MCell BLE hardware.
"""

import argparse
import logging
import math
import pickle
import socket
import threading
import time
from typing import List, Optional

import numpy as np

import config_live as config


def generate_emg_sample(
    sample_idx: int,
    num_channels: int,
    sampling_rate_hz: float = config.SAMPLING_RATE_HZ,
    fatigue_duration_sec: float = 30.0,
    rng: Optional[np.random.Generator] = None,
) -> List[float]:
    """
    Generate one multi-channel synthetic EMG datagroup (one float per channel).

    Simulates a band-limited muscle contraction whose dominant spectral energy
    shifts downward from ~38 Hz to ~14 Hz over `fatigue_duration_sec` while
    amplitude increases modestly—mirroring physiological EMG fatigue dynamics.
    """
    if rng is None:
        rng = np.random.default_rng()

    t = sample_idx / sampling_rate_hz
    fatigue_progress = min(1.0, t / max(fatigue_duration_sec, 1e-3))

    # Instantaneous frequency sweeps downward (38 Hz -> 14 Hz) via chirp phase
    f_start = 38.0
    f_end = 14.0
    if t <= fatigue_duration_sec:
        k = (f_end - f_start) / max(fatigue_duration_sec, 1e-3)
        phase = 2.0 * math.pi * (f_start * t + 0.5 * k * t * t)
    else:
        phase_end = 2.0 * math.pi * (f_start * fatigue_duration_sec + 0.5 * (f_end - f_start) * fatigue_duration_sec)
        phase = phase_end + 2.0 * math.pi * f_end * (t - fatigue_duration_sec)

    base_amplitude = 45.0 * (1.0 + 0.6 * fatigue_progress)
    values: List[float] = []
    for ch in range(num_channels):
        ch_offset = ch * 0.7
        # Primary motor-unit carrier + harmonic + physiological broadband noise
        carrier = math.sin(phase + ch_offset) + 0.35 * math.sin(1.35 * phase + ch_offset * 1.3)
        noise = float(rng.normal(0.0, 6.0))
        val = 512.0 + base_amplitude * carrier + noise
        values.append(float(val))
    return values


def run_mock_streamer_process(
    device_name: str,
    num_channels: int,
    udp_host: str,
    udp_port: int,
    sampling_rate_hz: float = config.SAMPLING_RATE_HZ,
    run_duration_sec: Optional[float] = None,
    fatigue_duration_sec: float = 30.0,
    stop_event_mp=None,
) -> None:
    """Target function for `multiprocessing.Process` or standalone execution."""
    logger = logging.getLogger(f"MockStreamer.{device_name}")
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rng = np.random.default_rng(seed=abs(hash(device_name)) % (2**32))

    period_sec = 1.0 / sampling_rate_hz
    sample_idx = 0
    start_time = time.monotonic()

    logger.info(
        f"Starting synthetic EMG stream for {device_name} ({num_channels} ch @ {sampling_rate_hz:.0f} Hz) "
        f"-> udp://{udp_host}:{udp_port}"
    )

    try:
        while True:
            if stop_event_mp and stop_event_mp.is_set():
                logger.info("Stop event received. Ending mock stream.")
                break
            elapsed = time.monotonic() - start_time
            if run_duration_sec is not None and elapsed >= run_duration_sec:
                logger.info(f"Reached run duration ({run_duration_sec}s). Stopping mock stream.")
                break

            datagroup = generate_emg_sample(
                sample_idx=sample_idx,
                num_channels=num_channels,
                sampling_rate_hz=sampling_rate_hz,
                fatigue_duration_sec=fatigue_duration_sec,
                rng=rng,
            )
            udp_socket.sendto(pickle.dumps(datagroup), (udp_host, udp_port))
            sample_idx += 1

            if sample_idx % int(sampling_rate_hz * 5) == 0:
                logger.info(f"Sent {sample_idx} synthetic datagroups (ch0={datagroup[0]:.2f}).")

            next_tick = start_time + sample_idx * period_sec
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        logger.info("Mock streamer interrupted.")
    finally:
        udp_socket.close()
        logger.info(f"Mock streamer for {device_name} stopped after {sample_idx} samples.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic multi-channel EMG UDP streamer for local testing.")
    parser.add_argument("--duration", type=float, default=35.0, help="Duration in seconds (default: 35s).")
    parser.add_argument("--fatigue-ramp", type=float, default=25.0, help="Seconds over which simulated MDF declines.")
    args = parser.parse_args()

    logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
    stop_event = threading.Event()
    threads = []

    for dev in config.DEVICE_STREAMERS_CONFIG:
        t = threading.Thread(
            target=run_mock_streamer_process,
            args=(
                dev["name"],
                dev["num_channels_expected"],
                config.UDP_STREAM_HOST,
                dev["udp_port"],
                config.SAMPLING_RATE_HZ,
                args.duration,
                args.fatigue_ramp,
                stop_event,
            ),
            name=f"MockStreamer-{dev['name']}",
        )
        threads.append(t)
        t.start()

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        stop_event.set()
        for t in threads:
            t.join(timeout=3)


if __name__ == "__main__":
    main()
