import argparse
import logging
import time
import threading
import multiprocessing
import numpy as np
import pickle
import socket
import json
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
import signal
from datetime import datetime, timezone
import queue

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Matplotlib for plotting
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# LibEMG imports
try:
    from libemg.filtering import Filter
    from libemg.feature_extractor import FeatureExtractor
except ImportError as e:
    print(f"Critical Error: Failed to import LibEMG modules. Ensure LibEMG is installed. Details: {e}")
    exit()

import config_live as config # Your configuration file
from fatigue_analyzer import perform_fatigue_analysis_and_alert # Your fatigue logic
# Assuming mcell_ble_udp_streamer.py contains run_streamer_process and is importable
from mcell_ble_udp_streamer import run_streamer_process
from mock_ble_streamer import run_mock_streamer_process
# NEW: Import the raw EMG broker client
from raw_emg_broker_client import RawEMGBrokerClient


# --- Global Objects & Setup ---
logger = logging.getLogger("LibEMGMainProcessor")
kafka_alert_producer = None
kafka_features_producer = None
# NEW: Global for raw data streamer client
raw_emg_broker_client = None


mdf_history = defaultdict(lambda: deque(maxlen=1)) # Actual maxlen set in main()
last_alert_time_mdf = defaultdict(float)
last_alert_time_mdf_lock = threading.Lock()
fatigue_analysis_executor = None
main_stop_event = multiprocessing.Event() # For signaling streamer processes
thread_stop_event = threading.Event() # For signaling processor threads

# Queues for plotting data from specific device
raw_data_plot_queue = queue.Queue(maxsize=100)
feature_plot_queue = queue.Queue(maxsize=100)

# Plotting globals
plot_fig = None
plot_axs = {}
plot_lines_raw = {}
plot_lines_features = {}
plot_data_buffers_raw = {}
plot_data_buffers_features = {}
plot_time_buffer_raw = None # Initialized in setup_plots
plot_time_buffer_features = None # Initialized in setup_plots

# --- LibEMG Related Calculated Parameters (Initialized in main) ---
LIBEMG_WINDOW_SAMPLES = 0
LIBEMG_WINDOW_HOP_SAMPLES = 0
FEATURES_PER_SEC_PER_CHANNEL = 0 # Renamed from MDFS_PER_SEC_PER_CHANNEL for generality

def initialize_kafka_producers():
    global kafka_alert_producer, kafka_features_producer
    try:
        common_kwargs = {
            "bootstrap_servers": config.KAFKA_BROKERS,
            "value_serializer": lambda v: json.dumps(v).encode('utf-8'),
            "retries": 3, "request_timeout_ms": 10000, "linger_ms": 100, "batch_size": 16384*2,
        }
        kafka_alert_producer = KafkaProducer(**common_kwargs)
        logger.info(f"KafkaProducer for alerts (topic: {config.FATIGUE_ALERT_TOPIC}) initialized.")
        kafka_features_producer = KafkaProducer(**common_kwargs)
        logger.info(f"KafkaProducer for features (topic: {config.FEATURES_VISUALIZATION_TOPIC}) initialized.")
        return True
    except KafkaError as e:
        logger.error(f"Failed to initialize KafkaProducers: {e}", exc_info=True)
        return False
    except Exception as e: # Catch other potential errors like misconfiguration
        logger.error(f"Generic error initializing KafkaProducers: {e}", exc_info=True)
        return False

# NEW: Function to initialize the raw EMG data streamer client
def initialize_raw_emg_streamer():
    global raw_emg_broker_client
    if config.RAW_EMG_STREAMER.get("ENABLE"):
        broker_type = config.RAW_EMG_STREAMER.get("BROKER_TYPE")
        if broker_type == 'kafka':
            broker_config = config.RAW_EMG_STREAMER.get("KAFKA_RAW_CONFIG")
        elif broker_type == 'mqtt':
            broker_config = config.RAW_EMG_STREAMER.get("MQTT_RAW_CONFIG")
        else:
            logger.error(f"Invalid broker type '{broker_type}' in config.RAW_EMG_STREAMER.")
            return False
        
        if broker_config:
            try:
                raw_emg_broker_client = RawEMGBrokerClient(broker_type, broker_config)
                logger.info(f"RawEMGBrokerClient initialized for broker type: {broker_type}")
                return True
            except Exception as e:
                logger.error(f"Failed to initialize RawEMGBrokerClient: {e}", exc_info=True)
                return False
    else:
        logger.info("Raw EMG data streamer is disabled in config.")
        return True # Not an error, just disabled


def device_data_processor_thread(device_id_str: str, device_config_dict: dict,
                                 mvc_values: dict, stop_event: threading.Event):
    thread_logger = logging.getLogger(f"Processor.{device_id_str}")
    udp_port = device_config_dict["udp_port"]
    num_channels = device_config_dict["num_channels_expected"]
    channel_names_ordered = device_config_dict.get("channel_names_from_header", [f"Ch{i+1}" for i in range(num_channels)])
    fatigue_channel_map = device_config_dict.get("fatigue_channel_map", {})

    thread_logger.info(f"Initializing for UDP {config.UDP_STREAM_HOST}:{udp_port}, {num_channels} channels.")

    filter_set = None
    if config.APPLY_LIBEMG_FILTERING:
        try:
            filter_set = Filter(sampling_frequency=config.SAMPLING_RATE_HZ)
            for f_conf in config.LIBEMG_FILTER_CONFIGS:
                # Ensure 'fs' is NOT in f_conf as Filter() already has it.
                # LibEMG's install_filters takes a single dictionary.
                clean_f_conf = {k: v for k, v in f_conf.items() if k != 'fs'}
                filter_set.install_filters(filter_dictionary=clean_f_conf)
            thread_logger.info("LibEMG Filters initialized and installed.")
        except Exception as e_filter_init:
            thread_logger.error(f"Failed to initialize/install LibEMG Filter: {e_filter_init}", exc_info=True)
            filter_set = None

    feature_extractor = FeatureExtractor()
    thread_logger.info(f"LibEMG FeatureExtractor for: {config.FEATURES_TO_EXTRACT}.")

    device_mvc_references = {}
    if mvc_values:
        for ch_idx, original_ch_name in enumerate(channel_names_ordered):
            # Check against generic muscle name or device-prefixed key (as saved by mvc_calibration_tool_live.py)
            monitored_muscle_name = fatigue_channel_map.get(ch_idx)
            if monitored_muscle_name:
                prefixed_key = f"{device_id_str}_{monitored_muscle_name}"
                if prefixed_key in mvc_values:
                    device_mvc_references[monitored_muscle_name] = mvc_values[prefixed_key]
                elif monitored_muscle_name in mvc_values:
                    device_mvc_references[monitored_muscle_name] = mvc_values[monitored_muscle_name]

            if original_ch_name in mvc_values: # If MVCs are keyed by original channel names
                 device_mvc_references[original_ch_name] = mvc_values[original_ch_name]


    thread_logger.info(f"Using MVC References for this device: {device_mvc_references}")

    datagroup_buffer = deque()
    samples_accumulated_for_hop = 0

    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        udp_socket.bind((config.UDP_STREAM_HOST, udp_port))
        udp_socket.settimeout(0.2)
        thread_logger.info("UDP Listener bound.")
    except Exception as e:
        thread_logger.error(f"Failed to bind UDP socket: {e}", exc_info=True)
        return

    while not stop_event.is_set():
        try:
            pickled_data, _ = udp_socket.recvfrom(4096)
            if pickled_data:
                datagroup_floats = pickle.loads(pickled_data)
                current_sample_timestamp = datetime.now(timezone.utc)
                
                # NEW: Send raw data to the broker if enabled
                if raw_emg_broker_client and config.RAW_EMG_STREAMER.get("ENABLE"):
                    raw_emg_broker_client.send_data(
                        thingid=device_id_str,
                        timestamp_iso=current_sample_timestamp.isoformat(),
                        channel_map=fatigue_channel_map, # Using fatigue_channel_map for JSON keys
                        data_values=datagroup_floats
                    )
                
                global raw_data_plot_queue # Queue for raw data plotting
                if isinstance(datagroup_floats, list) and len(datagroup_floats) == num_channels:
                    # ### VISUALIZATION CODE START ###
                    current_sample_timestamp_ms = int(current_sample_timestamp.timestamp() * 1000)

                    if config.ENABLE_LIVE_VISUALIZATION and device_id_str == config.VISUALIZATION_DEVICE_NAME_TARGET:
                        try:
                            raw_data_plot_queue.put_nowait((current_sample_timestamp_ms, list(datagroup_floats))) # Send a copy
                        except queue.Full:
                            thread_logger.warning("Raw data plot queue full for plotting. Discarding.")
    				# ### VISUALIZATION CODE END ###
                    datagroup_buffer.append(datagroup_floats)
                    samples_accumulated_for_hop += 1

                    if len(datagroup_buffer) >= LIBEMG_WINDOW_SAMPLES and \
                       samples_accumulated_for_hop >= LIBEMG_WINDOW_HOP_SAMPLES:

                        current_window_list = list(datagroup_buffer)[-LIBEMG_WINDOW_SAMPLES:]
                        window_s_by_c_np = np.array(current_window_list, dtype=np.float32) # Shape (WindowSamples, Channels)
                        window_timestamp_ms = int(time.time() * 1000)

                        samples_accumulated_for_hop = 0 # Reset hop counter

                        window_to_process_s_by_c = window_s_by_c_np
                        if config.APPLY_LIBEMG_FILTERING and filter_set:
                            try:
                                window_filtered_s_by_c = filter_set.filter(window_s_by_c_np) # Filter expects (Samples, Channels)
                                window_to_process_s_by_c = window_filtered_s_by_c
                            except Exception as e_filter:
                                thread_logger.error(f"Error filtering window: {e_filter}. Using unfiltered.")

                        # FeatureExtractor expects a list of windows, each (Channels, Samples)
                        window_to_process_c_by_s = window_to_process_s_by_c.T

                        feature_params_dict = {}
                        # Add sampling frequency for spectral features that require it via feature_dic
                        # Based on LibEMG FeatureExtractor docs for get<FeatureName>feat methods
                        if "MDF" in config.FEATURES_TO_EXTRACT: feature_params_dict["MDF_fs"] = config.SAMPLING_RATE_HZ
                        if "MNF" in config.FEATURES_TO_EXTRACT: feature_params_dict["MNF_fs"] = config.SAMPLING_RATE_HZ
                        if "DFTR" in config.FEATURES_TO_EXTRACT: feature_params_dict["DFTR_fs"] = config.SAMPLING_RATE_HZ
                        if "SM" in config.FEATURES_TO_EXTRACT: feature_params_dict["SM_fs"] = config.SAMPLING_RATE_HZ
                        if "WENG" in config.FEATURES_TO_EXTRACT: feature_params_dict["WENG_fs"] = config.SAMPLING_RATE_HZ
                        if "WENT" in config.FEATURES_TO_EXTRACT: feature_params_dict["WENT_fs"] = config.SAMPLING_RATE_HZ
                        if "WV" in config.FEATURES_TO_EXTRACT: feature_params_dict["WV_fs"] = config.SAMPLING_RATE_HZ
                        if "WWL" in config.FEATURES_TO_EXTRACT: feature_params_dict["WWL_fs"] = config.SAMPLING_RATE_HZ


                        # Reshape the single window (C, W) into a 3D batch of one window (1, C, W)
                        window_for_libemg_fe = window_to_process_c_by_s.reshape(
                            1,  # Number of windows in this batch
                            window_to_process_c_by_s.shape[0],  # Number of channels
                            window_to_process_c_by_s.shape[1]   # Number of samples in window
                        )

                        extracted_features = feature_extractor.extract_features(
                            config.FEATURES_TO_EXTRACT,
                            window_for_libemg_fe,  # <--- Pass the 3D NumPy array directly
                            feature_dic=feature_params_dict
                        )

                        current_window_features_per_channel = {}
                        for feat_name in config.FEATURES_TO_EXTRACT:
                            if feat_name in extracted_features and extracted_features[feat_name].shape[0] > 0:
                                current_window_features_per_channel[feat_name] = extracted_features[feat_name][0]
                            else:
                                current_window_features_per_channel[feat_name] = np.full(num_channels, np.nan)

                        process_libemg_features(
                            current_window_features_per_channel, window_timestamp_ms,
                            device_id_str, channel_names_ordered,
                            fatigue_channel_map, device_mvc_references
                        )

                        # Slide the main buffer by removing HOP samples from the left
                        for _ in range(LIBEMG_WINDOW_HOP_SAMPLES):
                            if datagroup_buffer: datagroup_buffer.popleft()
                            else: break
                else:
                    logger.warning(f"UDP: Unpickled data not list or wrong channel count. Type: {type(datagroup_floats)}, Len: {len(datagroup_floats) if hasattr(datagroup_floats, '__len__') else 'N/A'}")
        except socket.timeout:
            continue
        except pickle.UnpicklingError as pe: logger.error(f"UDP UnpicklingError: {pe}")
        except Exception as e: thread_logger.error(f"Error in processor loop: {e}", exc_info=True); time.sleep(0.1)

    thread_logger.info("Stopping UDP listener and cleaning up.")
    udp_socket.close()

def process_libemg_features(features_per_channel_dict, window_timestamp_ms,
                            device_id_str, channel_names_ordered_list,
                            fatigue_channel_mapping, mvc_refs_for_this_device):
    global mdf_history, kafka_features_producer, kafka_alert_producer, \
           last_alert_time_mdf, last_alert_time_mdf_lock, fatigue_analysis_executor

    num_channels = len(channel_names_ordered_list)
    kafka_payload_base = {
        "thingId": device_id_str,
        "participant": config.PARTICIPANT_ID,
        "timestamp_utc": datetime.fromtimestamp(window_timestamp_ms / 1000.0, timezone.utc).isoformat(),
        "sourceTimestampMs": window_timestamp_ms,
        "channels_features": {}
    } 

    for ch_idx in range(num_channels):
        # Use original channel name parsed from header (via config) as the primary key for features
        channel_name_key = channel_names_ordered_list[ch_idx]
        channel_features_dict_for_kafka = {"original_ch_name": channel_name_key}

        # Populate features for Kafka payload
        for feat_name in config.FEATURES_TO_EXTRACT:
            # features_per_channel_dict[feat_name] is np.array([val_ch1, val_ch2, ...])
            if feat_name in features_per_channel_dict:
                feat_val = features_per_channel_dict[feat_name][ch_idx]
                if feat_val is not None and not np.isnan(feat_val):
                    channel_features_dict_for_kafka[feat_name] = round(feat_val, 4)

        # MVC Normalization for RMS
        if "RMS" in channel_features_dict_for_kafka:
            rms_val = channel_features_dict_for_kafka["RMS"]
            # MVCs are keyed by generic muscle name (e.g., "deltoids_right") or device-specific original name
            monitored_muscle_name_for_mvc = fatigue_channel_mapping.get(ch_idx, channel_name_key) # Fallback to original name for MVC lookup

            mvc_reference_val = mvc_refs_for_this_device.get(monitored_muscle_name_for_mvc)
            if mvc_reference_val is None and channel_name_key != monitored_muscle_name_for_mvc: # Try original name if mapped one failed
                 mvc_reference_val = mvc_refs_for_this_device.get(channel_name_key)

            if mvc_reference_val is not None and mvc_reference_val > 1e-6:
                channel_features_dict_for_kafka["RMS_percent_MVC"] = round((rms_val / mvc_reference_val) * 100, 2)

        kafka_payload_base["channels_features"][channel_name_key] = channel_features_dict_for_kafka

        # Fatigue Analysis for specific monitored muscles
        monitored_muscle_name = fatigue_channel_mapping.get(ch_idx)
        if monitored_muscle_name and monitored_muscle_name in config.MUSCLES_TO_MONITOR_FOR_FATIGUE:
            fatigue_key = (device_id_str, monitored_muscle_name)
            mdf_val_for_fatigue = features_per_channel_dict.get("MDF", np.full(num_channels, np.nan))[ch_idx]

            if mdf_val_for_fatigue is not None and not np.isnan(mdf_val_for_fatigue):
                mdf_history[fatigue_key].append((window_timestamp_ms, mdf_val_for_fatigue, window_timestamp_ms, window_timestamp_ms))
                cutoff_ts = window_timestamp_ms - (config.TREND_WINDOW_SEC * 1000)
                while mdf_history[fatigue_key] and mdf_history[fatigue_key][0][0] < cutoff_ts:
                    mdf_history[fatigue_key].popleft()

                current_time_s = time.time()
                submit_fatigue = False
                with last_alert_time_mdf_lock:
                    if current_time_s >= last_alert_time_mdf.get(fatigue_key, 0) + config.ALERT_COOLDOWN_SEC:
                        if len(mdf_history[fatigue_key]) >= config.MIN_MDF_POINTS_FOR_TREND:
                            submit_fatigue = True
                if submit_fatigue:
                    history_copy = list(mdf_history[fatigue_key])
                    fatigue_analysis_executor.submit(
                        perform_fatigue_analysis_and_alert,
                        fatigue_key, history_copy, window_timestamp_ms,
                        kafka_alert_producer, last_alert_time_mdf, last_alert_time_mdf_lock)
    global feature_plot_queue
    # ### VISUALIZATION CODE START ###
    features_for_plotting_this_window = {}
    num_channels = len(channel_names_ordered_list) # Get num_channels from passed list

    for ch_idx in range(num_channels):
        channel_name = channel_names_ordered_list[ch_idx]
        # Check if this channel and its features are targeted for plotting
        if device_id_str == config.VISUALIZATION_DEVICE_NAME_TARGET and \
           ch_idx in config.VISUALIZATION_FEATURE_CHANNELS_INDICES:
            for feat_name in config.VISUALIZATION_FEATURES_TO_PLOT:
                if feat_name in features_per_channel_dict: # Check if feature exists for this channel set
                    # features_per_channel_dict[feat_name] is an array of (num_channels)
                    if ch_idx < len(features_per_channel_dict[feat_name]):
                        feat_val = features_per_channel_dict[feat_name][ch_idx]
                        if feat_val is not None and not np.isnan(feat_val):
                            line_key = f"{channel_name}_{feat_name}"
                            features_for_plotting_this_window[line_key] = round(feat_val, 4)

    if config.ENABLE_LIVE_VISUALIZATION and device_id_str == config.VISUALIZATION_DEVICE_NAME_TARGET and features_for_plotting_this_window:
        try:
            feature_plot_queue.put_nowait((window_timestamp_ms, features_for_plotting_this_window))
        except queue.Full:
            logging.getLogger(f"Processor.{device_id_str}").warning(f"Feature plot queue full for {device_id_str}. Discarding.")
    # ### VISUALIZATION CODE END ###

    if kafka_features_producer and kafka_payload_base["channels_features"]:
        try:
            kafka_features_producer.send(config.FEATURES_VISUALIZATION_TOPIC, value=kafka_payload_base)
        except Exception as e_feat_viz:
            logger.error(f"Error sending aggregated features to viz topic for {device_id_str}: {e_feat_viz}")

def load_mvc_values(filepath=config.MVC_VALUES_FILE):
    # (Same as your provided version)
    try:
        with open(filepath, 'r') as f: mvc_data = json.load(f)
        logger.info(f"Loaded MVC values from {filepath}: {mvc_data}")
        return mvc_data
    except FileNotFoundError: logger.warning(f"MVC file {filepath} not found."); return {}
    except Exception as e: logger.error(f"Error loading MVC values: {e}"); return {}

# ### VISUALIZATION CODE START ###
def setup_plots():
    global plot_fig, plot_axs, plot_lines_raw, plot_lines_features, \
           plot_data_buffers_raw, plot_data_buffers_features, \
           plot_time_buffer_raw, plot_time_buffer_features, FEATURES_PER_SEC_PER_CHANNEL

    num_raw_channels_to_plot = len(config.VISUALIZATION_RAW_CHANNELS_INDICES)

    feature_plot_details = []
    if config.VISUALIZATION_FEATURES_TO_PLOT and config.VISUALIZATION_FEATURE_CHANNELS_INDICES:
        vis_dev_conf = next((d for d in config.DEVICE_STREAMERS_CONFIG if d["name"] == config.VISUALIZATION_DEVICE_NAME_TARGET), None)
        if vis_dev_conf:
            for feat_name in config.VISUALIZATION_FEATURES_TO_PLOT:
                for ch_idx in config.VISUALIZATION_FEATURE_CHANNELS_INDICES:
                    if ch_idx < len(vis_dev_conf["channel_names_from_header"]):
                        ch_name = vis_dev_conf["channel_names_from_header"][ch_idx]
                        line_key = f"{ch_name}_{feat_name}"
                        feature_plot_details.append({"name": feat_name, "channel_index": ch_idx, "channel_name": ch_name, "line_key": line_key})

    num_feature_lines_to_plot = len(feature_plot_details)

    # Determine number of subplots: one for each raw channel, one for all features
    num_subplots = num_raw_channels_to_plot + (1 if num_feature_lines_to_plot > 0 else 0)

    if num_subplots == 0:
        logger.info("No channels or features configured for live plotting.")
        return False

    plot_fig, all_axes = plt.subplots(num_subplots, 1, sharex=False, figsize=(12, 3 * num_subplots))
    if num_subplots == 1 and not isinstance(all_axes, np.ndarray):
        all_axes = [all_axes]
    
    plot_axs['raw'] = []
    current_axis_idx = 0

    plot_time_buffer_raw = deque(maxlen=int(config.PLOT_WINDOW_DURATION_SEC * config.SAMPLING_RATE_HZ))

    if num_raw_channels_to_plot > 0:
        vis_dev_conf = next((d for d in config.DEVICE_STREAMERS_CONFIG if d["name"] == config.VISUALIZATION_DEVICE_NAME_TARGET), None)
        if vis_dev_conf:
            for i, ch_idx in enumerate(config.VISUALIZATION_RAW_CHANNELS_INDICES):
                if ch_idx < len(vis_dev_conf["channel_names_from_header"]):
                    ch_name = vis_dev_conf["channel_names_from_header"][ch_idx]
                    ax_raw = all_axes[current_axis_idx]
                    plot_axs['raw'].append(ax_raw)
                    line, = ax_raw.plot([], [], lw=1, label=f"{ch_name} (Raw)")
                    plot_lines_raw[ch_idx] = line # Key by channel index
                    plot_data_buffers_raw[ch_idx] = deque(maxlen=int(config.PLOT_WINDOW_DURATION_SEC * config.SAMPLING_RATE_HZ))
                    ax_raw.set_title(f"Raw EMG: {ch_name} ({config.VISUALIZATION_DEVICE_NAME_TARGET})")
                    ax_raw.set_ylabel("Amplitude")
                    ax_raw.legend(loc='upper left', fontsize='small')
                    ax_raw.grid(True)
                    current_axis_idx += 1

    plot_time_buffer_features = deque(maxlen=int(config.PLOT_WINDOW_DURATION_SEC * (FEATURES_PER_SEC_PER_CHANNEL if FEATURES_PER_SEC_PER_CHANNEL > 0 else 2)))

    if num_feature_lines_to_plot > 0:
        ax_feat = all_axes[current_axis_idx]
        plot_axs['features'] = ax_feat
        for plot_detail in feature_plot_details:
            line_key = plot_detail["line_key"]
            line, = ax_feat.plot([], [], lw=1.5, label=line_key)
            plot_lines_features[line_key] = line
            plot_data_buffers_features[line_key] = deque(maxlen=int(config.PLOT_WINDOW_DURATION_SEC * (FEATURES_PER_SEC_PER_CHANNEL if FEATURES_PER_SEC_PER_CHANNEL > 0 else 2)))
        ax_feat.set_title(f"Extracted Features ({config.VISUALIZATION_DEVICE_NAME_TARGET})")
        ax_feat.set_ylabel("Feature Value")
        ax_feat.set_xlabel("Time (s relative to plot start)")
        ax_feat.legend(loc='upper left', fontsize='small')
        ax_feat.grid(True)

    plot_fig.tight_layout()
    plt.ion()
    plt.show(block=False)
    return True

def update_plots(frame_num):
    global raw_data_plot_queue, feature_plot_queue, plot_lines_raw, plot_lines_features, \
           plot_data_buffers_raw, plot_data_buffers_features, \
           plot_time_buffer_raw, plot_time_buffer_features, plot_axs

    # Update raw data plot
    new_raw_data_received = False
    while not raw_data_plot_queue.empty():
        try:
            ts, datagroup = raw_data_plot_queue.get_nowait()
            plot_time_buffer_raw.append(ts / 1000.0)
            for ch_idx_to_plot in config.VISUALIZATION_RAW_CHANNELS_INDICES:
                if ch_idx_to_plot in plot_data_buffers_raw and ch_idx_to_plot < len(datagroup):
                    plot_data_buffers_raw[ch_idx_to_plot].append(datagroup[ch_idx_to_plot])
            new_raw_data_received = True
        except queue.Empty: break

    if new_raw_data_received and plot_axs.get('raw') and plot_time_buffer_raw:
        min_time_raw = plot_time_buffer_raw[0]
        relative_time_raw = [t - min_time_raw for t in plot_time_buffer_raw]
        for i, ch_idx in enumerate(config.VISUALIZATION_RAW_CHANNELS_INDICES):
            if ch_idx in plot_lines_raw and plot_axs['raw'][i]: # Check if axis exists
                plot_lines_raw[ch_idx].set_data(relative_time_raw[-len(plot_data_buffers_raw[ch_idx]):], list(plot_data_buffers_raw[ch_idx]))
                plot_axs['raw'][i].relim()
                plot_axs['raw'][i].autoscale_view(True, True, True)

    # Update features plot
    new_feature_data_received = False
    latest_feature_ts_for_update = None
    features_batch_for_update = defaultdict(list)

    while not feature_plot_queue.empty():
        try:
            ts, features_for_window = feature_plot_queue.get_nowait()
            latest_feature_ts_for_update = ts / 1000.0
            for line_key, value in features_for_window.items():
                if line_key in plot_data_buffers_features:
                    features_batch_for_update[line_key].append(value) # Collect all values
            new_feature_data_received = True
        except queue.Empty: break

    if new_feature_data_received and latest_feature_ts_for_update is not None and plot_axs.get('features'):
        # For simplicity, append only the latest timestamp for this batch,
        # and append all values received for each feature.
        # This assumes feature_plot_queue items for one 'update_plots' call are somewhat co-timed.
        plot_time_buffer_features.append(latest_feature_ts_for_update)

        for line_key, new_values_list in features_batch_for_update.items():
            plot_data_buffers_features[line_key].extend(new_values_list) # Extend with all values

        min_time_feat = plot_time_buffer_features[0] if plot_time_buffer_features else 0
        relative_time_feat_full = [t - min_time_feat for t in plot_time_buffer_features]

        for line_key, line_obj in plot_lines_features.items():
            y_data = list(plot_data_buffers_features[line_key])
            # Ensure x and y data have compatible lengths for plotting
            # We need one x-time per y-value. The current logic adds one timestamp per batch of feature updates.
            # This needs refinement if multiple feature sets arrive between plot updates.
            # Simplest for now: plot all y_data against all available relative_time_feat points,
            # ensuring the x-axis buffer for features also gets populated per feature set if possible.
            x_data_len = len(y_data)
            if x_data_len > len(relative_time_feat_full): # Should not happen if one ts per update batch
                x_data_for_line = relative_time_feat_full
            else:
                x_data_for_line = relative_time_feat_full[-x_data_len:]

            if x_data_for_line and y_data:
                line_obj.set_data(x_data_for_line, y_data)

        plot_axs['features'].relim()
        plot_axs['features'].autoscale_view(True, True, True)

    artists = list(plot_lines_raw.values()) + list(plot_lines_features.values())
    return artists # Return all artists that were updated


def parse_args():
    parser = argparse.ArgumentParser(
        description="Real-time EMG signal processing, fatigue detection, and Kafka/MQTT streaming pipeline."
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use synthetic EMG UDP streamers (mock_ble_streamer.py) instead of physical MCell BLE hardware."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without connecting to Kafka/MQTT brokers (logs extracted fatigue alerts locally)."
    )
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Enable live Matplotlib visualization (overrides config_live.ENABLE_LIVE_VISUALIZATION)."
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Optional run duration in seconds before graceful shutdown."
    )
    return parser.parse_args()


def main():
    global fatigue_analysis_executor, mdf_history, main_stop_event, thread_stop_event
    global LIBEMG_WINDOW_SAMPLES, LIBEMG_WINDOW_HOP_SAMPLES, FEATURES_PER_SEC_PER_CHANNEL
    global plot_fig # Make sure plot_fig is accessible for FuncAnimation and cleanup

    args = parse_args()
    if args.visualize:
        config.ENABLE_LIVE_VISUALIZATION = True

    logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)
    mode_label = "MOCK" if args.mock else "LIVE BLE"
    broker_label = "DRY-RUN (no brokers)" if args.dry_run else "KAFKA/MQTT"
    logger.info(f"--- LibEMG Processor Starting [{mode_label} | {broker_label}] ---")

    LIBEMG_WINDOW_SAMPLES = int(config.LIBEMG_WINDOW_LENGTH_SEC * config.SAMPLING_RATE_HZ)
    LIBEMG_WINDOW_HOP_SAMPLES = int(LIBEMG_WINDOW_SAMPLES * (1.0 - config.LIBEMG_WINDOW_SLIDE_PERCENT))
    if LIBEMG_WINDOW_HOP_SAMPLES <= 0: LIBEMG_WINDOW_HOP_SAMPLES = 1
    FEATURES_PER_SEC_PER_CHANNEL = config.SAMPLING_RATE_HZ / LIBEMG_WINDOW_HOP_SAMPLES if LIBEMG_WINDOW_HOP_SAMPLES > 0 else 0
    logger.info(f"Feature Extraction: Win={LIBEMG_WINDOW_SAMPLES}smp, Hop={LIBEMG_WINDOW_HOP_SAMPLES}smp, ~{FEATURES_PER_SEC_PER_CHANNEL:.2f} sets/sec/ch.")

    mdf_history_maxlen_calc = int(config.TREND_WINDOW_SEC * FEATURES_PER_SEC_PER_CHANNEL if FEATURES_PER_SEC_PER_CHANNEL > 0 else config.MIN_MDF_POINTS_FOR_TREND * 2)
    if mdf_history_maxlen_calc < config.MIN_MDF_POINTS_FOR_TREND: mdf_history_maxlen_calc = config.MIN_MDF_POINTS_FOR_TREND * 2
    logger.info(f"MDF history max length: {mdf_history_maxlen_calc} points.")
    mdf_history = defaultdict(lambda: deque(maxlen=mdf_history_maxlen_calc))

    # Initialize all brokers (skipped in --dry-run mode)
    if not args.dry_run:
        if not initialize_kafka_producers(): return
        if not initialize_raw_emg_streamer(): return
    else:
        logger.info("Dry-run mode enabled: skipping Kafka and MQTT broker initialization.")
    
    mvc_references = load_mvc_values()
    fatigue_analysis_executor = ThreadPoolExecutor(max_workers=config.FATIGUE_ANALYSIS_WORKERS, thread_name_prefix='FatigueWork')

    streamer_processes = []
    processor_threads = []
    signal.signal(signal.SIGINT, lambda s, f: (logger.info("SIGINT received by main."), main_stop_event.set(), thread_stop_event.set()))
    signal.signal(signal.SIGTERM, lambda s, f: (logger.info("SIGTERM received by main."), main_stop_event.set(), thread_stop_event.set()))

    for dev_conf in config.DEVICE_STREAMERS_CONFIG:
        if args.mock:
            logger.info(f"Starting MOCK synthetic EMG UDP streamer for {dev_conf['name']}...")
            p = threading.Thread(
                target=run_mock_streamer_process,
                args=(
                    dev_conf["name"],
                    dev_conf["num_channels_expected"],
                    config.UDP_STREAM_HOST,
                    dev_conf["udp_port"],
                    config.SAMPLING_RATE_HZ,
                    args.duration,
                    25.0,
                    thread_stop_event,
                ),
                name=f"MockStreamer-{dev_conf['name']}",
                daemon=True,
            )
        else:
            logger.info(f"Starting UDP streamer process for {dev_conf['name']}...")
            p = multiprocessing.Process(target=run_streamer_process,
                                        args=(dev_conf["ble_address"], dev_conf["name"], config.MUSCLE_SERVICE_CHAR_UUID, config.COMMAND_DICT,
                                              config.UDP_STREAM_HOST, dev_conf["udp_port"], config.BLE_HEADER_PARSE_TIMEOUT_SECONDS,
                                              args.duration if args.duration is not None else config.BLE_RUN_DURATION_SECONDS, main_stop_event),
                                        name=f"Streamer-{dev_conf['name']}")
        streamer_processes.append(p)
        p.start()
        time.sleep(0.05 if args.mock else 1) # Stagger streamer starts

    for device_config_entry in config.DEVICE_STREAMERS_CONFIG:
        device_identifier = device_config_entry["name"]
        thread = threading.Thread(target=device_data_processor_thread,
                                  args=(device_identifier, device_config_entry, mvc_references, thread_stop_event),
                                  name=f"Processor-{device_identifier}")
        processor_threads.append(thread)
        thread.start()

    logger.info(f"Started {len(processor_threads)} data processor threads.")

    animation_obj = None # Store the animation object to keep it alive
    if config.ENABLE_LIVE_VISUALIZATION:
        if setup_plots(): # setup_plots calls plt.ion() and plt.show(block=False)
            logger.info("Starting live plot animation.")
            # *** MODIFICATION 1: Set blit=False for stability ***
            animation_obj = FuncAnimation(plot_fig, update_plots,
                                      interval=config.PLOT_UPDATE_INTERVAL_MS,
                                      blit=False,
                                      cache_frame_data=False)
            # No explicit plt.show() here again, as setup_plots handles it non-blockingly
        else:
            logger.warning("Live visualization setup failed or no channels/features to plot.")

    logger.info("Application running. Main thread active. Press Ctrl+C to stop.")
    run_start_time = time.monotonic()

    try:
        while not thread_stop_event.is_set():
            if args.duration is not None and (time.monotonic() - run_start_time) >= args.duration:
                logger.info(f"Reached configured run duration ({args.duration}s). Initiating graceful shutdown.")
                main_stop_event.set()
                thread_stop_event.set()
                break
            if config.ENABLE_LIVE_VISUALIZATION and plot_fig and plt.get_fignums():
                try:
                    # This allows Matplotlib's event loop to process and FuncAnimation to draw
                    plt.pause(0.01) # Short pause to yield to GUI event loop
                except Exception as e_plot_pause:
                    logger.info(f"Plot window likely closed or error during pause: {e_plot_pause}")
                    if not main_stop_event.is_set(): main_stop_event.set() # Ensure streamers are stopped
                    if not thread_stop_event.is_set(): thread_stop_event.set() # Ensure this loop and other threads exit
                    break
            else:
                # If not plotting, just sleep and check worker status
                time.sleep(0.5)

            # Check if all worker threads/processes have unexpectedly terminated
            # This check should ideally be less frequent if plt.pause is very short.
            if not any(t.is_alive() for t in processor_threads) and \
               not any(p.is_alive() for p in streamer_processes):
                logger.info("All worker processes/threads seem to have terminated. Stopping main.")
                if not main_stop_event.is_set(): main_stop_event.set()
                if not thread_stop_event.is_set(): thread_stop_event.set()
                break

            # Periodically check if the stop event was set by the signal handler
            if thread_stop_event.is_set():
                break

    except KeyboardInterrupt:
        logger.info("Main thread caught KeyboardInterrupt.")
        if not main_stop_event.is_set(): main_stop_event.set()
        if not thread_stop_event.is_set(): thread_stop_event.set()

    finally:
        logger.info("Main thread initiating shutdown sequence...")
        if not main_stop_event.is_set(): main_stop_event.set()
        if not thread_stop_event.is_set(): thread_stop_event.set()

        logger.info("Waiting for streamer processes to terminate...")
        for p_idx, p in enumerate(streamer_processes):
            p.join(timeout=10)
            if p.is_alive():
                logger.warning(f"Streamer {p.name} did not terminate gracefully.")
                if hasattr(p, "terminate"):
                    p.terminate()
                    p.join(2)

        logger.info("Waiting for processor threads to join...")
        for t_idx, t in enumerate(processor_threads):
            t.join(timeout=10)
            if t.is_alive(): logger.warning(f"Processor thread {t.name} did not join gracefully.")

        if fatigue_analysis_executor: logger.info("Shutting down fatigue executor..."); fatigue_analysis_executor.shutdown(wait=True)
        if kafka_alert_producer: logger.info("Closing alert producer..."); kafka_alert_producer.close(timeout=5)
        if kafka_features_producer: logger.info("Closing features producer..."); kafka_features_producer.close(timeout=5)
        # NEW: Close the raw EMG broker client
        if raw_emg_broker_client: logger.info("Closing raw EMG broker client..."); raw_emg_broker_client.close()


        if config.ENABLE_LIVE_VISUALIZATION and plot_fig:
            logger.info("Attempting to close plot window...")
            try:
                plt.close(plot_fig) # Try to close the specific figure
                logger.info("Plot window closed.")
            except Exception as e_plot_close:
                logger.warning(f"Error closing plot: {e_plot_close}")

        logger.info("--- LibEMG Processor Main Shutdown Complete ---")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()