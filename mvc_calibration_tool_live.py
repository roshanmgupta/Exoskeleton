# mvc_calibration_tool.py
import numpy as np
import time
import json
import logging
import os
import multiprocessing
import pickle # For unpickling UDP data
import socket # For manual UDP listening
import tempfile # For OfflineDataHandler workaround
import shutil   # For OfflineDataHandler workaround
from colorama import Fore, Back, Style

# print(Fore.RED + 'some red text')

# LibEMG imports
from libemg.data_handler import OfflineDataHandler, RegexFilter
from libemg.feature_extractor import FeatureExtractor

import config_live as config
# Assuming mcell_ble_udp_streamer.py contains run_streamer_process
from mcell_ble_udp_streamer import run_streamer_process

logger = logging.getLogger("MVCCalibrationTool")
# BasicConfig should be set once in the __main__ block
# logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT)

def record_live_mvc_trial_data(device_config: dict,
                               channel_index_to_calibrate: int,
                               duration_sec: int = 5,
                               sampling_rate_hz: float = config.SAMPLING_RATE_HZ):
    """
    Records live EMG data for an MVC trial from a specific MCell device channel
    by launching the mcell_ble_udp_streamer.py and listening to its UDP output.
    """
    device_name = device_config["name"]
    ble_address = device_config["ble_address"]
    udp_port = device_config["udp_port"]
    # This num_device_channels is what the streamer *should* send based on header.
    # The UDP listener will try to unpickle based on this assumption.
    num_device_channels = device_config["num_channels_expected"]

    logger.info(f"Prepare for LIVE MVC trial: Device '{device_name}', Calibrating Channel Index {channel_index_to_calibrate}.")
    logger.info(f"Ensure MCell device {ble_address} is ON and ready.")
    logger.info(f"MCell Streamer will send data to UDP {config.UDP_STREAM_HOST}:{udp_port}.")
    logger.info(f"Contract maximally for {duration_sec} seconds when prompted.")
    input("Press Enter when ready to start the MCell streamer AND then the MVC recording countdown...")

    streamer_stop_event = multiprocessing.Event()
    streamer_process = multiprocessing.Process(
        target=run_streamer_process,
        args=(
            ble_address, device_name,
            config.MUSCLE_SERVICE_CHAR_UUID, config.COMMAND_DICT,
            config.UDP_STREAM_HOST, udp_port,
            config.BLE_HEADER_PARSE_TIMEOUT_SECONDS,
            None, # Streamer runs until stop_event
            streamer_stop_event
        ),
        name=f"Streamer-{device_name}-MVC"
    )
    streamer_process.daemon = True
    streamer_process.start()
    logger.info(f"MCell BLE UDP Streamer process for {device_name} started (PID: {streamer_process.pid}).")
    
    # Give streamer crucial time to connect, parse header, and start streaming
    # This needs to be sufficient for your slowest device to be ready.
    logger.info("Waiting for streamer to initialize and start sending data (e.g., 15-20 seconds)...")
    time.sleep(15) # ADJUST THIS BASED ON OBSERVATION

    listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    all_datagroups_collected_this_trial = [] # List to store all datagroups (each is a list of floats)

    try:
        listener_socket.bind((config.UDP_STREAM_HOST, udp_port))
        listener_socket.settimeout(1.0) # Timeout for socket.recvfrom()
        logger.info(f"Manual UDP listener bound to {config.UDP_STREAM_HOST}:{udp_port}.")

        # Second prompt for the actual contraction
        input(f"Press Enter AGAIN to start the {duration_sec}-second MAXIMAL CONTRACTION recording...")
        logger.info(f"Recording MVC for {duration_sec} seconds...")
        
        start_record_time = time.monotonic()
        datagroups_count_in_mvc = 0

        while time.monotonic() - start_record_time < duration_sec:
            try:
                # Each UDP packet should contain one pickled datagroup (list of floats)
                pickled_datagroup_bytes, addr = listener_socket.recvfrom(4096) # Adjust buffer size if needed
                
                if pickled_datagroup_bytes:
                    datagroup_floats = pickle.loads(pickled_datagroup_bytes)
                    if isinstance(datagroup_floats, list) and len(datagroup_floats) == num_device_channels:
                        all_datagroups_collected_this_trial.append(datagroup_floats)
                        datagroups_count_in_mvc += 1
                    else:
                        logger.warning(f"UDP packet unpickled to unexpected type/channel count: {type(datagroup_floats)}, len: {len(datagroup_floats) if hasattr(datagroup_floats, '__len__') else 'N/A'}")
            except socket.timeout:
                logger.debug("UDP recvfrom timeout during MVC recording period.")
                if not streamer_process.is_alive():
                    logger.error("Streamer process died unexpectedly during MVC. Aborting trial.")
                    return None
                continue
            except pickle.UnpicklingError as pe:
                logger.error(f"Error unpickling UDP data: {pe}")
            except Exception as e_recv:
                logger.error(f"Error receiving/processing UDP data: {e_recv}")
                time.sleep(0.01) 

        logger.info(f"MVC recording period finished. Collected {datagroups_count_in_mvc} datagroups for this trial.")

        if not all_datagroups_collected_this_trial:
            logger.error("No EMG datagroups collected via UDP during MVC contraction period.")
            return None

        # Convert list of datagroups into a NumPy array (samples, channels)
        full_trial_data_np = np.array(all_datagroups_collected_this_trial, dtype=np.float32)
        
        if full_trial_data_np.ndim == 2 and 0 <= channel_index_to_calibrate < full_trial_data_np.shape[1]:
            emg_data_for_target_channel_1d = full_trial_data_np[:, channel_index_to_calibrate]
            logger.info(f"Extracted {len(emg_data_for_target_channel_1d)} samples for target channel index {channel_index_to_calibrate}.")
            return emg_data_for_target_channel_1d
        else:
            logger.error(f"Could not extract target channel index {channel_index_to_calibrate} from collected UDP data. Data shape: {full_trial_data_np.shape}.")
            return None

    except Exception as e:
        logger.error(f"Error during live MVC data collection for {device_name}: {e}", exc_info=True)
        return None
    finally:
        if 'listener_socket' in locals() and listener_socket:
            listener_socket.close()
            logger.info(f"Manual UDP listener socket for port {udp_port} closed.")
        
        logger.info(f"Signaling MCell BLE UDP Streamer for {device_name} to stop...")
        streamer_stop_event.set()
        if streamer_process.is_alive():
            logger.info(f"Waiting for streamer process {device_name} to join...")
            streamer_process.join(timeout=25) # Give it time to stop BLE etc.
            if streamer_process.is_alive():
                logger.warning(f"Streamer process {device_name} did not terminate gracefully. Forcing.")
                streamer_process.terminate()
                streamer_process.join(5)
        logger.info(f"Streamer process for {device_name} stopped or was already stopped.")


def calculate_peak_rms_from_data(emg_data_1d: np.ndarray, sampling_rate: int, muscle_name_for_file: str):
    """
    Calculates peak RMS from a 1D EMG signal using LibEMG OfflineDataHandler
    via a temporary file workaround.
    """
    if emg_data_1d is None or len(emg_data_1d) < config.MVC_WINDOW_SAMPLES:
        logger.warning(f"Not enough data for MVC RMS calculation for {muscle_name_for_file}. Samples: {len(emg_data_1d) if emg_data_1d is not None else 0}, Required: {config.MVC_WINDOW_SAMPLES}")
        return None

    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp()
        # Use a more unique temp file name to avoid collisions if multiple run concurrently (though not in this script)
        temp_file_name = f"{muscle_name_for_file}_{int(time.time()*1000)}_mvc_trial.csv"
        temp_file_path = os.path.join(temp_dir, temp_file_name)
        
        np.savetxt(temp_file_path, emg_data_1d, delimiter=',')
        logger.info(f"Saved temporary MVC data to: {temp_file_path}")

        odh = OfflineDataHandler()
        # Using the RegexFilter constructor that worked for your LibEMG 2.0.0
        regex_filter_instance = RegexFilter(
            left_bound="",      
            right_bound="",     
            values=[os.path.basename(temp_file_path)], # Match exact filename
            description="mvc_file" 
        )
        
        odh.get_data(
            folder_location=temp_dir, # LibEMG scans this folder
            regex_filters=[regex_filter_instance],
            delimiter=',',
            data_column=[0], 
            skiprows=0 
        )
        
        # Check where LibEMG loaded the data
        loaded_data_source = None
        if hasattr(odh, 'data') and odh.data and isinstance(odh.data, list) and len(odh.data) > 0 and isinstance(odh.data[0], np.ndarray):
            logger.info(f"Data loaded into odh.data. Using it. Shape of first item: {odh.data[0].shape}")
            # For parse_windows, it likely expects the data to be in a standard place.
            # Let's assume parse_windows uses odh.raw_emg or similar internal attributes that get_data populates.
            # If get_data populates 'odh.data', we need to ensure 'parse_windows' can access it.
            # The structure of 'odh.data' after get_data is a list of arrays, each array usually (channels, samples_in_trial)
            # For our single CSV, odh.data[0] should be (1, num_samples_in_mvc_trial) if data_column=[0] worked as expected.
            # If parse_windows needs data in odh.raw_emg
            if not hasattr(odh, 'raw_emg') or not odh.raw_emg:
                 odh.raw_emg = odh.data # Make data accessible via raw_emg if parse_windows uses it
            loaded_data_source = "odh.data (copied to odh.raw_emg if needed)"

        elif hasattr(odh, 'raw_emg') and odh.raw_emg and isinstance(odh.raw_emg, list) and len(odh.raw_emg) > 0 and isinstance(odh.raw_emg[0], np.ndarray):
            logger.info(f"Data found directly in odh.raw_emg. Using it. Shape of first item: {odh.raw_emg[0].shape}")
            loaded_data_source = "odh.raw_emg"
        else:
            logger.error(f"Failed to find loaded data in odh.data or odh.raw_emg for {muscle_name_for_file}.")
            return None
        logger.debug(f"Data for parse_windows sourced from: {loaded_data_source}")
        
        # Set sampling frequency on ODH instance if it has the attribute and parse_windows needs it
        # Or pass to FeatureExtractor if that's where it's used.
        # For RMS, fs might not be needed by FeatureExtractor, but good practice if ODH supports it.
        if hasattr(odh, 'sampling_frequency'): odh.sampling_frequency = sampling_rate
        elif hasattr(odh, 'fs'): odh.fs = sampling_rate

        windowed_data_array, windowed_metadata = odh.parse_windows(
            window_size=config.MVC_WINDOW_SAMPLES,
            window_increment=config.MVC_WINDOW_INCREMENT_SAMPLES
        )

        if windowed_data_array is None or windowed_data_array.size == 0:
             logger.warning(f"odh.parse_windows returned no windows for MVC of {muscle_name_for_file}.")
             return None
        
        # windowed_data_array is (num_windows, num_channels, window_length)
        # FeatureExtractor expects a LIST of windows, each (num_channels, window_length)
        mvc_windows_for_fe = [window_data for window_data in windowed_data_array]

        fe = FeatureExtractor()
        rms_features_dict = fe.extract_features(
            feature_list=["RMS"], 
            windows=mvc_windows_for_fe
            # fs parameter removed as it caused TypeError for RMS previously
        )
    
        if "RMS" in rms_features_dict and isinstance(rms_features_dict["RMS"], np.ndarray) and rms_features_dict["RMS"].size > 0:
            # rms_features_dict["RMS"] is shape (num_windows, num_channels)
            peak_rms = np.max(rms_features_dict["RMS"][:, 0]) # Assuming 1 channel for MVC trial processing
            return peak_rms
        else:
            logger.warning(f"RMS feature not found or empty for {muscle_name_for_file}. Got: {rms_features_dict.get('RMS')}")
        return None

    except Exception as e:
        logger.error(f"Error in calculate_peak_rms_from_data for {muscle_name_for_file}: {e}", exc_info=True)
        return None
    finally:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            logger.debug(f"Cleaned up temporary directory: {temp_dir}")

if __name__ == "__main__":
    logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT) # Setup logging once
    logger.info("--- Live MVC Calibration Tool ---")
    mvc_references = {}
    
    if not config.DEVICE_STREAMERS_CONFIG:
        logger.error("No devices configured in config_live.DEVICE_STREAMERS_CONFIG. Exiting MVC tool.")
        exit()

    for device_conf_entry in config.DEVICE_STREAMERS_CONFIG:
        device_name_for_mvc = device_conf_entry["name"]
        logger.info(f"\nStarting MVC calibration for device: {device_name_for_mvc}")
        
        if not device_conf_entry.get("fatigue_channel_map"):
            logger.warning(f"No 'fatigue_channel_map' defined for device {device_name_for_mvc}. Skipping MVC for this device.")
            continue

        for ch_idx_str, muscle_to_monitor_key in device_conf_entry.get("fatigue_channel_map", {}).items():
            try:
                ch_idx = int(ch_idx_str)
                # Ensure channel_names_from_header exists and ch_idx is valid
                if "channel_names_from_header" not in device_conf_entry or \
                   not (0 <= ch_idx < len(device_conf_entry["channel_names_from_header"])):
                    logger.warning(f"Channel index {ch_idx} is invalid or channel_names_from_header missing for {device_name_for_mvc}. Skipping muscle {muscle_to_monitor_key}.")
                    continue
                
                original_channel_name_on_device = device_conf_entry["channel_names_from_header"][ch_idx]
                
                logger.info(f"-- Calibrating: Target Muscle '{muscle_to_monitor_key}' (from Device '{device_name_for_mvc}', Channel Index: {ch_idx}, Original Name: '{original_channel_name_on_device}') --")
                
                emg_data_for_this_channel_mvc = record_live_mvc_trial_data(
                    device_config=device_conf_entry,
                    channel_index_to_calibrate=ch_idx, # This index refers to the column in the multi-channel UDP data
                    duration_sec=5,
                    sampling_rate_hz=config.SAMPLING_RATE_HZ
                )
                
                if emg_data_for_this_channel_mvc is not None and len(emg_data_for_this_channel_mvc) > 0 :
                    # file_id_muscle_name for temporary file uniqueness
                    file_id_muscle_name = f"{device_name_for_mvc}_{original_channel_name_on_device.replace(' ','_')}"
                    peak_rms = calculate_peak_rms_from_data(
                        emg_data_for_this_channel_mvc, 
                        config.SAMPLING_RATE_HZ, 
                        file_id_muscle_name
                    )
                    
                    if peak_rms is not None:
                        mvc_storage_key = f"{device_name_for_mvc}_{muscle_to_monitor_key}" 
                        mvc_references[mvc_storage_key] = float(peak_rms) 
                        logger.info(f"SUCCESS: MVC for '{mvc_storage_key}': {peak_rms:.2f}")
                    else:
                        logger.error(f"Failed to calculate peak RMS for {muscle_to_monitor_key} on device {device_name_for_mvc}.")
            except ValueError: # For int(ch_idx_str)
                logger.error(f"Invalid channel index '{ch_idx_str}' in fatigue_channel_map for {device_name_for_mvc}. Skipping.")
            except Exception as e_loop:
                 logger.error(f"Unexpected error during MVC calibration for {device_name_for_mvc}, muscle {muscle_to_monitor_key if 'muscle_to_monitor_key' in locals() else 'unknown'}: {e_loop}", exc_info=True)

            logger.info("Pausing briefly before next muscle/device calibration...")
            time.sleep(3)

    if mvc_references:
        try:
            with open(config.MVC_VALUES_FILE, 'w') as f:
                json.dump(mvc_references, f, indent=4)
            logger.info(f"All MVC values saved to {config.MVC_VALUES_FILE}")
            print("\n--- Final Saved MVC References ---")
            print(json.dumps(mvc_references, indent=4))
        except Exception as e:
            logger.error(f"Error saving MVC values to file: {e}")
    else:
        logger.warning("No MVC values were calculated to save.")

    logger.info("--- Live MVC Calibration Finished ---")