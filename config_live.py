# config_live.py

# Activate the digital enviroment to run all the python scripts
# --> .dvenv\Scripts\activate
# --> python config.live.py

# --- Kafka Configuration ---
# KAFKA_BROKERS = ['192.168.1.125:9092', '192.168.1.125:9093', '192.168.1.125:9094'] # Server Enrique Home
KAFKA_BROKERS = ['192.168.50.152:9092', '192.168.50.152:9093', '192.168.50.152:9094'] # Server IFF HPZ4
FATIGUE_ALERT_TOPIC = 'exolink_emg_mdf_fatigue_alerts'
FEATURES_VISUALIZATION_TOPIC = 'exolink_emg_shirt01_libemg_features'
#FEATURES_VISUALIZATION_TOPIC = 'iff_emg_libemg_features'

# --- Raw EMG Data Streamer Configuration ---
RAW_EMG_STREAMER = {
    "ENABLE": True, # Set to True to send raw EMG data
    "BROKER_TYPE": "kafka", # Choose "kafka" or "mqtt"

    "KAFKA_RAW_CONFIG": {
        "bootstrap_servers": KAFKA_BROKERS,
        "topics": {
            "Shirt01-Left": "k_myontec_01_left_emg",
            "Shirt01-Right": "k_myontec_01_right_emg",
            "Shirt01-Trunk": "k_myontec_01_trunk_emg"
        }
    },
    "MQTT_RAW_CONFIG": {
        "host": "localhost",
        "port": 1883,
        "topics": {
            "Shirt01-Left": "emg/raw/shirt01",
            "Shirt01-Right": "emg/raw/shirt02",
            "Shirt01-Trunk": "emg/raw/shirt03"
        }
    }
}

# --- MCell Device & UDP Streamer Configuration ---
DEVICE_STREAMERS_CONFIG = [
    {
        "ble_address": "54:6C:0E:9B:42:B2", # Shirt01 -> Left MCell-18031240
        "udp_port": 50001,
        "name": "Shirt01-Left",
        "num_channels_expected": 5,
        "channel_names_from_header": ["Left Deltoids", "Left Triceps", "Left Biceps", "Left Wrist extensors", "Left Wrist flexors"],
        "fatigue_channel_map": {
			0: "deltoids_left",
			1: "triceps_left",
			2: "biceps_left",
			3: "wrist_extensors_left",
			4: "wrist_flexors_left"
		}
    },
    {
        "ble_address": "F0:F8:F2:CC:22:88", # Shirt01  -> Right MCell-18031242
        "udp_port": 50002,
        "name": "Shirt01-Right",
        "num_channels_expected": 5,
        "channel_names_from_header": ["Right Deltoids", "Right Triceps", "Right Biceps", "Right Wrist extensors", "Right Wrist flexors"],
        "fatigue_channel_map": {
            0: "deltoids_right",
            1: "triceps_right",
            2: "biceps_right",
			3: "wrist_extensors_right",
			4: "wrist_flexors_right"
        }
    },
    {
        "ble_address": "F0:F8:F2:CC:20:C3", # Shirt01 -> Trunk MCell-18031239
        "udp_port": 50003,
        "name": "Shirt01-Trunk",
        "num_channels_expected": 6,
        "channel_names_from_header": ["Left Trapezius", "Right Trapezius", "Right Pectoralis", "Left Pectoralis", "Left Latissimus dorsi", "Right Latissimus dorsi"],
        "fatigue_channel_map": {
			0: "trapezius_left",
			1: "trapezius_right",
			2: "pectoralis_right",
			3: "pectoralis_left",
			4: "latissimus_dorsi_left",
			5: "latissimus_dorsi_right"
		}
    }
]

UDP_STREAM_HOST = "127.0.0.1" # Streamers send to this host

# --- BLE Specific Configurations (for mcell_ble_udp_streamer.py) ---
MUSCLE_SERVICE_CHAR_UUID = "eba11dc2-6fab-479b-a742-1a2dfad8f122" # As used by MCell

CMD_PREPARE_MEASUREMENT = b'\x6F\x6D\x34\x34\x30\x31\x31\x0D' # 100Hz EMG, IMU Off, Two-Phase Enabled, Not Silent UI
CMD_START_STREAM_DATA = b'\x6F\x6D\x74\x0D'  # Standard MCell command: omt + CR
CMD_STOP_MEASUREMENT = b'\x6F\x6D\x6E\x0D'  # Standard MCell command: omen + CR (Stop online measurement)

COMMAND_DICT = {
    "prepare": CMD_PREPARE_MEASUREMENT,
    "start_stream": CMD_START_STREAM_DATA,
    "stop": CMD_STOP_MEASUREMENT
}

BLE_HEADER_PARSE_TIMEOUT_SECONDS = 20 # For streamer to wait for MCell header
BLE_RUN_DURATION_SECONDS = None # For streamer process; None = run until stop_event

# --- LibEMG Processing & General EMG Parameters ---
SAMPLING_RATE_HZ = 100.0  # This MUST match the data rate configured on MCell (e.g., 100Hz by CMD_PREPARE_MEASUREMENT)

# Windowing for LibEMG feature extraction
LIBEMG_WINDOW_LENGTH_SEC = 1.0
LIBEMG_WINDOW_INCREMENT_SEC = 0.5 # 50% overlap -> 2 feature sets per second per channel (if data arrives consistently)
# --- LibEMG Processing & General EMG Parameters ---
LIBEMG_WINDOW_SLIDE_PERCENT = 0.5 # How much the window slides (determines feature rate)

# Windowing for MVC calculation (using LibEMG's feature extractor on windowed MVC data)
MVC_WINDOW_LENGTH_SEC = 0.25 # seconds
MVC_WINDOW_INCREMENT_SEC = 0.05 # seconds
MVC_WINDOW_SAMPLES = int(MVC_WINDOW_LENGTH_SEC * SAMPLING_RATE_HZ)
MVC_WINDOW_INCREMENT_SAMPLES = int(MVC_WINDOW_INCREMENT_SEC * SAMPLING_RATE_HZ)

LIBEMG_FILTER_CONFIGS = [
    {'name': 'bandpass', 'cutoff': [10, 48], 'order': 4},
    # {'name': 'notch', 'cutoff': 50, 'order': 4}
]
APPLY_LIBEMG_FILTERING = True

# Features to extract using LibEMG
FEATURES_TO_EXTRACT = ["RMS", "MDF", "MNF", "MNP"]

# --- Fatigue Analysis Parameters ---
TREND_WINDOW_SEC = 20  # defines the maximum observation period MDF history.
MIN_MDF_POINTS_FOR_TREND = 20 # Minimum number of MDF values needed in the trend window
MDF_SLOPE_FATIGUE_THRESHOLD = -0.1 # Hz/second. Alert if MDF decreases faster than this.
REGRESSION_R_SQUARED_THRESHOLD = 0.25 # How well the trendline fits the data (0 to 1)
ALERT_COOLDOWN_SEC = 60 # Seconds before another alert for the same muscle

MUSCLES_TO_MONITOR_FOR_FATIGUE = {
    "deltoids_right",
    "triceps_right",
    "biceps_right"
}

# --- MVC Values File ---
MVC_VALUES_FILE = "mvc_references.json"

# --- Threading/Processing Configuration ---
FATIGUE_ANALYSIS_WORKERS = 3


# --- Live Visualization Parameters ---
ENABLE_LIVE_VISUALIZATION = False #True #True  # Set to True to enable live plots
#VISUALIZATION_DEVICE_NAME_TARGET = "Shirt02-Left" # Which device's data to plot from DEVICE_STREAMERS_CONFIG
#VISUALIZATION_DEVICE_NAME_TARGET = "Shirt02-Right" # Which device's data to plot from DEVICE_STREAMERS_CONFIG
VISUALIZATION_DEVICE_NAME_TARGET = "Shirt01-Trunk" # Which device's data to plot from DEVICE_STREAMERS_CONFIG
# For raw signal plot: list of channel indices from the device's channel_names_from_header
VISUALIZATION_RAW_CHANNELS_INDICES = [0, 1, 2] # e.g., plot first two channels of the target device
# For feature plot: list of feature names (must be in FEATURES_TO_EXTRACT)
VISUALIZATION_FEATURES_TO_PLOT = ["MDF"] # e.g., plot MDF, RMS, and MNF features
# For feature plot: list of channel indices whose selected features will be plotted
VISUALIZATION_FEATURE_CHANNELS_INDICES = [0, 1, 2] # e.g., plot RMS and MDF for the first channel of target device

PLOT_UPDATE_INTERVAL_MS = 100  # How often to refresh the plots (milliseconds)
PLOT_WINDOW_DURATION_SEC = 180   # How many seconds of data to show in the plot window


# --- Logging Configuration ---
LOG_LEVEL = "INFO"
LOG_FORMAT = '%(asctime)s - %(levelname)s - [%(processName)s:%(threadName)s] - [%(name)s] - %(message)s'