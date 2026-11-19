# config_live.py
import os

# Activate your virtual environment before running the pipeline:
#   Windows: venv\Scripts\activate
#   Linux/macOS: source venv/bin/activate

# --- Kafka Configuration ---
KAFKA_BROKERS = [
    b.strip()
    for b in os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
    if b.strip()
]
FATIGUE_ALERT_TOPIC = os.getenv("FATIGUE_ALERT_TOPIC", "exolink_emg_mdf_fatigue_alerts")
FEATURES_VISUALIZATION_TOPIC = os.getenv(
    "FEATURES_VISUALIZATION_TOPIC", "exolink_emg_shirt01_libemg_features"
)
PARTICIPANT_ID = os.getenv("PARTICIPANT_ID", "demo-participant-01")

# --- Raw EMG Data Streamer Configuration ---
RAW_EMG_STREAMER = {
    "ENABLE": os.getenv("ENABLE_RAW_STREAMER", "true").lower() == "true",  # Set to True to send raw EMG data
    "BROKER_TYPE": os.getenv("RAW_BROKER_TYPE", "kafka").lower(),  # Choose "kafka" or "mqtt"

    "KAFKA_RAW_CONFIG": {
        "bootstrap_servers": KAFKA_BROKERS,
        "topics": {
            "Shirt01-Left": "k_myontec_01_left_emg",
            "Shirt01-Right": "k_myontec_01_right_emg",
            "Shirt01-Trunk": "k_myontec_01_trunk_emg"
        }
    },
    "MQTT_RAW_CONFIG": {
        "host": os.getenv("MQTT_HOST", "localhost"),
        "port": int(os.getenv("MQTT_PORT", "1883")),
        "topics": {
            "Shirt01-Left": "emg/raw/shirt01",
            "Shirt01-Right": "emg/raw/shirt02",
            "Shirt01-Trunk": "emg/raw/shirt03"
        }
    }
}

# --- MCell Device & UDP Streamer Configuration ---
# Override BLE addresses via environment variables (see .env.example) when using physical hardware.
DEVICE_STREAMERS_CONFIG = [
    {
        "ble_address": os.getenv("BLE_ADDR_LEFT", "00:00:00:00:00:01"),
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
        "ble_address": os.getenv("BLE_ADDR_RIGHT", "00:00:00:00:00:02"),
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
        "ble_address": os.getenv("BLE_ADDR_TRUNK", "00:00:00:00:00:03"),
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

UDP_STREAM_HOST = os.getenv("UDP_STREAM_HOST", "127.0.0.1")  # Streamers send to this host

# --- BLE Specific Configurations (for mcell_ble_udp_streamer.py) ---
MUSCLE_SERVICE_CHAR_UUID = "eba11dc2-6fab-479b-a742-1a2dfad8f122"  # As used by MCell

CMD_PREPARE_MEASUREMENT = b'\x6F\x6D\x34\x34\x30\x31\x31\x0D'  # 100Hz EMG, IMU Off, Two-Phase Enabled, Not Silent UI
CMD_START_STREAM_DATA = b'\x6F\x6D\x74\x0D'  # Standard MCell command: omt + CR
CMD_STOP_MEASUREMENT = b'\x6F\x6D\x6E\x0D'  # Standard MCell command: omn + CR (Stop online measurement)

COMMAND_DICT = {
    "prepare": CMD_PREPARE_MEASUREMENT,
    "start_stream": CMD_START_STREAM_DATA,
    "stop": CMD_STOP_MEASUREMENT
}

BLE_HEADER_PARSE_TIMEOUT_SECONDS = 20  # For streamer to wait for MCell header
BLE_RUN_DURATION_SECONDS = None  # For streamer process; None = run until stop_event

# --- LibEMG Processing & General EMG Parameters ---
SAMPLING_RATE_HZ = 100.0  # Must match the data rate configured on MCell (e.g., 100Hz by CMD_PREPARE_MEASUREMENT)

# Windowing for LibEMG feature extraction
LIBEMG_WINDOW_LENGTH_SEC = 1.0
LIBEMG_WINDOW_INCREMENT_SEC = 0.5  # 50% overlap -> 2 feature sets per second per channel
LIBEMG_WINDOW_SLIDE_PERCENT = 0.5  # How much the window slides (determines feature rate)

# Windowing for MVC calculation (using LibEMG's feature extractor on windowed MVC data)
MVC_WINDOW_LENGTH_SEC = 0.25  # seconds
MVC_WINDOW_INCREMENT_SEC = 0.05  # seconds
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
TREND_WINDOW_SEC = 20  # Maximum observation period for MDF history
MIN_MDF_POINTS_FOR_TREND = 20  # Minimum number of MDF values needed in the trend window
MDF_SLOPE_FATIGUE_THRESHOLD = -0.1  # Hz/second. Alert if MDF decreases faster than this.
REGRESSION_R_SQUARED_THRESHOLD = 0.25  # How well the trendline fits the data (0 to 1)
ALERT_COOLDOWN_SEC = 60  # Seconds before another alert for the same muscle

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
ENABLE_LIVE_VISUALIZATION = os.getenv("ENABLE_LIVE_VISUALIZATION", "false").lower() == "true"
VISUALIZATION_DEVICE_NAME_TARGET = os.getenv("VISUALIZATION_DEVICE_NAME_TARGET", "Shirt01-Right")
VISUALIZATION_RAW_CHANNELS_INDICES = [0, 1, 2]
VISUALIZATION_FEATURES_TO_PLOT = ["MDF"]
VISUALIZATION_FEATURE_CHANNELS_INDICES = [0, 1, 2]

PLOT_UPDATE_INTERVAL_MS = 100  # How often to refresh the plots (milliseconds)
PLOT_WINDOW_DURATION_SEC = 180  # How many seconds of data to show in the plot window

# --- Logging Configuration ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = '%(asctime)s - %(levelname)s - [%(processName)s:%(threadName)s] - [%(name)s] - %(message)s'
