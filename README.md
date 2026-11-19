# Real-Time EMG Analysis and Data Streaming Pipeline

This project is a comprehensive, multi-threaded Python pipeline for real-time acquisition, processing, and analysis of Electromyography (EMG) data from MCell wireless sensors. It is designed to monitor multiple muscles simultaneously, calculate muscle activation features (e.g., RMS, MDF), detect signs of muscle fatigue, and stream both raw and processed data to configurable message brokers.

The system is built for scalability and research, allowing for the concurrent operation of two distinct data streams:

1.  **Raw Data Stream**: Forwards the raw, unprocessed EMG data directly to a message broker (Kafka or MQTT) for recording, offline analysis, or use in other real-time applications.
2.  **Processed Feature Stream**: Performs advanced signal processing using the `libemg` library to extract meaningful features, analyzes them for fatigue, and streams the results to Kafka.

This dual-stream approach provides maximum flexibility, offering immediate access to raw data while simultaneously providing a high-level, processed data feed with built-in fatigue analytics.

## High-Level Architecture

The pipeline follows a modular, parallel data flow:

1.  **Data Acquisition**: A dedicated process for each MCell device (`mcell_ble_udp_streamer.py`) connects via Bluetooth Low Energy (BLE), parses the device's data header, and streams the raw EMG samples over UDP to a local port.
2.  **UDP Ingestion & Fan-Out**: The main processor (`libemg_processor_main.py`) listens on the specified UDP ports. As soon as a data packet arrives, it is immediately fanned out to two parallel pathways:
      * **Pathway A (Raw Streaming)**: The raw data is instantly sent to the `raw_emg_broker_client.py` module, which formats it as a JSON message and publishes it to a pre-configured Kafka or MQTT topic.
      * **Pathway B (Feature Extraction)**: The same raw data is added to a buffer for signal processing.
3.  **Signal Processing & Feature Extraction**: For each device, a dedicated thread windows the buffered data, applies digital filters, and uses the `libemg` library to extract features like Root Mean Square (RMS), Median Frequency (MDF), and Mean Power (MNP).
4.  **Fatigue Analysis**: While the project is designed to be forward-compatible with high-frequency (\>=1kHz) streaming for more robust fatigue analysis, it currently implements a functional fatigue detection algorithm based on the slope of the MDF trend, which can be used for preliminary analysis.
5.  **Processed Data Streaming**: All extracted features and any generated fatigue alerts are serialized and sent to dedicated Kafka topics.
6.  **Calibration & Visualization**: The pipeline includes a standalone utility (`mvc_calibration_tool_live.py`) for MVC calibration and an optional live plotting feature to visualize both raw signals and extracted features in real-time.

## Features

  * **Dual Data Streams**: Simultaneously stream raw EMG data to Kafka/MQTT and processed features/alerts to Kafka.
  * **Broker Agnostic**: Easily switch between Kafka and MQTT for raw data streaming via the configuration file.
  * **Multi-Device Streaming**: Concurrently stream data from multiple MCell BLE devices, each handled in an isolated process for robustness.
  * **Real-Time Feature Extraction**: Utilizes the `libemg` library to calculate a variety of EMG features from a sliding window.
  * **MVC Normalization**: Includes a calibration tool to record Maximum Voluntary Contraction (MVC) and calculate normalized RMS (%MVC), a standard metric for muscle effort.
  * **Configurable Fatigue Detection**: Implements a fatigue detection algorithm based on the slope and R² value of the MDF trend over time.
  * **Live Visualization**: Optional real-time plotting of both raw EMG signals and extracted features using Matplotlib.
  * **Highly Configurable**: All parameters—from device addresses and broker endpoints to filter settings and fatigue thresholds—are managed in a single configuration file (`config_live.py`).
  * **Robust & Concurrent**: Uses a combination of `multiprocessing` and `threading` for efficient and resilient operation.

## Setup and Installation

### Prerequisites

  * Python 3.8+
  * An running Apache Kafka and/or an MQTT broker (e.g., Mosquitto).
  * Access to the MCell BLE devices.

### Installation

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/roshanmgupta/Exoskeleton.gi
    cd <your-repo-directory>
    ```

2.  **Install Python dependencies:**
    It is highly recommended to use a virtual environment.

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    pip install -r requirements.txt
    ```

## Configuration

All system parameters are configured in the `config_live.py` file. Before running the application, you must review and edit this file.

**Key settings to configure:**

  * `RAW_EMG_STREAMER`:
      * Set `ENABLE` to `True` to send raw EMG data.
      * Choose your `BROKER_TYPE`: `"kafka"` or `"mqtt"`.
      * Fill in the appropriate configuration block (`KAFKA_RAW_CONFIG` or `MQTT_RAW_CONFIG`) with your broker details and topics.
  * `KAFKA_BROKERS`, `FATIGUE_ALERT_TOPIC`, etc.: Your Kafka endpoints for the processed feature stream.
  * `DEVICE_STREAMERS_CONFIG`: A list of dictionaries, one for each MCell device. You **must** enter the correct `ble_address`, `udp_port`, and channel information for each device.
  * `LIBEMG_...`: Parameters for signal processing, such as window length and features to extract.
  * `FATIGUE_ANALYSIS_...`: Thresholds for the fatigue detection algorithm.
  * `ENABLE_LIVE_VISUALIZATION`: Set to `True` or `False` to control the live plotting feature.

## How to Run

The application is run in two stages: a one-time calibration followed by the main processing pipeline.

### Step 1: Perform MVC Calibration (Required First Step)

This step measures the peak muscle activation for each muscle of interest, which is used as a reference for normalization. You must run this tool once to generate the `mvc_references.json` file.

1.  Ensure the MCell devices are on and available for connection.
2.  Run the calibration script from your terminal:
    ```bash
    python mvc_calibration_tool_live.py
    ```
3.  The tool will prompt you to perform a maximal contraction for each muscle defined in the `fatigue_channel_map` in your config file. Follow the on-screen instructions.
4.  Upon completion, the script will create/update the `mvc_references.json` file with the calculated peak RMS values.

### Step 2: Launch the Main Processing Pipeline

Once calibration is complete and your `config_live.py` is set up, you can start the main application.

1.  Ensure your Kafka/MQTT brokers are running and accessible.
2.  Run the main processor script:
    ```bash
    python libemg_processor_main.py
    ```
3.  The application will start, launching a streamer process for each configured device and a processor thread for each data stream.
4.  You will see log messages in your terminal indicating the status of all broker connections, BLE connections, and UDP listeners.
5.  If `ENABLE_LIVE_VISUALIZATION` is `True`, a Matplotlib window will appear, displaying the specified signals in real time.
6.  To stop the application, press `Ctrl+C` in the terminal. The application will perform a graceful shutdown of all processes and threads.

## Code Structure

  * `config_live.py`: Central configuration file for all parameters.
  * `libemg_processor_main.py`: The main application entry point. Orchestrates streamer processes, data processing threads, and all broker clients.
  * `raw_emg_broker_client.py`: A dedicated client for publishing raw EMG data to either Kafka or MQTT, based on the configuration.
  * `mcell_ble_udp_streamer.py`: Manages the BLE connection, header parsing, and UDP streaming for a single MCell device.
  * `mvc_calibration_tool_live.py`: A standalone command-line tool for performing MVC calibration.
  * `fatigue_analyzer.py`: Contains the fatigue detection logic based on linear regression of MDF values.
  * `requirements.txt`: A list of all Python package dependencies and their versions.
  * `mvc_references.json`: Stores the output of the calibration tool. It is used as an input by the main processor for %MVC calculation.
