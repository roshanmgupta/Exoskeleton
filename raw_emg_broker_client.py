# raw_emg_broker_client.py
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)

class RawEMGBrokerClient:
    """
    A client to send raw EMG data to a configured message broker (Kafka or MQTT).
    """
    def __init__(self, broker_type, config):
        self.broker_type = broker_type
        self.config = config
        self.client = None
        self.topics = None

        if self.broker_type == 'kafka':
            self._initialize_kafka()
        elif self.broker_type == 'mqtt':
            self._initialize_mqtt()
        else:
            logger.error(f"Unsupported broker type: {self.broker_type}. Choose 'kafka' or 'mqtt'.")
            raise ValueError("Unsupported broker type")

    def _initialize_kafka(self):
        """Initializes the Kafka producer."""
        try:
            self.client = KafkaProducer(
                bootstrap_servers=self.config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=3,
                request_timeout_ms=10000
            )
            self.topics = self.config['topics']
            logger.info(f"Kafka producer initialized for servers {self.config['bootstrap_servers']}")
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka producer: {e}", exc_info=True)
            self.client = None

    def _initialize_mqtt(self):
        """Initializes the MQTT client."""
        try:
            self.client = mqtt.Client()
            self.client.on_connect = self._on_mqtt_connect
            self.client.on_disconnect = self._on_mqtt_disconnect
            self.client.connect(self.config['host'], self.config['port'], 60)
            self.client.loop_start() # Start network loop in background thread
            self.topics = self.config['topics']
            logger.info(f"MQTT client initialized for host {self.config['host']}:{self.config['port']}")
        except Exception as e:
            logger.error(f"Failed to initialize MQTT client: {e}", exc_info=True)
            self.client = None

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("Successfully connected to MQTT broker.")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}\n")

    def _on_mqtt_disconnect(self, client, userdata, rc):
        logger.warning(f"Disconnected from MQTT broker with result code {rc}")

    def send_data(self, thingid, timestamp_iso, channel_map, data_values):
        """
        Formats the raw data into JSON and sends it to the configured broker.

        Args:
            thingid (str): The ID of the thing/device (e.g., "myo_shirt_01").
            timestamp_iso (str): The ISO 8601 formatted timestamp.
            channel_map (dict): The mapping from channel index to muscle name.
            data_values (list): The list of raw float values from the EMG sensor.
        """
        if not self.client:
            logger.warning(f"Broker client not initialized. Cannot send data for {thingid}.")
            return

        payload = {
            "thingid": thingid,
            "timestamp": timestamp_iso
        }
        for index, muscle_name in channel_map.items():
            if 0 <= index < len(data_values):
                payload[muscle_name] = round(data_values[index], 2)
            else:
                logger.warning(f"Channel index {index} for muscle '{muscle_name}' is out of bounds for data from {thingid}")

        if self.broker_type == 'kafka':
            self._send_kafka(payload)
        elif self.broker_type == 'mqtt':
            self._send_mqtt(payload)

    def _send_kafka(self, payload):
        """Sends a message to the appropriate Kafka topic."""
        thingid = payload.get("thingid")
        topic = self.topics.get(thingid)
        if topic:
            try:
                self.client.send(topic, value=payload)
            except Exception as e:
                logger.error(f"Error sending data to Kafka topic {topic} for {thingid}: {e}")
        else:
            logger.warning(f"No Kafka topic configured for thingid: {thingid}")

    def _send_mqtt(self, payload):
        """Sends a message to the appropriate MQTT topic."""
        thingid = payload.get("thingid")
        topic = self.topics.get(thingid)
        if topic:
            try:
                json_payload = json.dumps(payload)
                self.client.publish(topic, json_payload)
            except Exception as e:
                logger.error(f"Error sending data to MQTT topic {topic} for {thingid}: {e}")
        else:
            logger.warning(f"No MQTT topic configured for thingid: {thingid}")


    def close(self):
        """Closes the connection to the broker."""
        if self.client:
            if self.broker_type == 'kafka':
                self.client.close(timeout=5)
                logger.info("Kafka producer closed.")
            elif self.broker_type == 'mqtt':
                self.client.loop_stop()
                self.client.disconnect()
                logger.info("MQTT client disconnected.")