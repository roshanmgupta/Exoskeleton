import asyncio
import struct
import socket
import pickle
import logging
import time
from datetime import datetime, timezone
from bleak import BleakClient, BleakError

# This script is intended to be run as a separate process for each MCell device.

class MCellBLEUDPStreamer:
    def __init__(self, ble_address: str, device_name: str,
                 char_uuid: str, commands: dict,
                 udp_host: str, udp_port: int,
                 header_timeout_sec: int, run_duration_sec: int = None,
                 stop_event_mp=None): # multiprocessing.Event for external stop

        self.ble_address = ble_address
        self.device_name = device_name
        self.char_uuid = char_uuid
        self.commands = commands
        self.udp_host = udp_host
        self.udp_port = udp_port
        self.header_timeout_sec = header_timeout_sec
        self.run_duration_sec = run_duration_sec
        self.stop_event_mp = stop_event_mp # For external process signaling

        self.logger = logging.getLogger(f"Streamer.{self.device_name}")
        self.client = BleakClient(self.ble_address, disconnected_callback=self._disconnected_callback)
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.header_info = {
            "target_header_size": 0, "received_header_size": 0,
            "is_fully_received": False, "channel_count": 0,
            "data_rate_hz": 0, "header_buffer": bytearray(),
            "channel_names_ordered": [] # Will be populated from MCell header
        }
        self.expecting_sample_data_flag = False
        self.header_parsing_complete_event = asyncio.Event()
        self.datagroup_count = 0

    def _disconnected_callback(self, client):
        self.logger.warning(f"BLE disconnected from {client.address}!")
        if self.stop_event_mp and not self.stop_event_mp.is_set():
            self.logger.info("Setting stop event due to unexpected disconnect.")
            self.stop_event_mp.set() # Signal main process if it needs to know

    def _parse_mcell_header(self):
        """Parses MCell specific header from self.header_info['header_buffer']"""
        buffer = self.header_info['header_buffer']
        try:
            self.header_info["target_header_size"] = int.from_bytes(buffer[0:4], byteorder='big')
            self.header_info["header_version"] = int.from_bytes(buffer[4:8], byteorder='big')
            self.header_info["channel_count"] = int.from_bytes(buffer[8:12], byteorder='big')
            self.header_info["data_rate_hz"] = int.from_bytes(buffer[12:16], byteorder='big')
            # ... (add parsing for date/time and channel names if needed by streamer)
            # For now, channel_count and data_rate_hz are most critical for the streamer.
            # Channel names can be useful for logging here or passed in config.
            
            # Conceptual: Parse channel names (SignalSource field)
            # Each ChannelInfo struct is 70 bytes, starting after the first 23 general header bytes.
            current_offset = 23
            parsed_channel_names = []
            for i in range(self.header_info["channel_count"]):
                if current_offset + 70 > len(buffer): break
                name_bytes = buffer[current_offset + 20 : current_offset + 20 + 40] # SignalSource field
                try:
                    name = name_bytes.decode('ascii', errors='ignore').split('\x00')[0].strip()
                    parsed_channel_names.append(name if name else f"CH_{i+1}")
                except:
                    parsed_channel_names.append(f"CH_{i+1}_Err")
                current_offset += 70
            self.header_info["channel_names_ordered"] = parsed_channel_names

            self.logger.info(f"Header Parsed: TargetSize={self.header_info['target_header_size']}, "
                             f"Version={self.header_info.get('header_version', 'N/A')}, "
                             f"Channels={self.header_info['channel_count']}, "
                             f"Rate={self.header_info['data_rate_hz']}Hz, "
                             f"Names={self.header_info['channel_names_ordered']}")
            return True
        except Exception as e:
            self.logger.error(f"Error parsing MCell header: {e}. Buffer (first 64B): {buffer[:64].hex()}")
            return False

    async def _notification_handler(self, sender, data: bytearray):
        if not self.header_info["is_fully_received"]:
            self.header_info["header_buffer"].extend(data)
            current_buffered_len = len(self.header_info["header_buffer"])
            
            # Initial parse for target_header_size if not known
            if self.header_info["target_header_size"] == 0 and current_buffered_len >= 4:
                try:
                    size = int.from_bytes(self.header_info["header_buffer"][0:4], byteorder='big')
                    if 0 < size < 2048: self.header_info["target_header_size"] = size
                except: pass

            if self.header_info["target_header_size"] > 0 and \
               current_buffered_len >= self.header_info["target_header_size"]:
                if not self.header_info["is_fully_received"]: # Process only once
                    if self._parse_mcell_header():
                        self.header_info["is_fully_received"] = True
                        self.header_parsing_complete_event.set()
                        self.logger.info("Full header received and parsed.")
                        # Process any data appended with the last header packet
                        remaining_data = self.header_info["header_buffer"][self.header_info["target_header_size"]:]
                        if remaining_data and self.expecting_sample_data_flag:
                            self._process_ble_data_as_emg(remaining_data)
                    else:
                        self.logger.error("Failed to parse critical fields from header. Streamer might not work correctly.")
                        # Potentially stop if header is essential and failed
            return # Still in header phase or just finished

        if self.expecting_sample_data_flag:
            self._process_ble_data_as_emg(data)

    def _parse_single_mcell_datagroup(self, raw_ble_bytes_for_datagroup, num_channels):
        """Parses one datagroup from MCell BLE bytes into a list of floats."""
        values = []
        if len(raw_ble_bytes_for_datagroup) != num_channels * 2:
            self.logger.warning(f"BLE datagroup size mismatch. Expected {num_channels * 2}, got {len(raw_ble_bytes_for_datagroup)}")
            return None
        for i in range(num_channels):
            val_raw = int.from_bytes(raw_ble_bytes_for_datagroup[i*2 : i*2+2], byteorder='little', signed=False)
            if i == 0: # Channel 1: Apply sync mark correction
                if 0x5000 <= val_raw < 0x6000: val_processed = float(val_raw - 0x5000)
                else:
                    self.logger.debug(f"CH1 value {val_raw} (0x{val_raw:04X}) missing sync mark. Group: {raw_ble_bytes_for_datagroup.hex()}")
                    return None 
            else:
                val_processed = float(val_raw) # Convert to float for pickling
            values.append(val_processed)
        return values

    def _process_ble_data_as_emg(self, ble_data_packet: bytearray):
        ch_count = self.header_info.get("channel_count", 0)
        if ch_count <= 0:
            self.logger.warning("Channel count not known or zero, cannot process EMG packet.")
            return

        datagroup_size_bytes = ch_count * 2
        num_datagroups_in_packet = len(ble_data_packet) // datagroup_size_bytes

        if num_datagroups_in_packet == 0 and ble_data_packet:
            self.logger.debug(f"BLE packet too short for a full datagroup: {len(ble_data_packet)} bytes.")
            return

        for i in range(num_datagroups_in_packet):
            chunk_start = i * datagroup_size_bytes
            chunk_end = chunk_start + datagroup_size_bytes
            chunk = ble_data_packet[chunk_start:chunk_end]
            
            datagroup_float_values = self._parse_single_mcell_datagroup(chunk, ch_count)

            if datagroup_float_values:
                self.datagroup_count += 1
                try:
                    # LibEMG example streamer uses pickle.dumps(list(emg))
                    # where emg is a tuple/list of channel values for one time point.
                    pickled_datagroup = pickle.dumps(datagroup_float_values)
                    self.udp_socket.sendto(pickled_datagroup, (self.udp_host, self.udp_port))
                    if self.datagroup_count % self.header_info.get("data_rate_hz", 100) == 0: # Log once per second
                        self.logger.info(f"Sent datagroup #{self.datagroup_count} via UDP. First val: {datagroup_float_values[0]:.2f}")
                except Exception as e:
                    self.logger.error(f"Error pickling or sending datagroup via UDP: {e}")
            # else: self.logger.debug(f"Failed to parse datagroup from chunk: {chunk.hex()}")


    async def start_streaming(self):
        self.logger.info(f"Streamer for {self.device_name} ({self.ble_address}) starting.")
        try:
            self.logger.info("Attempting BLE connect...")
            await self.client.connect(timeout=15.0)
            if not self.client.is_connected:
                self.logger.error("BLE connection failed.")
                return

            self.logger.info("BLE Connected. Subscribing to notifications...")
            await self.client.start_notify(self.char_uuid, self._notification_handler)
            
            self.logger.info("Sending 'Prepare Measurement' command...")
            await self.client.write_gatt_char(self.char_uuid, self.commands['prepare'], response=True)
            self.logger.info("'Prepare Measurement' sent. Waiting for header...")

            try:
                await asyncio.wait_for(self.header_parsing_complete_event.wait(), timeout=self.header_timeout_sec)
                self.logger.info("Header successfully received and parsed.")
            except asyncio.TimeoutError:
                self.logger.error(f"Timeout waiting for full header. Processed {self.header_info['received_header_size']}/{self.header_info['target_header_size']}B.")
                if self.header_info["channel_count"] == 0: # Critical failure
                    return # Stop if essential header info missing
                self.header_info["is_fully_received"] = True # Proceed if some info was parsed

            if not self.header_info["is_fully_received"] or self.header_info["channel_count"] == 0:
                 self.logger.error("Header not parsed or channel count is 0. Cannot stream data.")
                 return

            self.logger.info("Sending 'Start Stream Data' command...")
            await self.client.write_gatt_char(self.char_uuid, self.commands['start_stream'], response=True)
            self.expecting_sample_data_flag = True
            self.logger.info("Data streaming started. Sending data via UDP...")

            start_time_monotonic = time.monotonic()
            while True:
                if self.stop_event_mp and self.stop_event_mp.is_set():
                    self.logger.info("External stop event received.")
                    break
                if self.run_duration_sec and (time.monotonic() - start_time_monotonic > self.run_duration_sec):
                    self.logger.info(f"Run duration {self.run_duration_sec}s reached.")
                    break
                if not self.client.is_connected:
                    self.logger.warning("BLE disconnected during streaming.")
                    if self.stop_event_mp: self.stop_event_mp.set() # Ensure main process knows
                    break
                await asyncio.sleep(0.1) # Keep alive, check stop event

        except BleakError as e: self.logger.error(f"BleakError: {e}", exc_info=True)
        except asyncio.CancelledError: self.logger.info("Streaming task cancelled.")
        except Exception as e: self.logger.error(f"Unexpected error in streamer: {e}", exc_info=True)
        finally:
            self.logger.info("Streamer cleanup...")
            if self.client and self.client.is_connected:
                self.logger.info("Sending BLE stop command...")
                try:
                    await asyncio.wait_for(self.client.write_gatt_char(self.char_uuid, self.commands['stop'], response=True), timeout=5.0)
                except: pass # Ignore errors on stop command during shutdown
                try: await asyncio.wait_for(self.client.stop_notify(self.char_uuid), timeout=5.0)
                except: pass
                try: await asyncio.wait_for(self.client.disconnect(), timeout=10.0)
                except: pass
            if self.udp_socket: self.udp_socket.close()
            self.logger.info(f"Streamer for {self.device_name} stopped.")

def run_streamer_process(ble_address, device_name, char_uuid, commands,
                         udp_host, udp_port, header_timeout, run_duration, stop_event_mp):
    """Target function for multiprocessing.Process"""
    # Setup logging for this specific process if needed, or rely on root
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(processName)s] - %(message)s')
    
    streamer = MCellBLEUDPStreamer(ble_address, device_name, char_uuid, commands,
                                  udp_host, udp_port, header_timeout, run_duration, stop_event_mp)
    asyncio.run(streamer.start_streaming())

if __name__ == "__main__":
    import os
    import threading

    # This __main__ is for testing one streamer.
    # The libemg_processor_main.py will launch these as processes.
    print("--- Standalone MCell BLE to UDP Streamer Test ---")

    # Set BLE_ADDR_TEST in your environment to test against a physical MCell unit.
    test_device_config = {
        "ble_address": os.getenv("BLE_ADDR_TEST", "00:00:00:00:00:02"),
        "udp_port": 50002,
        "name": "Shirt01-Right-TestStreamer",
        # num_channels and data_rate_hz will be determined from MCell header
    }
    dummy_stop_event = threading.Event()


    # Load commands from a conceptual config for consistency
    # (These would be defined in the actual config.py used by the main launcher)
    _CMD_PREPARE_MEASUREMENT = b'\x6F\x6D\x34\x34\x30\x30\x31\x0D'
    _CMD_START_STREAM_DATA = b'\x6F\x6D\x74\x0D'
    _CMD_STOP_MEASUREMENT = b'\x6F\x6D\x6E\x0D'
    _COMMAND_DICT = {"prepare": _CMD_PREPARE_MEASUREMENT, "start_stream": _CMD_START_STREAM_DATA, "stop": _CMD_STOP_MEASUREMENT}
    _CHAR_UUID = "eba11dc2-6fab-479b-a742-1a2dfad8f122"
    _UDP_HOST = "127.0.0.1"
    _HEADER_TIMEOUT = 20
    _RUN_DURATION = 60 # Run for 60 seconds for test

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(processName)s:%(threadName)s] - [%(name)s] - %(message)s')

    # Note: If running this standalone, multiprocessing might not be set up.
    # This is primarily designed to be called by the main processor script.
    # For a quick test without multiprocessing module fully imported:
    if 'multiprocessing' not in dir() or not hasattr(multiprocessing, 'Event'):
        import threading
        dummy_stop_event = threading.Event()
        print("Running standalone test with threading.Event for stop_event.")
    
    try:
        run_streamer_process(
            test_device_config["ble_address"],
            test_device_config["name"],
            _CHAR_UUID,
            _COMMAND_DICT,
            _UDP_HOST,
            test_device_config["udp_port"],
            _HEADER_TIMEOUT,
            _RUN_DURATION,
            dummy_stop_event
        )
    except KeyboardInterrupt:
        print("Standalone streamer test interrupted.")
        dummy_stop_event.set()
    print("--- Standalone Streamer Test Finished ---")