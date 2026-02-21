#!/usr/bin/env python3
"""
Mock Sybase SQL Anywhere server for testing purposes.

This is a placeholder that simulates basic TDS protocol responses.
For actual integration testing, you would need a real SQL Anywhere instance.

Note: SAP SQL Anywhere is commercial software and requires a license.
For development, you can:
1. Use the SQL Anywhere Developer Edition (free for development)
2. Use a trial version
3. Connect to an existing SQL Anywhere instance
"""

import socket
import struct
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TDS Protocol constants
TDS_PRELOGIN = 0x12
TDS_LOGIN7 = 0x10
TDS_QUERY = 0x01
TDS_RESPONSE = 0x04


class MockTDSServer:
    """
    A minimal mock TDS server for testing connection handling.

    This does NOT implement the full TDS protocol - it's meant to verify
    that the FDW can establish connections. For actual data testing,
    use a real SQL Anywhere instance.
    """

    def __init__(self, host="0.0.0.0", port=2638):
        self.host = host
        self.port = port
        self.socket = None
        self.running = False

    def start(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)
        self.running = True

        logger.info(f"Mock Sybase TDS server listening on {self.host}:{self.port}")

        while self.running:
            try:
                client, address = self.socket.accept()
                logger.info(f"Connection from {address}")
                thread = threading.Thread(target=self.handle_client, args=(client,))
                thread.daemon = True
                thread.start()
            except socket.error:
                break

    def handle_client(self, client):
        """Handle a client connection."""
        try:
            while True:
                # Read TDS packet header (8 bytes)
                header = client.recv(8)
                if not header:
                    break

                packet_type = header[0]
                length = struct.unpack(">H", header[2:4])[0]

                # Read the rest of the packet
                remaining = length - 8
                if remaining > 0:
                    data = client.recv(remaining)
                else:
                    data = b""

                logger.info(f"Received packet type: {packet_type}, length: {length}")

                # Send a basic response
                response = self.create_response(packet_type, data)
                if response:
                    client.send(response)

        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            client.close()

    def create_response(self, packet_type, data):
        """Create a basic TDS response."""
        if packet_type == TDS_PRELOGIN:
            return self.create_prelogin_response()
        elif packet_type == TDS_LOGIN7:
            return self.create_login_response()
        elif packet_type == TDS_QUERY:
            return self.create_query_response()
        return None

    def create_prelogin_response(self):
        """Create a minimal prelogin response."""
        prelogin_data = bytes(
            [
                0x00, 0x00, 0x1A, 0x00, 0x06,  # VERSION token
                0x01, 0x00, 0x20, 0x00, 0x01,  # ENCRYPTION token
                0x02, 0x00, 0x21, 0x00, 0x01,  # INSTOPT token
                0xFF,                           # Terminator
                0x0F, 0x00, 0x07, 0xD1, 0x00, 0x00,  # VERSION value
                0x00,  # ENCRYPTION value (0 = off)
                0x00,  # INSTOPT value
            ]
        )
        return self.create_packet(TDS_RESPONSE, prelogin_data)

    def create_login_response(self):
        """Create a login acknowledgment response."""
        loginack = bytes(
            [
                0xAD,              # LOGINACK token
                0x00, 0x24,        # Length
                0x01,              # Interface (SQL Server)
                0x74, 0x00, 0x00, 0x04,  # TDS version 7.4
                0x10,              # Program name length
            ]
        )
        loginack += b"SQL Anywhere\x00\x00\x00\x00"  # Program name (16 bytes)
        loginack += bytes([0x10, 0x00, 0x00, 0x11])   # Major.Minor.Build version

        done = bytes(
            [
                0xFD,              # DONE token
                0x00, 0x00,        # Status
                0x00, 0x00,        # CurCmd
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  # DoneRowCount
            ]
        )
        return self.create_packet(TDS_RESPONSE, loginack + done)

    def create_query_response(self):
        """Create an empty query response."""
        done = bytes(
            [
                0xFD,              # DONE token
                0x00, 0x00,        # Status
                0x00, 0x00,        # CurCmd
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  # DoneRowCount
            ]
        )
        return self.create_packet(TDS_RESPONSE, done)

    def create_packet(self, packet_type, data):
        """Create a TDS packet with header."""
        length = 8 + len(data)
        header = bytes([packet_type, 0x01])
        header += struct.pack(">H", length)
        header += bytes([0x00, 0x00, 0x01, 0x00])
        return header + data

    def stop(self):
        self.running = False
        if self.socket:
            self.socket.close()


if __name__ == "__main__":
    server = MockTDSServer()
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        server.stop()
