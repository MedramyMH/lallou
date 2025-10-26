"""
IQ Option API Compatibility Fix
Fixes websocket client compatibility issues
"""

import logging
from iqoptionapi.ws.client import WebsocketClient

# Monkey patch the WebsocketClient to fix the on_message method
original_on_message = WebsocketClient.on_message

def patched_on_message(self, ws, message=None):
    """Patched on_message method to handle both 2 and 3 argument calls"""
    try:
        if message is None:
            # If called with 2 arguments, ws is actually the message
            message = ws
            ws = self.wss
        
        # Call the original method with the correct arguments
        if hasattr(self, '_on_message'):
            return self._on_message(message)
        else:
            # Fallback to original behavior
            return original_on_message(self, message)
    except Exception as e:
        logging.error(f"Error in patched on_message: {e}")

# Apply the patch
WebsocketClient._on_message = WebsocketClient.on_message
WebsocketClient.on_message = patched_on_message

def apply_iqoption_fixes():
    """Apply all necessary fixes for IQ Option API compatibility"""
    logging.info("Applied IQ Option API compatibility fixes")