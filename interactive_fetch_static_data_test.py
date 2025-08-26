import sys
import os
import logging
import getpass
import time
from ctypes import create_string_buffer

# --- Path setup and initial imports (all correct) ---
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_script_dir, '..'))
src_dir = os.path.join(project_root, 'src')

if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

try:
    from sphere_sdk.sphere_client import (
        SphereTradingClientSDK,
        SDKInitializationError,
        LoginFailedError,
        NotLoggedInError,
        TradingClientError
    )
    from sphere_sdk import sphere_sdk_types_pb2
except ImportError as e:
    print(f"Error importing SDK modules: {e}")
    print(f"Please ensure 'sphere_sdk' is in PYTHONPATH or the structure is correct.")
    print(f"Attempted to add '{src_dir}' to sys.path.")
    sys.exit(1)

test_logger = logging.getLogger("interactive_test")
logging.basicConfig(level=logging.INFO, format='[TEST_SCRIPT %(levelname)s] %(asctime)s: %(message)s')

def get_instruments(sdk: SphereTradingClientSDK):
    """
    Tests the get_instruments SDK method and prints the results.
    Returns the list of instruments on success, otherwise None.
    """
    test_logger.info("\n--- Testing get_instruments() ---")
    try:
        instruments = sdk.get_instruments()

        if not instruments:
            test_logger.warning("Call successful, but no instruments were returned from the backend.")
            return []

        test_logger.info(f"SUCCESS: Received {len(instruments)} instruments.")
        for i, instrument in enumerate(instruments[:5]):
            test_logger.info(f"  - [{i+1}] {instrument.name}")
        if len(instruments) > 5:
            test_logger.info("  - ... and more.")
        
        return instruments

    except TradingClientError as e:
        test_logger.error(f"An SDK error occurred while getting instruments: {e}", exc_info=True)
        return None


def get_expiries(sdk: SphereTradingClientSDK, instruments: list):
    """
    Tests the get_expiries_by_instrument_name SDK method for a valid instrument name.
    """
    if not instruments:
        test_logger.warning("\n--- Skipping expiry tests because no instruments were found. ---")
        return

    instrument_to_test = instruments[0]
    test_logger.info(f"\n--- Testing get_expiries_by_instrument_name() for a VALID instrument: '{instrument_to_test.name}' ---")
    try:
        expiries = sdk.get_expiries_by_instrument_name(instrument_to_test.name)
        
        if not expiries:
            test_logger.warning(f"Call successful, but no expiries were returned for '{instrument_to_test.name}'.")
        else:
            test_logger.info(f"SUCCESS: Received {len(expiries)} expiries for '{instrument_to_test.name}'.")
            for expiry in expiries[:5]:
                test_logger.info(f"  - {expiry.name}")
            if len(expiries) > 5:
                test_logger.info("  - ... and more.")

    except TradingClientError as e:
        test_logger.error(f"An SDK error occurred while getting expiries for a valid instrument: {e}", exc_info=True)

    print("    ")

def get_brokers(sdk: SphereTradingClientSDK):
    """
    Tests the get_brokers SDK method and prints the results.
    Returns the list of brokers on success, otherwise None.
    """
    test_logger.info("\n--- Testing get_brokers() ---")
    try:
        brokers = sdk.get_brokers()

        if not brokers:
            test_logger.warning("Call successful, but no brokers were returned from the backend.")
            return []

        test_logger.info(f"SUCCESS: Received {len(brokers)} brokers.")
        for i, broker in enumerate(brokers[:5]):
            test_logger.info(f"  - [{i+1}] {broker.name} (Code: {broker.code})")
        if len(brokers) > 5:
            test_logger.info("  - ... and more.")
        
        return brokers

    except TradingClientError as e:
        test_logger.error(f"An SDK error occurred while getting brokers: {e}", exc_info=True)
        return None

def get_clearing_options(sdk: SphereTradingClientSDK):
    """
    Tests the get_clearing_options SDK method and prints the results.
    Returns the list of clearing options on success, otherwise None.
    """
    test_logger.info("\n--- Testing get_clearing_options() ---")
    try:
        clearing_options = sdk.get_clearing_options()

        if not clearing_options:
            test_logger.warning("Call successful, but no clearing options were returned from the backend.")
            return []

        test_logger.info(f"SUCCESS: Received {len(clearing_options)} clearing options.")
        for i, clearing_option in enumerate(clearing_options[:5]):
            test_logger.info(f"  - [{i+1}] {clearing_option.code}")
        if len(clearing_options) > 5:
            test_logger.info("  - ... and more.")
        
        return clearing_options

    except TradingClientError as e:
        test_logger.error(f"An SDK error occurred while getting clearing options: {e}", exc_info=True)
        return None


def main():
    test_logger.info("Starting Interactive SDK Test Script...")

    sdk_instance = None

    try:
        sdk_instance = SphereTradingClientSDK()
        test_logger.info("SDK initialized.")

        username = input("Enter username: ")
        password = getpass.getpass("Enter password: ")
        sdk_instance.login(username, password)
        test_logger.info(f"Login successful for user '{username}'.")

        available_instruments = get_instruments(sdk_instance)
        
        if available_instruments is not None:
            get_expiries(sdk_instance, available_instruments)

        get_brokers(sdk_instance)

        get_clearing_options(sdk_instance)

    except KeyboardInterrupt:
        test_logger.info("\nCtrl+C detected. Shutting down...")
    except (SDKInitializationError, LoginFailedError, NotLoggedInError, TradingClientError) as e:
        test_logger.error(f"A critical SDK error occurred: {e}", exc_info=True)
    except Exception as e:
        test_logger.error(f"An unexpected error occurred in the main script: {e}", exc_info=True)
    finally:
        if sdk_instance and sdk_instance._is_logged_in:
            test_logger.info("\nLogging out...")
            sdk_instance.logout()
            test_logger.info("Logout complete.")

        test_logger.info("Sphere Instrument Data Test Script has finished.")
    

if __name__ == "__main__":
    main()