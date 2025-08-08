import sys
import os
import logging
import getpass
import time
from google.protobuf.json_format import MessageToDict

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

def on_trade_event_received(trade_data: sphere_sdk_types_pb2.TradeMessageDto):
    """
    Callback function to handle incoming trade data payloads.
    """
    test_logger.info("<<< Received Trade Data Payload >>>")
    
    event_type_str = sphere_sdk_types_pb2.TradeEventType.Name(trade_data.event_type).replace('TRADE_EVENT_TYPE_', '')
    
    if event_type_str == 'SNAPSHOT':
        test_logger.info("Event Type: SNAPSHOT")
        snapshot_body = trade_data.body
        if snapshot_body:
            pretty_details = format_trade_snapshot(snapshot_body) 
            test_logger.info(f"\n{pretty_details}")
        else:
            test_logger.info("Trade snapshot is empty.")
    else:
        test_logger.info("Event Type:", event_type_str)
        delta_body = trade_data.body
        if delta_body:
            pretty_details = format_trade_snapshot(delta_body) 
            test_logger.info(f"\n{pretty_details}")


    test_logger.info("---------------------------------")

def format_trade_snapshot(snapshot_body: list[sphere_sdk_types_pb2.TradeDto]) -> str:
    """Helper function to format the trade snapshot for pretty printing."""
    lines = []
    for i, trade_details in enumerate(snapshot_body):
        contract = trade_details.contract
        price = trade_details.price

        lines.append(f"--- Contract {i+1}/{len(snapshot_body)} ---")
        
        inst_type_str = sphere_sdk_types_pb2.InstrumentType.Name(contract.instrument_type).replace('INSTRUMENT_TYPE_', '')

        lines.append(f"  Instrument: {contract.instrument_name} ({inst_type_str})")
        lines.append(f"  Expiry:     {contract.expiry}")
        
        if contract.legs:
            lines.append("  Legs:")
            for leg in contract.legs:
                lines.append(f"Expiry: {leg.expiry}")
      
                        
        unit_str = sphere_sdk_types_pb2.Unit.Name(trade_details.price.units).replace('UNIT_', '')                
        unit_period_str = sphere_sdk_types_pb2.Unit.Name(trade_details.price.unit_period).replace('UNIT_PERIOD_', '')

        lines.append(
            f"    - ID: {trade_details.id} | "
            f"Qty: {price.quantity:>10} @ "
            f"Unit: {unit_str:>10} | "
            f"Unit Period: {unit_period_str:>10} | "
            f"Price: {price.per_price_unit:>8} | "
            f"Updated: {trade_details.updated_time}"              
        )

        lines.append("-" * 25)
        
    return "\n".join(lines)

def main():
    test_logger.info("Starting Interactive SDK Test Script...")

    sdk_instance = None

    try:
        sdk_instance = SphereTradingClientSDK()
        test_logger.info("SDK Initialized successfully.")

        username = input("Enter username: ")
        password = getpass.getpass("Enter password: ")

        test_logger.info(f"Attempting login for user '{username}'...")
        sdk_instance.login(username, password)
        test_logger.info(f"Login successful for '{username}'.")

        try:
            test_logger.info("Subscribing to trade events...")
            sdk_instance.subscribe_to_trade_events(on_trade_event_received)
            test_logger.info("Successfully subscribed. Listening for events...")
            test_logger.info("Press Ctrl+C to logout and exit.")

            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            test_logger.info("\nCtrl+C detected. Proceeding to logout...")
        finally:
            if sdk_instance and sdk_instance._is_logged_in and sdk_instance._user_trade_callback:
                test_logger.info("Unsubscribing from trade events...")
                try:
                    sdk_instance.unsubscribe_from_trade_events()
                except TradingClientError as e:
                    test_logger.warning(f"Error during explicit unsubscription: {e}")

    except (SDKInitializationError, LoginFailedError, TradingClientError) as e:
        test_logger.error(f"A critical SDK error occurred: {e}")
    except Exception as e:
        test_logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if sdk_instance and sdk_instance._is_logged_in:
            test_logger.info("Logging out...")
            sdk_instance.logout()
            test_logger.info("Logout complete.")
        elif sdk_instance:
            test_logger.info("SDK was initialized but not logged in or already logged out.")
        else:
            test_logger.info("SDK was not initialized.")
        test_logger.info("Interactive SDK Test Script finished.")

if __name__ == "__main__":
    main()