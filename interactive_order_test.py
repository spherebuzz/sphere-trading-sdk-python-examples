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

def on_order_event_received(order_data: sphere_sdk_types_pb2.OrderStacksDto):
    """
    Callback function to handle incoming order data payloads.
    """
    test_logger.info("<<< Received Order Data Payload >>>")

    event_type_str = sphere_sdk_types_pb2.OrderStacksEventType.Name(order_data.event_type).replace('ORDER_STACKS_EVENT_TYPE_', '')

    print("Event Type: ", event_type_str)

    if event_type_str == 'SNAPSHOT':
        test_logger.info("Event Type: SNAPSHOT")
        snapshot_body = order_data.body
        if snapshot_body:
            pretty_details = format_order_stacks(snapshot_body)
            test_logger.info(f"\n{pretty_details}")
        else:
            test_logger.info("Order snapshot is empty.")
    else:
        test_logger.info(f"Event Type: {event_type_str}")
        delta_body = order_data.body
        if delta_body:
            pretty_details = format_order_stacks(delta_body)
            test_logger.info(f"\n{pretty_details}")

    test_logger.info("---------------------------------")

def format_order_stacks(snapshot_body: list[sphere_sdk_types_pb2.OrderStackDto]) -> str:
    """Helper function to format the order snapshot for pretty printing."""
    lines = []
    label_width = 12
    for i, contract_details in enumerate(snapshot_body):
        contract = contract_details.contract
        orders = contract_details.orders

        lines.append(f"--- Contract {i+1}/{len(snapshot_body)} ---")

        inst_type_str = sphere_sdk_types_pb2.InstrumentType.Name(contract.instrument_type).replace('INSTRUMENT_TYPE_', '')
        expiry_type_str = sphere_sdk_types_pb2.ExpiryType.Name(contract.expiry_type).replace('EXPIRY_TYPE_', '')
        side_str = sphere_sdk_types_pb2.OrderSide.Name(contract.side).replace('ORDER_SIDE_', '')

        lines.append(f"  {'Instrument:':<{label_width}}{contract.instrument_name} ({inst_type_str})")
        lines.append(f"  {'Expiry:':<{label_width}}{contract.expiry} ({expiry_type_str})")
        lines.append(f"  {'Side:':<{label_width}}{side_str}")

        if contract.constituents:
            lines.append(f"  {'Constituents:':<{label_width}}")
            for const in contract.constituents:
                lines.append(f"    - {const.expiry}")

        if contract.legs:
            lines.append(f"  {'Legs:':<{label_width}}")
            for j, leg in enumerate(contract.legs, 1):
                side = sphere_sdk_types_pb2.SpreadSideType.Name(leg.spread_side).replace('SPREAD_SIDE_TYPE_', '')
                leg_expiry_type_str = sphere_sdk_types_pb2.LegExpiryType.Name(leg.expiry_type).replace('LEG_EXPIRY_TYPE_', '')
                instrument_name = leg.instrument_name or 'N/A'
                expiry = leg.expiry or 'N/A'
                lines.append(f"    - Leg {j} ({side}): {instrument_name} @ {expiry} ({leg_expiry_type_str})")
                if leg.constituents:
                    lines.append(f"      {'Constituents:':<{label_width}}")
                    for const in leg.constituents:
                        lines.append(f"        - {const.expiry}")

        if orders:
            lines.append(f"  Orders ({len(orders)}):")
            for order in orders:
                interest_type_str = sphere_sdk_types_pb2.InterestType.Name(order.interest_type).replace('INTEREST_TYPE_', '')
                tradability_str = sphere_sdk_types_pb2.Tradability.Name(order.tradability).replace('TRADABILITY_', '')
                unit_str = sphere_sdk_types_pb2.Unit.Name(order.price.units).replace('UNIT_', '')
                unit_period_str = sphere_sdk_types_pb2.UnitPeriod.Name(order.price.unit_period).replace('UNIT_PERIOD_', '')

                quantity_details_str = f"{order.price.quantity}"
                if unit_str != 'NONE':
                    quantity_details_str += f" {unit_str}"
                    if unit_period_str not in ['NONE', 'NOT_APPLICABLE', 'TOTAL_VOLUME']:
                        quantity_details_str += f"/{unit_period_str}"
                    elif unit_period_str == 'TOTAL_VOLUME':
                        quantity_details_str += " (Total Volume)"

                lines.append(
                    f"    - ID: {order.id} | Instance ID: {order.instance_id} | "
                    f"Qty: {quantity_details_str:<30} | "
                    f"Price: {order.price.per_price_unit:>8} | "
                    f"Interest: {interest_type_str:<10} | "
                    f"Tradable: {tradability_str:<10} | "
                    f"Updated: {order.updated_time} | "
                    f"Stack Position: {order.stack_position}"
                )

                if order.HasField('parties'):
                    parts = []

                    if order.parties.HasField('initiator_trader'):
                        t = order.parties.initiator_trader
                        if t.full_name or t.company_name:
                            parts.append(f"Initiator Trader: {t.full_name} ({t.company_name})")

                    if order.parties.HasField('initiator_broker'):
                        b = order.parties.initiator_broker
                        if b.company_name:
                            parts.append(f"Initiator Broker: {b.company_name}")

                    if order.parties.brokers:
                        codes = [b.code for b in order.parties.brokers if b.code]
                        if codes:
                            broker_list_str = ", ".join(codes)
                            parts.append(f"Brokers: [{broker_list_str}]")

                    if parts:
                        lines.append(" | ".join(parts))
            else:
                lines.append("  (No active orders for this contract)")
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
            test_logger.info("Subscribing to order events...")
            sdk_instance.subscribe_to_order_events(on_order_event_received)
            test_logger.info("Successfully subscribed. Listening for events...")
            test_logger.info("Press Ctrl+C to logout and exit.")

            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            test_logger.info("\nCtrl+C detected. Proceeding to logout...")
        finally:
            if sdk_instance and sdk_instance._is_logged_in and sdk_instance._user_order_callback:
                test_logger.info("Unsubscribing from order events...")
                try:
                    sdk_instance.unsubscribe_from_order_events()
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