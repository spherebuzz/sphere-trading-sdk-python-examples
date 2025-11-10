import sys
import os
import logging
import getpass
import uuid
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass, field
from typing import List

try:
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_script_dir, '..'))
    src_dir = os.path.join(project_root, 'src')

    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    from sphere_sdk.sphere_client import (
        SphereTradingClientSDK,
        SDKInitializationError,
        LoginFailedError,
        TradingClientError,
        CreateOrderFailedError
    )
    from sphere_sdk import sphere_sdk_types_pb2
except ImportError as e:
    print(f"Error importing SDK modules: {e}")
    print(f"Please ensure 'sphere_sdk' is in PYTHONPATH or the structure is correct.")
    print(f"Attempted to add '{src_dir}' to sys.path.")
    sys.exit(1)

logger = logging.getLogger("order_creator")
logging.basicConfig(
    level=logging.debug,
    format='[%(levelname)s] (%(name)s) %(asctime)s: %(message)s'
)

class OrderSubmissionTool:
    """
    Manages interactive prompting for order details and submitting them to Sphere.
    """
    def __init__(self, sdk_client: SphereTradingClientSDK):
        """
        Initializes the OrderSubmissionTool.

        Args:
            sdk_client: An initialized and logged-in instance of SphereTradingClientSDK.
        """
        self.sdk = sdk_client

    def prompt_and_submit_orders(self):
        """Interactively prompts the user to create and submit new orders."""
        logger.info("--- New Order Submission ---")
        logger.info("Enter details for your orders. Type 'done' when finished.")
        while True:
            instrument_name = input("\nEnter Instrument Name (e.g., 'Naphtha MOPJ') or 'done': ")
            if instrument_name.lower() == 'done':
                break

            front_expiry = input(f"Enter Front Expiry for {instrument_name} (e.g., 'Oct-25'): ")
            back_expiry = input(f"Enter Back Expiry for {instrument_name} (e.g., 'Oct-25'): ")

            side_str = ""
            while side_str not in ['buy', 'sell']:
                side_str = input("Enter Side ('buy' or 'sell'): ").lower()

            side = (sphere_sdk_types_pb2.ORDER_SIDE_BID if side_str == 'buy'
                    else sphere_sdk_types_pb2.ORDER_SIDE_ASK)

            quantity_str = input("Enter Quantity: ")
            per_price_unit_str = input("Enter Price (e.g., '100'): ")

            primary_broker_code = input(f"Enter Primary Broker Code: ")

            secondary_broker_codes = []
            while True:
                add_secondary = input("Add a secondary broker? (yes/no): ").lower()
                if add_secondary == 'yes':
                    secondary_broker_codes.append(input(f"Enter Secondary Broker Code: "))
                else:
                    break

            clearing_options = []
            while True:
                add_clearing = input("Add a clearing option code? (yes/no): ").lower()
                if add_clearing == 'yes':
                    code = input("Enter Clearing Option Code (e.g., 'ICE'): ")
                    clearing_options.append(code)
                else:
                    break

            try:
                per_price_unit = Decimal(per_price_unit_str)
                quantity = Decimal(quantity_str)

                idempotency_key = str(uuid.uuid4())

                price_dto = sphere_sdk_types_pb2.OrderRequestPriceDto(
                    per_price_unit=str(per_price_unit),
                    quantity=str(quantity),
                    ordered_clearing_options=[
                        sphere_sdk_types_pb2.OrderRequestClearingOptionDto(code=code)
                        for code in clearing_options
                    ]
                )

                primary_broker_dto = sphere_sdk_types_pb2.OrderRequestBrokerDto(
                    code=primary_broker_code
                )

                secondary_brokers_dtos = [
                    sphere_sdk_types_pb2.OrderRequestBrokerDto(code=b)
                    for b in secondary_broker_codes
                ]

                parties_dto = sphere_sdk_types_pb2.TraderOrderRequestPartiesDto(
                    primary_broker=primary_broker_dto,
                    secondary_brokers=secondary_brokers_dtos
                )

                new_order_request = sphere_sdk_types_pb2.TraderSpreadOrderRequestDto(
                    idempotency_key=idempotency_key,
                    side=side,
                    front_expiry=front_expiry,
                    back_expiry=back_expiry,
                    instrument_name=instrument_name,
                    price=price_dto,
                    parties=parties_dto
                )
                
                logger.info(f"Prepared order: {new_order_request}")
                self._submit_order(new_order_request)

            except (InvalidOperation, ValueError) as e:
                logger.error(f"Invalid input for price/quantity: {e}. Please try again.")
            except CreateOrderFailedError as e:
                logger.error(f"Failed to submit order: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred during order creation: {e}", exc_info=True)

            print("-" * 20)

        logger.info("Finished submitting orders.")

    def _submit_order(self, sdk_order_request: sphere_sdk_types_pb2.TraderSpreadOrderRequestDto):
        """
        Submit new order request.
        """
        logger.info(f"Submitting order with idempotency_key: {sdk_order_request.idempotency_key}")
        
        try:
            orderResponse = self.sdk.create_trader_spread_order(sdk_order_request)
            logger.info(f"Successfully submitted order. Order ID: {orderResponse.id}, Instance ID: {orderResponse.instance_id}")
        except CreateOrderFailedError as e:
            logger.error(f"Failed to submit order for {sdk_order_request.instrument_name} {sdk_order_request.front_expiry} - {sdk_order_request.back_expiry} "
                         f"({sphere_sdk_types_pb2.OrderSide.Name(sdk_order_request.side)} @ {sdk_order_request.price.per_price_unit}): {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while submitting order: {e}", exc_info=True)
            raise

def main():
    """
    Main function to initialize the SDK, log in, and run the order submission tool.
    """
    logger.info("Starting Sphere Interactive Order Creator...")
    sdk_instance = None
    try:
        sdk_instance = SphereTradingClientSDK()
        logger.info("SDK initialized.")

        username = input("Enter username: ")
        password = getpass.getpass("Enter password: ")
        sdk_instance.login(username, password)
        logger.info(f"Login successful for user '{username}'.")

        order_tool = OrderSubmissionTool(sdk_instance)
        order_tool.prompt_and_submit_orders()

    except KeyboardInterrupt:
        logger.info("\nCtrl+C detected. Shutting down gracefully...")
    except (SDKInitializationError, LoginFailedError, LoginFailedError, TradingClientError) as e:
        logger.error(f"A critical SDK error occurred: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        if sdk_instance and sdk_instance._is_logged_in:
            logger.info("Logging out...")
            sdk_instance.logout()
            logger.info("Logout complete.")

        logger.info("Sphere Interactive Order Creator has finished.")


if __name__ == "__main__":
    main()