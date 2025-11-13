import sys
import os
import logging
import getpass
import uuid
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass, field
from typing import List, Union

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
    level=logging.DEBUG, # Change to INFO for less verbose output during normal operation
    format='[%(levelname)s] (%(name)s) %(asctime)s: %(message)s'
)

# Define a type alias for all possible order request DTOs
OrderRequestDto = Union[
    sphere_sdk_types_pb2.TraderFlatOrderRequestDto,
    sphere_sdk_types_pb2.TraderFlyOrderRequestDto,
    sphere_sdk_types_pb2.TraderSpreadOrderRequestDto,
    sphere_sdk_types_pb2.TraderStripOrderRequestDto
]

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

    def _get_common_order_details(self, instrument_name: str, allow_multiple_brokers: bool = True):
        """Helper to get common order details (side, quantity, price, brokers, clearing)."""
        side_str = ""
        while side_str not in ['buy', 'sell']:
            side_str = input("Enter Side ('buy' or 'sell'): ").lower()

        side = (sphere_sdk_types_pb2.ORDER_SIDE_BID if side_str == 'buy'
                else sphere_sdk_types_pb2.ORDER_SIDE_ASK)

        quantity_str = input("Enter Quantity: ")
        per_price_unit_str = input("Enter Price (e.g., '100'): ")

        primary_broker_code = input(f"Enter Primary Broker Code: ")

        secondary_broker_codes = []
        if allow_multiple_brokers:
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
        
        return side, quantity_str, per_price_unit_str, primary_broker_code, secondary_broker_codes, clearing_options

    def _create_price_parties_dtos(self, quantity_str: str, per_price_unit_str: str, clearing_options: List[str], primary_broker_code: str, secondary_broker_codes: List[str]):
        """Helper to create PriceDto and PartiesDto."""
        per_price_unit = Decimal(per_price_unit_str)
        quantity = Decimal(quantity_str)

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
        return price_dto, parties_dto

    def _submit_order(self, sdk_order_request: OrderRequestDto):
        """
        Submit new order request, dynamically calling the correct SDK method.
        """
        logger.info(f"Submitting order with idempotency_key: {sdk_order_request.idempotency_key}")
        
        try:
            orderResponse = None
            if isinstance(sdk_order_request, sphere_sdk_types_pb2.TraderFlatOrderRequestDto):
                orderResponse = self.sdk.create_trader_flat_order(sdk_order_request)
                order_details = f"{sdk_order_request.instrument_name} {sdk_order_request.expiry}"
            elif isinstance(sdk_order_request, sphere_sdk_types_pb2.TraderFlyOrderRequestDto):
                orderResponse = self.sdk.create_trader_fly_order(sdk_order_request)
                order_details = (f"{sdk_order_request.instrument_name} "
                                 f"{sdk_order_request.first_expiry}/{sdk_order_request.second_expiry}/"
                                 f"{sdk_order_request.third_expiry}")
            elif isinstance(sdk_order_request, sphere_sdk_types_pb2.TraderSpreadOrderRequestDto):
                orderResponse = self.sdk.create_trader_spread_order(sdk_order_request)
                order_details = (f"{sdk_order_request.instrument_name} "
                                 f"{sdk_order_request.front_expiry}-{sdk_order_request.back_expiry}")
            elif isinstance(sdk_order_request, sphere_sdk_types_pb2.TraderStripOrderRequestDto):
                orderResponse = self.sdk.create_trader_strip_order(sdk_order_request)
                order_details = (f"{sdk_order_request.instrument_name} "
                                 f"{sdk_order_request.front_expiry}-{sdk_order_request.back_expiry}")
            else:
                raise ValueError(f"Unknown order request DTO type: {type(sdk_order_request)}")

            logger.info(f"Successfully submitted order. Order ID: {orderResponse.id}, Instance ID: {orderResponse.instance_id}")
        except CreateOrderFailedError as e:
            side_name = sphere_sdk_types_pb2.OrderSide.Name(sdk_order_request.side)
            price_unit = sdk_order_request.price.per_price_unit if hasattr(sdk_order_request, 'price') else "N/A"
            logger.error(f"Failed to submit order for {order_details} ({side_name} @ {price_unit}): {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while submitting order: {e}", exc_info=True)
            raise

    def _prompt_and_submit_flat_order(self):
        logger.info("--- New Flat Order Submission ---")
        instrument_name = input("\nEnter Instrument Name (e.g., 'Naphtha MOPJ'): ")
        expiry = input(f"Enter Expiry for {instrument_name} (e.g., 'Oct-25'): ")

        side, quantity_str, per_price_unit_str, primary_broker_code, secondary_broker_codes, clearing_options = \
            self._get_common_order_details(instrument_name)

        try:
            price_dto, parties_dto = self._create_price_parties_dtos(
                quantity_str, per_price_unit_str, clearing_options, primary_broker_code, secondary_broker_codes
            )
            idempotency_key = str(uuid.uuid4())

            new_order_request = sphere_sdk_types_pb2.TraderFlatOrderRequestDto(
                idempotency_key=idempotency_key,
                side=side,
                expiry=expiry,
                instrument_name=instrument_name,
                price=price_dto,
                parties=parties_dto
            )
            
            logger.info(f"Prepared Flat Order: {new_order_request}")
            self._submit_order(new_order_request)

        except (InvalidOperation, ValueError) as e:
            logger.error(f"Invalid input for price/quantity: {e}. Please try again.")
        except CreateOrderFailedError as e:
            logger.error(f"Failed to submit Flat order: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Flat order creation: {e}", exc_info=True)
        print("-" * 20)

    def _prompt_and_submit_fly_order(self):
        logger.info("--- New Fly Order Submission ---")
        instrument_name = input("\nEnter Instrument Name (e.g., 'Naphtha MOPJ'): ")
        first_expiry = input(f"Enter First Expiry for {instrument_name} (e.g., 'Oct-25'): ")
        second_expiry = input(f"Enter Second Expiry for {instrument_name} (e.g., 'Oct-25'): ")
        third_expiry = input(f"Enter Third Expiry for {instrument_name} (e.g., 'Oct-25'): ")

        side, quantity_str, per_price_unit_str, primary_broker_code, secondary_broker_codes, clearing_options = \
            self._get_common_order_details(instrument_name)

        try:
            price_dto, parties_dto = self._create_price_parties_dtos(
                quantity_str, per_price_unit_str, clearing_options, primary_broker_code, secondary_broker_codes
            )
            idempotency_key = str(uuid.uuid4())

            new_order_request = sphere_sdk_types_pb2.TraderFlyOrderRequestDto(
                idempotency_key=idempotency_key,
                side=side,
                first_expiry=first_expiry,
                second_expiry=second_expiry,
                third_expiry=third_expiry,
                instrument_name=instrument_name,
                price=price_dto,
                parties=parties_dto
            )
            
            logger.info(f"Prepared Fly Order: {new_order_request}")
            self._submit_order(new_order_request)

        except (InvalidOperation, ValueError) as e:
            logger.error(f"Invalid input for price/quantity: {e}. Please try again.")
        except CreateOrderFailedError as e:
            logger.error(f"Failed to submit Fly order: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Fly order creation: {e}", exc_info=True)
        print("-" * 20)

    def _prompt_and_submit_spread_order(self):
        logger.info("--- New Spread Order Submission ---")
        instrument_name = input("\nEnter Instrument Name (e.g., 'Naphtha MOPJ'): ")
        front_expiry = input(f"Enter Front Expiry for {instrument_name} (e.g., 'Oct-25'): ")
        back_expiry = input(f"Enter Back Expiry for {instrument_name} (e.g., 'Oct-25'): ")

        side, quantity_str, per_price_unit_str, primary_broker_code, secondary_broker_codes, clearing_options = \
            self._get_common_order_details(instrument_name)

        try:
            price_dto, parties_dto = self._create_price_parties_dtos(
                quantity_str, per_price_unit_str, clearing_options, primary_broker_code, secondary_broker_codes
            )
            idempotency_key = str(uuid.uuid4())

            new_order_request = sphere_sdk_types_pb2.TraderSpreadOrderRequestDto(
                idempotency_key=idempotency_key,
                side=side,
                front_expiry=front_expiry,
                back_expiry=back_expiry,
                instrument_name=instrument_name,
                price=price_dto,
                parties=parties_dto
            )
            
            logger.info(f"Prepared Spread Order: {new_order_request}")
            self._submit_order(new_order_request)

        except (InvalidOperation, ValueError) as e:
            logger.error(f"Invalid input for price/quantity: {e}. Please try again.")
        except CreateOrderFailedError as e:
            logger.error(f"Failed to submit Spread order: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Spread order creation: {e}", exc_info=True)
        print("-" * 20)

    def _prompt_and_submit_strip_order(self):
        logger.info("--- New Strip Order Submission ---")
        instrument_name = input("\nEnter Instrument Name (e.g., 'Naphtha MOPJ'): ")
        front_expiry = input(f"Enter Front Expiry for {instrument_name} (e.g., 'Oct-25'): ")
        back_expiry = input(f"Enter Back Expiry for {instrument_name} (e.g., 'Oct-25'): ")

        side, quantity_str, per_price_unit_str, primary_broker_code, secondary_broker_codes, clearing_options = \
            self._get_common_order_details(instrument_name)

        try:
            price_dto, parties_dto = self._create_price_parties_dtos(
                quantity_str, per_price_unit_str, clearing_options, primary_broker_code, secondary_broker_codes
            )
            idempotency_key = str(uuid.uuid4())

            new_order_request = sphere_sdk_types_pb2.TraderStripOrderRequestDto(
                idempotency_key=idempotency_key,
                side=side,
                front_expiry=front_expiry,
                back_expiry=back_expiry,
                instrument_name=instrument_name,
                price=price_dto,
                parties=parties_dto
            )
            
            logger.info(f"Prepared Strip Order: {new_order_request}")
            self._submit_order(new_order_request)

        except (InvalidOperation, ValueError) as e:
            logger.error(f"Invalid input for price/quantity: {e}. Please try again.")
        except CreateOrderFailedError as e:
            logger.error(f"Failed to submit Strip order: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Strip order creation: {e}", exc_info=True)
        print("-" * 20)


    def run_interactive_order_creator(self):
        """
        Presents options to the user for creating different order types.
        """
        while True:
            print("\n--- Select Order Type ---")
            print("1. Flat Order")
            print("2. Fly Order")
            print("3. Spread Order")
            print("4. Strip Order")
            print("Type 'exit' to quit.")

            choice = input("Enter your choice (1-4 or 'exit'): ").lower()

            if choice == '1':
                self._prompt_and_submit_flat_order()
            elif choice == '2':
                self._prompt_and_submit_fly_order()
            elif choice == '3':
                self._prompt_and_submit_spread_order()
            elif choice == '4':
                self._prompt_and_submit_strip_order()
            elif choice == 'exit':
                logger.info("Exiting order creation tool.")
                break
            else:
                print("Invalid choice. Please enter 1, 2, 3, 4, or 'exit'.")

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
        order_tool.run_interactive_order_creator()

    except KeyboardInterrupt:
        logger.info("\nCtrl+C detected. Shutting down gracefully...")
    except (SDKInitializationError, LoginFailedError, TradingClientError) as e:
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