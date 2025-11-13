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
        UpdateOrderFailedError
    )
    from sphere_sdk import sphere_sdk_types_pb2
except ImportError as e:
    print(f"Error importing SDK modules: {e}")
    print(f"Please ensure 'sphere_sdk' is in PYTHONPATH or the structure is correct.")
    print(f"Attempted to add '{src_dir}' to sys.path.")
    sys.exit(1)

logger = logging.getLogger("order_updater")
logging.basicConfig(
    level=logging.INFO, # Change to INFO for less verbose output during normal operation
    format='[%(levelname)s] (%(name)s) %(asctime)s: %(message)s'
)

# Define a type alias for all possible order update request DTOs
UpdateOrderRequestDto = Union[
    sphere_sdk_types_pb2.TraderUpdateFlatOrderRequestDto,
    sphere_sdk_types_pb2.TraderUpdateFlyOrderRequestDto,
    sphere_sdk_types_pb2.TraderUpdateSpreadOrderRequestDto,
    sphere_sdk_types_pb2.TraderUpdateStripOrderRequestDto
]

class OrderUpdateSubmissionTool:
    """
    Manages interactive prompting for order update details and submitting them to Sphere.
    """
    def __init__(self, sdk_client: SphereTradingClientSDK):
        """
        Initializes the OrderUpdateSubmissionTool.

        Args:
            sdk_client: An initialized and logged-in instance of SphereTradingClientSDK.
        """
        self.sdk = sdk_client

    def _get_common_update_details(self, instance_id: str):
        """Helper to get common order update details (quantity, price, brokers, clearing)."""
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
        
        return quantity_str, per_price_unit_str, primary_broker_code, secondary_broker_codes, clearing_options

    def _create_price_parties_dtos(self, quantity_str: str, per_price_unit_str: str, clearing_options: List[str], primary_broker_code: str, secondary_broker_codes: List[str]):
        """Helper to create PriceDto and PartiesDto for updates."""
        
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

    def _submit_order_update(self, sdk_update_request: UpdateOrderRequestDto):
        """
        Submit new order update request, dynamically calling the correct SDK method.
        """
        logger.info(f"Submitting order update with idempotency_key: {sdk_update_request.idempotency_key} for instance ID: {sdk_update_request.instance_id}")
        
        try:
            orderResponse = None
            if isinstance(sdk_update_request, sphere_sdk_types_pb2.TraderUpdateFlatOrderRequestDto):
                orderResponse = self.sdk.update_trader_flat_order(sdk_update_request)
                order_type_desc = "Flat Order"
            elif isinstance(sdk_update_request, sphere_sdk_types_pb2.TraderUpdateFlyOrderRequestDto):
                orderResponse = self.sdk.update_trader_fly_order(sdk_update_request)
                order_type_desc = "Fly Order"
            elif isinstance(sdk_update_request, sphere_sdk_types_pb2.TraderUpdateSpreadOrderRequestDto):
                orderResponse = self.sdk.update_trader_spread_order(sdk_update_request)
                order_type_desc = "Spread Order"
            elif isinstance(sdk_update_request, sphere_sdk_types_pb2.TraderUpdateStripOrderRequestDto):
                orderResponse = self.sdk.update_trader_strip_order(sdk_update_request)
                order_type_desc = "Strip Order"
            else:
                raise ValueError(f"Unknown order update request DTO type: {type(sdk_update_request)}")

            logger.info(f"Successfully submitted {order_type_desc} update. Order ID: {orderResponse.id}, Instance ID: {orderResponse.instance_id}")
        except UpdateOrderFailedError as e:
            logger.error(f"Failed to update {order_type_desc} with Instance ID: {sdk_update_request.instance_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while submitting {order_type_desc} update: {e}", exc_info=True)
            raise

    def _prompt_and_submit_flat_order_update(self):
        logger.info("--- Flat Order Update Submission ---")
        instance_id = input("\nEnter Flat Order Instance Id: ")
        if not instance_id:
            logger.warning("Order Instance ID cannot be empty. Skipping flat order update.")
            return

        try:
            quantity_str, per_price_unit_str, primary_broker_code, secondary_broker_codes, clearing_options = \
                self._get_common_update_details(instance_id)
            
            price_dto, parties_dto = self._create_price_parties_dtos(
                quantity_str, per_price_unit_str, clearing_options, primary_broker_code, secondary_broker_codes
            )
            idempotency_key = str(uuid.uuid4())

            new_update_request = sphere_sdk_types_pb2.TraderUpdateFlatOrderRequestDto(
                idempotency_key=idempotency_key,
                instance_id=instance_id,
                price=price_dto,
                parties=parties_dto
            )
            
            logger.info(f"Prepared Flat Order Update: {new_update_request}")
            self._submit_order_update(new_update_request)

        except (InvalidOperation, ValueError) as e:
            logger.error(f"Invalid input for price/quantity: {e}. Please try again.")
        except UpdateOrderFailedError as e:
            logger.error(f"Failed to submit Flat order update: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Flat order update: {e}", exc_info=True)
        print("-" * 20)

    def _prompt_and_submit_fly_order_update(self):
        logger.info("--- Fly Order Update Submission ---")
        instance_id = input("\nEnter Fly Order Instance Id: ")
        if not instance_id:
            logger.warning("Order Instance ID cannot be empty. Skipping fly order update.")
            return

        try:
            quantity_str, per_price_unit_str, primary_broker_code, secondary_broker_codes, clearing_options = \
                self._get_common_update_details(instance_id)
            
            price_dto, parties_dto = self._create_price_parties_dtos(
                quantity_str, per_price_unit_str, clearing_options, primary_broker_code, secondary_broker_codes
            )
            idempotency_key = str(uuid.uuid4())

            new_update_request = sphere_sdk_types_pb2.TraderUpdateFlyOrderRequestDto(
                idempotency_key=idempotency_key,
                instance_id=instance_id,
                price=price_dto,
                parties=parties_dto
            )
            
            logger.info(f"Prepared Fly Order Update: {new_update_request}")
            self._submit_order_update(new_update_request)

        except (InvalidOperation, ValueError) as e:
            logger.error(f"Invalid input for price/quantity: {e}. Please try again.")
        except UpdateOrderFailedError as e:
            logger.error(f"Failed to submit Fly order update: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Fly order update: {e}", exc_info=True)
        print("-" * 20)

    def _prompt_and_submit_spread_order_update(self):
        logger.info("--- Spread Order Update Submission ---")
        instance_id = input("\nEnter Spread Order Instance Id: ")
        if not instance_id:
            logger.warning("Order Instance ID cannot be empty. Skipping spread order update.")
            return

        try:
            quantity_str, per_price_unit_str, primary_broker_code, secondary_broker_codes, clearing_options = \
                self._get_common_update_details(instance_id)
            
            price_dto, parties_dto = self._create_price_parties_dtos(
                quantity_str, per_price_unit_str, clearing_options, primary_broker_code, secondary_broker_codes
            )
            idempotency_key = str(uuid.uuid4())

            new_update_request = sphere_sdk_types_pb2.TraderUpdateSpreadOrderRequestDto(
                idempotency_key=idempotency_key,
                instance_id=instance_id,
                price=price_dto,
                parties=parties_dto
            )
            
            logger.info(f"Prepared Spread Order Update: {new_update_request}")
            self._submit_order_update(new_update_request)

        except (InvalidOperation, ValueError) as e:
            logger.error(f"Invalid input for price/quantity: {e}. Please try again.")
        except UpdateOrderFailedError as e:
            logger.error(f"Failed to submit Spread order update: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Spread order update: {e}", exc_info=True)
        print("-" * 20)

    def _prompt_and_submit_strip_order_update(self):
        logger.info("--- Strip Order Update Submission ---")
        instance_id = input("\nEnter Strip Order Instance Id: ")
        if not instance_id:
            logger.warning("Order Instance ID cannot be empty. Skipping strip order update.")
            return

        try:
            quantity_str, per_price_unit_str, primary_broker_code, secondary_broker_codes, clearing_options = \
                self._get_common_update_details(instance_id)
            
            price_dto, parties_dto = self._create_price_parties_dtos(
                quantity_str, per_price_unit_str, clearing_options, primary_broker_code, secondary_broker_codes
            )
            idempotency_key = str(uuid.uuid4())

            new_update_request = sphere_sdk_types_pb2.TraderUpdateStripOrderRequestDto(
                idempotency_key=idempotency_key,
                instance_id=instance_id,
                price=price_dto,
                parties=parties_dto
            )
            
            logger.info(f"Prepared Strip Order Update: {new_update_request}")
            self._submit_order_update(new_update_request)

        except (InvalidOperation, ValueError) as e:
            logger.error(f"Invalid input for price/quantity: {e}. Please try again.")
        except UpdateOrderFailedError as e:
            logger.error(f"Failed to submit Strip order update: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during Strip order update: {e}", exc_info=True)
        print("-" * 20)

    def run_interactive_order_updater(self):
        """
        Presents options to the user for updating different order types.
        """
        while True:
            print("\n--- Select Order Type to Update ---")
            print("1. Flat Order")
            print("2. Fly Order")
            print("3. Spread Order")
            print("4. Strip Order")
            print("Type 'exit' to quit.")

            choice = input("Enter your choice (1-4 or 'exit'): ").lower()

            if choice == '1':
                self._prompt_and_submit_flat_order_update()
            elif choice == '2':
                self._prompt_and_submit_fly_order_update()
            elif choice == '3':
                self._prompt_and_submit_spread_order_update()
            elif choice == '4':
                self._prompt_and_submit_strip_order_update()
            elif choice == 'exit':
                logger.info("Exiting order update tool.")
                break
            else:
                print("Invalid choice. Please enter 1, 2, 3, 4, or 'exit'.")

def main():
    """
    Main function to initialize the SDK, log in, and run the order update submission tool.
    """
    logger.info("Starting Sphere Interactive Order Updater...")
    sdk_instance = None
    try:
        sdk_instance = SphereTradingClientSDK()
        logger.info("SDK initialized.")

        username = input("Enter username: ")
        password = getpass.getpass("Enter password: ")
        sdk_instance.login(username, password)
        logger.info(f"Login successful for user '{username}'.")

        order_tool = OrderUpdateSubmissionTool(sdk_instance)
        order_tool.run_interactive_order_updater()

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

        logger.info("Sphere Interactive Order Updater has finished.")


if __name__ == "__main__":
    main()