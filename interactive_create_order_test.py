import sys
import os
import logging
import getpass
import time
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

@dataclass
class Broker:
    """Represents a broker party."""
    code: str

@dataclass
class NewOrderRequest:
    """Represents a single order request to be submitted."""
    instrument_name: str
    expiry: str
    side: sphere_sdk_types_pb2.OrderSide
    per_price_unit: Decimal
    quantity: Decimal
    primary_broker: Broker
    secondary_brokers: List[Broker] = field(default_factory=list)
    clearing_options: List[str] = field(default_factory=list)

    def __post_init__(self):
        """
        Normalizes key fields for case-insensitive matching.
        """        
        self.instrument_name = self.instrument_name.upper()
        self.expiry = self.expiry.upper()

    def __str__(self):
        side_str = sphere_sdk_types_pb2.OrderSide.Name(self.side).replace('ORDER_SIDE_', '')
        clearing_info = f"Clearing: {', '.join(self.clearing_options)}" if self.clearing_options else "No Clearing"
        return (f"[{side_str}] {self.instrument_name} {self.expiry} | "
                f"Price: {self.per_price_unit} | "
                f"Qty: {self.quantity} | "
                f"Primary Broker: {self.primary_broker.code} | "
                f"{clearing_info}")


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

    def _prompt_for_broker(self, broker_type: str) -> Broker:
        """Helper to prompt for broker details."""
        print(f"--- {broker_type} Broker Details ---")
        code = input(f"Enter {broker_type} Broker Code: ")
        return Broker(code=code)

    def prompt_and_submit_orders(self):
        """Interactively prompts the user to create and submit new orders."""
        logger.info("--- New Order Submission ---")
        logger.info("Enter details for your orders. Type 'done' when finished.")
        while True:
            instrument_name = input("\nEnter Instrument Name (e.g., 'Naphtha MOPJ') or 'done': ")
            if instrument_name.lower() == 'done':
                break

            expiry = input(f"Enter Expiry for {instrument_name} (e.g., 'Oct-25'): ")

            side_str = ""
            while side_str not in ['buy', 'sell']:
                side_str = input("Enter Side ('buy' or 'sell'): ").lower()

            side = (sphere_sdk_types_pb2.ORDER_SIDE_BID if side_str == 'buy'
                    else sphere_sdk_types_pb2.ORDER_SIDE_ASK)

            quantity_str = input("Enter Quantity: ")
            per_price_unit_str = input("Enter Price (e.g., '100'): ")

            primary_broker = self._prompt_for_broker("Primary")

            secondary_brokers = []
            while True:
                add_secondary = input("Add a secondary broker? (yes/no): ").lower()
                if add_secondary == 'yes':
                    secondary_brokers.append(self._prompt_for_broker("Secondary"))
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
                if quantity <= 0:
                    raise ValueError("Quantity must be positive.")

                new_order_request = NewOrderRequest(
                    instrument_name=instrument_name,
                    expiry=expiry,
                    side=side,
                    quantity=quantity,
                    per_price_unit=per_price_unit,
                    primary_broker=primary_broker,
                    secondary_brokers=secondary_brokers,
                    clearing_options=clearing_options
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

    def _create_sdk_order_request(self, order_req: NewOrderRequest) -> sphere_sdk_types_pb2.TraderFlatOrderRequestDto:
        """
        Converts a NewOrderRequest into an SDK TraderFlatOrderRequestDto.
        """
        idempotency_key = str(uuid.uuid4())

        price_dto = sphere_sdk_types_pb2.OrderRequestPriceDto(
            per_price_unit=str(order_req.per_price_unit),
            quantity=str(order_req.quantity),
            ordered_clearing_options=[
                sphere_sdk_types_pb2.OrderRequestClearingOptionDto(code=code)
                for code in order_req.clearing_options
            ]
        )

        primary_broker_dto = sphere_sdk_types_pb2.OrderRequestBrokerDto(
            code=order_req.primary_broker.code
        )

        secondary_brokers_dtos = [
            sphere_sdk_types_pb2.OrderRequestBrokerDto(code=b.code)
            for b in order_req.secondary_brokers
        ]

        parties_dto = sphere_sdk_types_pb2.TraderOrderRequestPartiesDto(
            primary_broker=primary_broker_dto,
            secondary_brokers=secondary_brokers_dtos
        )

        sdk_order_request = sphere_sdk_types_pb2.TraderFlatOrderRequestDto(
            idempotency_key=idempotency_key,
            side=order_req.side,
            expiry=order_req.expiry,
            instrument_name=order_req.instrument_name,
            price=price_dto,
            parties=parties_dto
        )
        return sdk_order_request

    def _submit_order(self, order_req: NewOrderRequest):
        """
        Converts the NewOrderRequest to an SDK DTO and submits it.
        """
        sdk_order_request = self._create_sdk_order_request(order_req)
        logger.info(f"Submitting order with idempotency_key: {sdk_order_request.idempotency_key}")
        
        try:
            orderResponse = self.sdk.create_trader_flat_order(sdk_order_request)
            logger.info(f"Successfully submitted order. Order Response: {orderResponse}")
        except CreateOrderFailedError as e:
            logger.error(f"Failed to submit order for {order_req.instrument_name} {order_req.expiry} "
                         f"({sphere_sdk_types_pb2.OrderSide.Name(order_req.side)} @ {order_req.per_price_unit}): {e}")
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