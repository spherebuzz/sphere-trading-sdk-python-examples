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
        CancelOrderFailedError
    )
    from sphere_sdk import sphere_sdk_types_pb2
except ImportError as e:
    print(f"Error importing SDK modules: {e}")
    print(f"Please ensure 'sphere_sdk' is in PYTHONPATH or the structure is correct.")
    print(f"Attempted to add '{src_dir}' to sys.path.")
    sys.exit(1)

logger = logging.getLogger("cancel_order_creator")
logging.basicConfig(
    level=logging.debug,
    format='[%(levelname)s] (%(name)s) %(asctime)s: %(message)s'
)

class CancelOrderSubmissionTool:
    """
    Manages an interactive prompt to gather and submit order cancellation requests to Sphere.
    """
    def __init__(self, sdk_client: SphereTradingClientSDK):
        """
        Initializes the CancelOrderSubmissionTool.

        Args:
            sdk_client: An initialized and logged-in instance of SphereTradingClientSDK.
        """
        self.sdk = sdk_client

    def prompt_and_submit_cancel_orders(self):
        """Interactively prompts the user to cancel orders."""
        logger.info("--- New Cancel Order Submission ---")
        logger.info("Enter details for your cancel order requests. Type 'done' when finished.")
        while True:
            order_instance_id = input("\nEnter Order Instance Id or 'done': ")
            if order_instance_id.lower() == 'done':
                break

            try:
                
                idempotency_key = str(uuid.uuid4())

                sdk_cancel_order_request = sphere_sdk_types_pb2.CancelOrderRequestDto(
                    idempotency_key=idempotency_key,
                    instance_id=order_instance_id
                )
                
                logger.info(f"Prepared cancel order: {sdk_cancel_order_request}")
                self._submit_cancel_order(sdk_cancel_order_request)

            except CancelOrderFailedError as e:
                logger.error(f"Failed to submit cancel order: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred during cancel order: {e}", exc_info=True)

            print("-" * 20)

        logger.info("Finished submitting cancel order request.")

    def _submit_cancel_order(self, sdk_cancel_order_request: sphere_sdk_types_pb2.CancelOrderRequestDto):
        """
            Submits a single order cancellation request to the Sphere API.
        """

        logger.info(f"Submitting order with idempotency_key: {sdk_cancel_order_request.idempotency_key}")
        
        try:
            cancelOrderResponse = self.sdk.cancel_order(sdk_cancel_order_request)
            logger.info(f"Successfully submitted cancel order. Order Response: {cancelOrderResponse}")
        except CancelOrderFailedError as e:
            logger.error(f"Failed to submit cancel order for {sdk_cancel_order_request.order_instance_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while submitting cancel order: {e}", exc_info=True)
            raise

def main():
    """
    Main function to initialize the SDK, log in, and run the cancel order submission tool.
    """
    logger.info("Starting Sphere Interactive Cancel Order Creator...")
    sdk_instance = None
    try:
        sdk_instance = SphereTradingClientSDK()
        logger.info("SDK initialized.")

        username = input("Enter username: ")
        password = getpass.getpass("Enter password: ")
        sdk_instance.login(username, password)
        logger.info(f"Login successful for user '{username}'.")

        order_tool = CancelOrderSubmissionTool(sdk_instance)
        order_tool.prompt_and_submit_cancel_orders()

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

        logger.info("Sphere Interactive Cancel Order Creator has finished.")


if __name__ == "__main__":
    main()