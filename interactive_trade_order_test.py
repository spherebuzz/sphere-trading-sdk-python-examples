import sys
import os
import logging
import getpass
import time
import threading
import uuid
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass, field

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
        NotLoggedInError,
        TradingClientError,
        TradeOrderFailedError
    )
    from sphere_sdk import sphere_sdk_types_pb2
except ImportError as e:
    print(f"Error importing SDK modules: {e}")
    print(f"Please ensure 'sphere_sdk' is in PYTHONPATH or the structure is correct.")
    print(f"Attempted to add '{src_dir}' to sys.path.")
    sys.exit(1)

logger = logging.getLogger("ghost_trader")
logging.basicConfig(
    level=logging.debug,
    format='[%(levelname)s] (%(name)s) %(asctime)s: %(message)s'
)

@dataclass
class GhostOrder:
    """Represents a single synthetic order in our internal order book."""
    instrument_name: str
    expiry: str
    side: sphere_sdk_types_pb2.OrderSide
    price: Decimal
    original_quantity: Decimal
    remaining_quantity: Decimal = field(init=False)

    def __post_init__(self):
        """
        Initializes remaining quantity and normalizes key fields for
        case-insensitive matching.
        """        
        self.instrument_name = self.instrument_name.upper()
        self.expiry = self.expiry.upper()
        self.remaining_quantity = self.original_quantity

    def __str__(self):
        side_str = sphere_sdk_types_pb2.OrderSide.Name(self.side).replace('ORDER_SIDE_', '')
        return (f"[{side_str}] {self.instrument_name} {self.expiry} | "
                f"Price: {self.price} | "
                f"Qty: {self.remaining_quantity}/{self.original_quantity}")


class GhostTrader:
    """
    Manages a synthetic order book and executes trades against real orders
    that match its criteria.
    """
    def __init__(self, sdk_client: SphereTradingClientSDK):
        """
        Initializes the GhostTrader.

        Args:
            sdk_client: An initialized and logged-in instance of SphereTradingClientSDK.
        """
        self.sdk = sdk_client
        self.ghost_order_book = defaultdict(lambda: {'bids': [], 'asks': []})
        self.processed_order_versions = set()
        self.lock = threading.Lock()

    def prompt_for_ghost_orders(self):
        """Interactively prompts the user to create ghost orders."""
        logger.info("--- Ghost Order Setup ---")
        logger.info("Enter your ghost orders. Type 'done' when finished.")
        logger.info("(Instrument and Expiry matching is case-insensitive)")
        while True:
            instrument_name = input("Enter Instrument Name (e.g., 'Naphtha MOPJ') or 'done': ")
            if instrument_name.lower() == 'done':
                break

            expiry = input(f"Enter Expiry for {instrument_name} (e.g., 'Oct-25'): ")

            side_str = ""
            while side_str not in ['buy', 'sell']:
                side_str = input("Enter Side ('buy' or 'sell'): ").lower()

            side = (sphere_sdk_types_pb2.ORDER_SIDE_BID if side_str == 'buy'
                    else sphere_sdk_types_pb2.ORDER_SIDE_ASK)

            price_str = input("Enter Price: ")
            quantity_str = input("Enter Quantity: ")

            try:
                price = Decimal(price_str)
                quantity = Decimal(quantity_str)
                if quantity <= 0:
                    raise ValueError("Quantity must be positive.")

                new_order = GhostOrder(
                    instrument_name=instrument_name,
                    expiry=expiry,
                    side=side,
                    price=price,
                    original_quantity=quantity
                )
                self._add_ghost_order(new_order)
                logger.info(f"Added Ghost Order: {new_order}")

            except (InvalidOperation, ValueError) as e:
                logger.error(f"Invalid input for price/quantity: {e}. Please try again.")

            print("-" * 20)

        self._print_order_book_summary()

    def _add_ghost_order(self, order: GhostOrder):
        """Adds a new ghost order to the internal book and keeps it sorted."""
        key = (order.instrument_name, order.expiry)
        if order.side == sphere_sdk_types_pb2.ORDER_SIDE_BID:
            bids = self.ghost_order_book[key]['bids']
            bids.append(order)
            bids.sort(key=lambda o: o.price, reverse=True)
        else: # ORDER_SIDE_ASK
            asks = self.ghost_order_book[key]['asks']
            asks.append(order)
            asks.sort(key=lambda o: o.price)

    def _print_order_book_summary(self):
        """Prints a summary of the configured ghost orders."""
        logger.info("--- Configured Ghost Order Book ---")
        if not self.ghost_order_book:
            logger.info("No ghost orders have been configured.")
            return

        for (instrument, expiry), sides in sorted(self.ghost_order_book.items()):
            logger.info(f"Market: {instrument} {expiry}")
            if sides['asks']:
                for order in sides['asks']:
                    logger.info(f"  - {order}")
            if sides['bids']:
                for order in sides['bids']:
                    logger.info(f"  - {order}")
        logger.info("-----------------------------------")


    def on_order_event(self, order_data: sphere_sdk_types_pb2.OrderStacksDto):
        """
        Callback for handling incoming real order events.
        Processes orders within a stack in ascending order of their stack_position.
        """
        with self.lock:
            if not order_data.body:
                return

            for stack in order_data.body:
                contract = stack.contract
            
                sorted_orders = sorted(stack.orders, key=lambda o: o.stack_position)

                for real_order in sorted_orders:
                    order_version_key = (real_order.id, real_order.updated_time)
                    log_prefix = f"[Real Order {real_order.id} @ {real_order.updated_time}]"

                    if order_version_key in self.processed_order_versions:
                        logger.debug(f"{log_prefix} Skipping, already processed this version.")
                        continue

                    self.processed_order_versions.add(order_version_key)

                    is_tradable = (real_order.tradability == sphere_sdk_types_pb2.TRADABILITY_TRADABLE)
                    if not is_tradable:
                        tradability_str = sphere_sdk_types_pb2.Tradability.Name(real_order.tradability)
                        logger.info(f"{log_prefix} Skipping, not tradable (Status: {tradability_str}).")
                        continue

                    logger.debug(f"{log_prefix} New tradable order (Pos: {real_order.stack_position}). Evaluating for a match...")
                    self.match_and_trade(real_order, contract)

    def match_and_trade(self, real_order: sphere_sdk_types_pb2.OrderDto, contract: sphere_sdk_types_pb2.ContractDto):
        """Finds a matching ghost order and executes a trade if conditions are met."""
        # --- 1. Set up context for logging ---
        log_prefix = f"[Real Order {real_order.id}]"
        key = (contract.instrument_name.upper(), contract.expiry.upper())
        real_order_side = contract.side
        real_order_side_str = sphere_sdk_types_pb2.OrderSide.Name(real_order_side).replace('ORDER_SIDE_', '')
        real_order_price = Decimal(real_order.price.per_price_unit)
        real_order_qty = Decimal(real_order.price.quantity)
        stack_position = real_order.stack_position
        updated_time = real_order.updated_time

        logger.debug(
            f"{log_prefix} Attempting to match: {real_order_side_str} {real_order_qty} @ {real_order_price} "
            f"for market '{key[0]} {key[1]}'"
        )

        # --- 2. Check if we have any ghost orders for this specific market ---
        if key not in self.ghost_order_book:
            logger.debug(f"{log_prefix} No match: No ghost orders configured for market '{key[0]} {key[1]}'.")
            return

        # --- 3. Determine which side of our book to check and if it has orders ---
        ghost_orders_to_check = []
        our_side_str = ""
        if real_order_side == sphere_sdk_types_pb2.ORDER_SIDE_ASK:
            ghost_orders_to_check = self.ghost_order_book[key]['bids']
            our_side_str = "BIDs"
        elif real_order_side == sphere_sdk_types_pb2.ORDER_SIDE_BID:
            ghost_orders_to_check = self.ghost_order_book[key]['asks']
            our_side_str = "ASKs"

        if not ghost_orders_to_check:
            logger.debug(
                f"{log_prefix} No match: Real order is a {real_order_side_str}, but we have no Ghost {our_side_str} "
                f"for market '{key[0]} {key[1]}'."
            )
            return

        # --- 4. Iterate through our sorted list of ghost orders to find a price match ---
        match_found = False
        for ghost_order in list(ghost_orders_to_check):
            logger.debug(f"{log_prefix} Checking against our Ghost Order: {ghost_order}")

            if ghost_order.remaining_quantity <= 0:
                logger.debug(f"{log_prefix} Skipping fully filled ghost order: {ghost_order}")
                continue

            is_price_match = (
                (ghost_order.side == sphere_sdk_types_pb2.ORDER_SIDE_BID and ghost_order.price >= real_order_price) or
                (ghost_order.side == sphere_sdk_types_pb2.ORDER_SIDE_ASK and ghost_order.price <= real_order_price)
            )

            if is_price_match:
                logger.info(f"{log_prefix} MATCH FOUND with Ghost Order.")
                logger.info(f"  - Real Order:  {real_order_side_str} {real_order_qty} @ {real_order_price} - Pos: {stack_position} Time: {updated_time}")
                logger.info(f"  - Ghost Order: {ghost_order}")

                trade_quantity = min(ghost_order.remaining_quantity, real_order_qty)

                if self.execute_trade(real_order, trade_quantity, ghost_order.side):
                    ghost_order.remaining_quantity -= trade_quantity
                    logger.info(f"{log_prefix} [FILLED] Ghost order updated. New remaining qty: {ghost_order.remaining_quantity}")

                    if ghost_order.remaining_quantity <= 0:
                        logger.info(f"{log_prefix} Ghost order fully filled and removed: {ghost_order}")
                        ghost_orders_to_check.remove(ghost_order)

                match_found = True
                break
            else:
                if ghost_order.side == sphere_sdk_types_pb2.ORDER_SIDE_BID:
                    reason = f"Ghost BID price ({ghost_order.price}) is lower than Real ASK price ({real_order_price}). Cannot lift."
                else:
                    reason = f"Ghost ASK price ({ghost_order.price}) is higher than Real BID price ({real_order_price}). Cannot hit."
                logger.debug(f"{log_prefix} No price match. Reason: {reason}")                  
                break

        if not match_found:
            logger.info(
                f"{log_prefix} No suitable ghost order found for Real "
                f"{real_order_side_str} @ {real_order_price} after checking "
                f"candidate(s)."
            )


    def execute_trade(self, real_order: sphere_sdk_types_pb2.OrderDto, quantity: Decimal, our_side: sphere_sdk_types_pb2.OrderSide) -> bool:
        """
        Creates and sends a trade request for a given real order, using appropriate
        trading terminology in logs.
        """
        if our_side == sphere_sdk_types_pb2.ORDER_SIDE_BID: # We are buying from a real ASK
            action_pp = "Lifting"  # Present participle
            action_verb = "lift"
            target_str = "offer"
        else: # We are selling to a real BID
            action_pp = "Hitting"
            action_verb = "hit"
            target_str = "bid"

        logger.info(f"--- {action_pp} the {target_str}: Trading {quantity} against real order ID: {real_order.id} ---")
        try:
            trade_request = sphere_sdk_types_pb2.TradeOrderRequestDto(
                id=real_order.id,
                quantity=str(quantity),
                idempotency_key=str(uuid.uuid4())
            )

            self.sdk.trade_order(trade_request)

            logger.info(f"[SUCCESS] '{action_verb.capitalize()}' request for order ID {real_order.id} submitted successfully.")
            return True

        except TradeOrderFailedError as e:
            logger.error(f"[FAILURE] Failed to {action_verb} the {target_str} on order ID {real_order.id}. Reason: {e}")
            return False
        except Exception as e:
            logger.error(f"[UNEXPECTED] An error occurred while {action_pp.lower()}ing the {target_str} on order ID {real_order.id}: {e}", exc_info=True)
            return False


def main():
    """
    Main function to initialize the SDK, log in, and run the ghost trading bot.
    """
    logger.info("Starting Sphere Ghost Trader Script...")
    sdk_instance = None
    try:
        # 1. Initialize the SDK
        sdk_instance = SphereTradingClientSDK()
        logger.info("SDK initialized.")

        # 2. Get credentials and log in
        username = input("Enter username: ")
        password = getpass.getpass("Enter password: ")
        sdk_instance.login(username, password)
        logger.info(f"Login successful for user '{username}'.")

        # 3. Set up the ghost trader and get user orders
        ghost_trader = GhostTrader(sdk_instance)
        ghost_trader.prompt_for_ghost_orders()

        # 4. Subscribe to order events
        logger.info("Subscribing to order events...")
        sdk_instance.subscribe_to_order_events(ghost_trader.on_order_event)
        logger.info("Successfully subscribed. Listening for matching orders...")
        logger.info("Press Ctrl+C to stop the bot and logout.")

        # 5. Keep the script running to receive events
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("\nCtrl+C detected. Shutting down gracefully...")
    except (SDKInitializationError, LoginFailedError, NotLoggedInError, TradingClientError) as e:
        logger.error(f"A critical SDK error occurred: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        # 6. Clean up resources on exit
        if sdk_instance and sdk_instance._is_logged_in:
            logger.info("Unsubscribing from event streams...")
            try:
                if sdk_instance._user_order_callback:
                    sdk_instance.unsubscribe_from_order_events()
            except TradingClientError as e:
                logger.warning(f"Could not cleanly unsubscribe from events: {e}")

            logger.info("Logging out...")
            sdk_instance.logout()
            logger.info("Logout complete.")

        logger.info("Sphere Ghost Trader Script has finished.")


if __name__ == "__main__":
    main()