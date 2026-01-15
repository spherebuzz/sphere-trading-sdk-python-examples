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
from abc import ABC, abstractmethod
import json
from google.protobuf.message import Message

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
logger.setLevel(logging.DEBUG) 

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('[%(levelname)s] (%(name)s) %(asctime)s: %(message)s')
ch.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(ch)

class InternalOrderType:
    FLAT = "FLAT"
    SPREAD = "SPREAD"
    FLY = "FLY"
    STRIP = "STRIP"

@dataclass
class BaseGhostOrder(ABC):
    """Base class for all synthetic orders."""
    order_type: InternalOrderType
    instrument_name: str
    side: sphere_sdk_types_pb2.OrderSide
    price: Decimal
    original_quantity: Decimal
    remaining_quantity: Decimal = field(init=False)
    
    ghost_id: str = field(default_factory=lambda: str(uuid.uuid4()), init=False)


    def __post_init__(self):
        self.instrument_name = self.instrument_name.upper()
        self.remaining_quantity = self.original_quantity

    @abstractmethod
    def get_market_key(self) -> tuple:
        """Returns a tuple that uniquely identifies the market for this order."""
        pass

    @abstractmethod
    def __str__(self):
        """String representation of the order."""
        pass

    def __repr__(self):
        return self.__str__() + f" (ID: {self.ghost_id[:8]})"


@dataclass
class FlatGhostOrder(BaseGhostOrder):
    expiry: str

    def __post_init__(self):
        super().__post_init__()
        self.expiry = self.expiry.upper()

    def get_market_key(self) -> tuple:
        return (self.order_type, self.instrument_name, self.expiry)

    def __str__(self):
        side_str = sphere_sdk_types_pb2.OrderSide.Name(self.side).replace('ORDER_SIDE_', '')
        return (f"[FLAT {side_str}] {self.instrument_name} {self.expiry} | "
                f"Price: {self.price} | Qty: {self.remaining_quantity}/{self.original_quantity}")


@dataclass
class SpreadGhostOrder(BaseGhostOrder):
    
    sell_leg_expiry: str
    buy_leg_expiry: str

    def __post_init__(self):
        super().__post_init__()
        self.sell_leg_expiry = self.sell_leg_expiry.upper()
        self.buy_leg_expiry = self.buy_leg_expiry.upper()

    def get_market_key(self) -> tuple:
        return (self.order_type, self.instrument_name, self.sell_leg_expiry, self.buy_leg_expiry)

    def __str__(self):
        side_str = sphere_sdk_types_pb2.OrderSide.Name(self.side).replace('ORDER_SIDE_', '')
        return (f"[SPREAD {side_str}] {self.instrument_name} SELL:{self.sell_leg_expiry}/BUY:{self.buy_leg_expiry} | "
                f"Price: {self.price} | Qty: {self.remaining_quantity}/{self.original_quantity}")


@dataclass
class FlyGhostOrder(BaseGhostOrder):
    first_expiry: str
    second_expiry: str
    third_expiry: str

    def __post_init__(self):
        super().__post_init__()
        self.first_expiry = self.first_expiry.upper()
        self.second_expiry = self.second_expiry.upper()
        self.third_expiry = self.third_expiry.upper()

    def get_market_key(self) -> tuple:
        return (self.order_type, self.instrument_name, self.first_expiry, self.second_expiry, self.third_expiry)

    def __str__(self):
        side_str = sphere_sdk_types_pb2.OrderSide.Name(self.side).replace('ORDER_SIDE_', '')
        return (f"[FLY {side_str}] {self.instrument_name} {self.first_expiry}/{self.second_expiry}/{self.third_expiry} | "
                f"Price: {self.price} | Qty: {self.remaining_quantity}/{self.original_quantity}")


@dataclass
class StripGhostOrder(BaseGhostOrder):
    front_expiry: str
    back_expiry: str = field(default=None)

    def __post_init__(self):
        super().__post_init__()
        self.front_expiry = self.front_expiry.upper()

        if self.back_expiry:
            self.back_expiry = self.back_expiry.upper()
        else:
            # If back_expiry is not provided, assume it's the same as front_expiry
            # This makes "Q1-25" look like "Q1-25-Q1-25" internally for key consistency
            self.back_expiry = self.front_expiry # Default to front_expiry if not given

    def get_market_key(self) -> tuple:
        return (self.order_type, self.instrument_name, self.front_expiry, self.back_expiry)

    def __str__(self):
        side_str = sphere_sdk_types_pb2.OrderSide.Name(self.side).replace('ORDER_SIDE_', '')

        expiry_display = f"{self.front_expiry}-{self.back_expiry}" if self.front_expiry != self.back_expiry else self.front_expiry
        return (f"[STRIP {side_str}] {self.instrument_name} {expiry_display} | "
                f"Price: {self.price} | Qty: {self.remaining_quantity}/{self.original_quantity}")


class GhostTrader:
    """
    Manages a synthetic order book and executes trades against real orders
    that match its criteria across various order types.
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

    def _get_user_input(self, prompt: str, validation_func=None, error_msg: str = "Invalid input. Please try again."):
        """Helper for robust user input."""
        while True:
            value = input(prompt).strip()
            if validation_func is None or validation_func(value):
                return value
            logger.error(error_msg)

    def _is_decimal(self, value: str) -> bool:
        try:
            Decimal(value)
            return True
        except InvalidOperation:
            return False

    def _is_positive_decimal(self, value: str) -> bool:
        try:
            d = Decimal(value)
            return d > 0
        except InvalidOperation:
            return False
            
    def _sdk_dto_to_dict(self, dto_object):
        """Recursively converts an SDK Protobuf DTO to a dictionary."""
        if isinstance(dto_object, Message):
            data = {}
            for field_descriptor, value in dto_object.ListFields():
                field_name = field_descriptor.name
                if field_descriptor.type == field_descriptor.TYPE_ENUM:
                    enum_type = field_descriptor.enum_type
                    value = enum_type.values_by_number[value].name
                elif field_descriptor.label == field_descriptor.LABEL_REPEATED:
                    value = [self._sdk_dto_to_dict(item) if isinstance(item, Message) else str(item) for item in value]
                elif isinstance(value, Message):
                    value = self._sdk_dto_to_dict(value)
                else:
                    value = str(value)
                data[field_name] = value
            return data
        return dto_object

    def prompt_for_ghost_orders(self):
        """Interactively prompts the user to create ghost orders."""
        logger.info("--- Ghost Order Setup ---")
        logger.info("Enter your ghost orders. Type 'done' when finished.")
        logger.info("(Instrument and Expiry matching is case-insensitive)")
        while True:
            order_type_str = self._get_user_input(
                "Enter Ghost Order Type (Flat, Spread, Fly, Strip) or 'done': ",
                lambda x: x.upper() in [InternalOrderType.FLAT, InternalOrderType.SPREAD, InternalOrderType.FLY, InternalOrderType.STRIP, 'DONE'],
                "Invalid order type."
            ).upper()
            if order_type_str == 'DONE':
                break

            try:
                instrument_name = self._get_user_input("Enter Instrument Name (e.g., 'Naphtha MOPJ'): ").upper()
                side_str = self._get_user_input("Enter Side ('buy' or 'sell'): ", lambda x: x in ['buy', 'sell'], "Invalid side.")
                side = (sphere_sdk_types_pb2.ORDER_SIDE_BID if side_str == 'buy' else sphere_sdk_types_pb2.ORDER_SIDE_ASK)
                
                price = Decimal(self._get_user_input("Enter Price: ", lambda x: self._is_decimal(x), "Invalid price."))
                quantity = Decimal(self._get_user_input("Enter Quantity: ", lambda x: self._is_positive_decimal(x), "Quantity must be a positive number."))

                new_order: BaseGhostOrder
                if order_type_str == InternalOrderType.FLAT:
                    expiry = self._get_user_input(f"Enter Expiry for {instrument_name} (e.g., 'Oct-25'): ").upper()
                    new_order = FlatGhostOrder(
                        order_type=InternalOrderType.FLAT,
                        instrument_name=instrument_name,
                        expiry=expiry,
                        side=side,
                        price=price,
                        original_quantity=quantity
                    )
                elif order_type_str == InternalOrderType.SPREAD:
                    logger.debug("DEBUG: For Spread orders, enter expiries in the order: SELL Leg then BUY Leg, based on the SDK's canonical representation (e.g., if SDK's Jan-26 leg is SELL and Dec-25 leg is BUY, enter Jan-26 then Dec-25).")
                       
                    sell_leg_expiry_input = self._get_user_input(f"Enter Expiry for the SELL LEG of the spread (e.g., 'Jan-26'): ").upper()
                    buy_leg_expiry_input = self._get_user_input(f"Enter Expiry for the BUY LEG of the spread (e.g., 'Dec-25'): ").upper()

                    new_order = SpreadGhostOrder(
                        order_type=InternalOrderType.SPREAD,
                        instrument_name=instrument_name,
                        sell_leg_expiry=sell_leg_expiry_input,
                        buy_leg_expiry=buy_leg_expiry_input,
                        side=side,
                        price=price,
                        original_quantity=quantity
                    )
                elif order_type_str == InternalOrderType.FLY:
                    first_expiry = self._get_user_input(f"Enter First Expiry for {instrument_name} fly: ").upper()
                    second_expiry = self._get_user_input(f"Enter Second Expiry for {instrument_name} fly: ").upper()
                    third_expiry = self._get_user_input(f"Enter Third Expiry for {instrument_name} fly: ").upper()
                    new_order = FlyGhostOrder(
                        order_type=InternalOrderType.FLY,
                        instrument_name=instrument_name,
                        first_expiry=first_expiry,
                        second_expiry=second_expiry,
                        third_expiry=third_expiry,
                        side=side,
                        price=price,
                        original_quantity=quantity
                    )
                elif order_type_str == InternalOrderType.STRIP:
                    front_expiry = self._get_user_input(f"Enter Front Expiry for {instrument_name} strip (e.g., 'Jan-26', 'Q1-25'): ").upper()
                    back_expiry_input = self._get_user_input(f"Enter Back Expiry for {instrument_name} strip (e.g., 'Mar-26', or leave blank for single-period strips like 'Q1-25'): ").upper()
                    
                    new_order = StripGhostOrder(
                        order_type=InternalOrderType.STRIP,
                        instrument_name=instrument_name,
                        front_expiry=front_expiry,
                        back_expiry=back_expiry_input if back_expiry_input else None,
                        side=side,
                        price=price,
                        original_quantity=quantity
                    )
                else:
                    raise ValueError("Unknown order type selected.")

                self._add_ghost_order(new_order)
                logger.info(f"Added Ghost Order: {new_order}")
                logger.debug(f"DEBUG: Ghost Order Market Key for {new_order.order_type}: {repr(new_order.get_market_key())}")


            except (InvalidOperation, ValueError) as e:
                logger.error(f"Invalid input: {e}. Please try again.")

            print("-" * 20)

        self._print_order_book_summary()
        self._debug_print_full_order_book()

    def _add_ghost_order(self, order: BaseGhostOrder):
        """Adds a new ghost order to the internal book and keeps it sorted."""
        key = order.get_market_key()
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
        logger.info("--- Configured Ghost Order Book Summary ---")
        if not self.ghost_order_book:
            logger.info("No ghost orders have been configured.")
            return

        for key, sides in sorted(self.ghost_order_book.items()):
            logger.info(f"Market: {key}")
            if sides['asks']:
                logger.info("  ASKS:")
                for order in sides['asks']:
                    logger.info(f"    - {order}")
            if sides['bids']:
                logger.info("  BIDS:")
                for order in sides['bids']:
                    logger.info(f"    - {order}")
        logger.info("------------------------------------------")

    def _debug_print_full_order_book(self):
        """Prints the full ghost order book with internal keys and remaining quantities for debugging."""
        logger.debug("--- DEBUG: Full Ghost Order Book Content (Internal View) ---")
        if not self.ghost_order_book:
            logger.debug("DEBUG: Ghost order book is empty.")
            return

        for key, sides in self.ghost_order_book.items():
            logger.debug(f"DEBUG: Market Key: {repr(key)}")
            logger.debug(f"  Bids: {[(str(o), o.price, o.remaining_quantity, o.ghost_id[:8]) for o in sides['bids']]}")
            logger.debug(f"  Asks: {[(str(o), o.price, o.remaining_quantity, o.ghost_id[:8]) for o in sides['asks']]}")
        logger.debug("----------------------------------------------------------")


    def on_order_event(self, order_data: sphere_sdk_types_pb2.OrderStacksDto):
        """
        Callback for handling incoming real order events.
        Processes orders within a stack in ascending order of their stack_position.
        """
        with self.lock:
            if logger.level <= logging.DEBUG:
                try:
                    order_data_dict = self._sdk_dto_to_dict(order_data)
                    logger.debug(f"DEBUG: Raw incoming OrderStacksDto: {json.dumps(order_data_dict, indent=2)}")
                except Exception as e:
                    logger.warning(f"Failed to log detailed OrderStacksDto: {e}")

            if not order_data.body:
                logger.debug("DEBUG: Received empty order_data.body, skipping.")
                return

            for stack in order_data.body:
                contract = stack.contract
            
                sorted_orders = sorted(stack.orders, key=lambda o: o.stack_position)

                for real_order in sorted_orders:
                    order_version_key = (real_order.id, real_order.updated_time)
                    log_prefix = f"[Real Order {real_order.id[:8]}@{real_order.updated_time}]"

                    if order_version_key in self.processed_order_versions:
                        logger.debug(f"{log_prefix} Skipping, already processed this version.")
                        continue

                    self.processed_order_versions.add(order_version_key)

                    is_tradable = (real_order.tradability == sphere_sdk_types_pb2.TRADABILITY_TRADABLE)
                    if not is_tradable:
                        tradability_str = sphere_sdk_types_pb2.Tradability.Name(real_order.tradability)

                        logger.info(f"{log_prefix} Skipping, not tradable (Status: {tradability_str}). "
                                    f"Contract: {contract.instrument_name} {contract.expiry or ''} "
                                    f"Price: {real_order.price.per_price_unit} Qty: {real_order.price.quantity}")
                        continue

                    logger.debug(f"{log_prefix} New tradable order (Pos: {real_order.stack_position}). Evaluating for a match...")
                    self.match_and_trade(real_order, contract)

    def match_and_trade(self, real_order: sphere_sdk_types_pb2.OrderDto, contract: sphere_sdk_types_pb2.ContractDto):
        """Finds a matching ghost order and executes a trade if conditions are met."""
        log_prefix = f"[Real Order {real_order.id[:8]}]"
        
        # --- 1. Determine the market key for the incoming real order ---
        logger.debug(f"{log_prefix} CONTRACT_DEBUG: Instrument: {repr(contract.instrument_name)}, ExpiryType: {sphere_sdk_types_pb2.ExpiryType.Name(contract.expiry_type)}")
        if contract.expiry_type == sphere_sdk_types_pb2.EXPIRY_TYPE_OUTRIGHT and contract.expiry:
            logger.debug(f"{log_prefix} CONTRACT_DEBUG: Outright Expiry: {repr(contract.expiry)}")
        if contract.legs:
            for i, leg in enumerate(contract.legs):
                logger.debug(f"{log_prefix} CONTRACT_DEBUG: Leg[{i}] Instrument: {repr(leg.instrument_name)}, Expiry: {repr(leg.expiry)}")
        if contract.constituents:
            for i, constituent in enumerate(contract.constituents):
                logger.debug(f"{log_prefix} CONTRACT_DEBUG: Constituent[{i}] Expiry: {repr(constituent.expiry)}")


        real_order_market_key = self._get_market_key_from_contract(contract)

        logger.debug(f"{log_prefix} DEBUG: Generated Real Order Market Key: {repr(real_order_market_key)}")


        if real_order_market_key is None:
            logger.warning(f"{log_prefix} Could not determine market key for contract type: {sphere_sdk_types_pb2.ExpiryType.Name(contract.expiry_type)}. Skipping.")
            return

        real_order_side = contract.side
        real_order_side_str = sphere_sdk_types_pb2.OrderSide.Name(real_order_side).replace('ORDER_SIDE_', '')
        
        try:
            real_order_price = Decimal(str(real_order.price.per_price_unit))
            real_order_qty = Decimal(str(real_order.price.quantity))
        except InvalidOperation as e:
            logger.error(f"{log_prefix} ERROR: Failed to convert real order price/quantity to Decimal: {e}. "
                         f"Raw Price: '{real_order.price.per_price_unit}', Raw Qty: '{real_order.price.quantity}'. Skipping.")
            return

        stack_position = real_order.stack_position
        updated_time = real_order.updated_time

        logger.debug(
            f"{log_prefix} Matching context: Side: {real_order_side_str}, Qty: {real_order_qty}, Price: {real_order_price} "
            f"for market key: {repr(real_order_market_key)}"
        )

        # --- 2. Check if we have any ghost orders for this specific market ---
        if real_order_market_key not in self.ghost_order_book:
            logger.debug(f"{log_prefix} No match: No ghost orders configured for market '{repr(real_order_market_key)}'.")
            logger.debug(f"{log_prefix} DEBUG: Available Ghost Order Book Keys: {list(map(repr, self.ghost_order_book.keys()))}")
            return

        # --- 3. Determine which side of our book to check and if it has orders ---
        ghost_orders_to_check: list[BaseGhostOrder] = []
        our_side_str = ""
        if real_order_side == sphere_sdk_types_pb2.ORDER_SIDE_ASK: # Real order is an ASK, we look for BIDs
            ghost_orders_to_check = self.ghost_order_book[real_order_market_key]['bids']
            our_side_str = "bids"
            logger.debug(f"{log_prefix} Real order is an ASK. Checking Ghost BIDs.")
        elif real_order_side == sphere_sdk_types_pb2.ORDER_SIDE_BID: # Real order is a BID, we look for ASKs
            ghost_orders_to_check = self.ghost_order_book[real_order_market_key]['asks']
            our_side_str = "asks"
            logger.debug(f"{log_prefix} Real order is a BID. Checking Ghost ASKs.")

        if not ghost_orders_to_check:
            logger.debug(
                f"{log_prefix} No match: Real order is a {real_order_side_str}, but we have no Ghost {our_side_str.upper()} "
                f"for market '{repr(real_order_market_key)}'."
            )
            return
        else:
            logger.debug(f"{log_prefix} Found {len(ghost_orders_to_check)} potential Ghost {our_side_str.upper()} to check.")


        # --- 4. Iterate through our sorted list of ghost orders to find a price match ---
        match_found = False
        
        for ghost_order in list(ghost_orders_to_check):
            logger.debug(f"{log_prefix} Attempting to match with Ghost Order: {ghost_order}")
            logger.debug(f"{log_prefix} DEBUG: Ghost Order details - ID: {ghost_order.ghost_id[:8]}, Key: {repr(ghost_order.get_market_key())}, Side: {sphere_sdk_types_pb2.OrderSide.Name(ghost_order.side)}, Price: {ghost_order.price}, Remaining Qty: {ghost_order.remaining_quantity}")

            if ghost_order.remaining_quantity <= 0:
                logger.debug(f"{log_prefix} Skipping fully filled ghost order (ID: {ghost_order.ghost_id[:8]}, {ghost_order.remaining_quantity} <= 0). Removing from book.")
                self.ghost_order_book[real_order_market_key][our_side_str].remove(ghost_order)
                continue

            # Price matching logic
            is_price_match = False
            if ghost_order.side == sphere_sdk_types_pb2.ORDER_SIDE_BID: # Our BID vs Real ASK
                if ghost_order.price >= real_order_price:
                    is_price_match = True
                    logger.debug(f"{log_prefix} Price Check: Ghost BID ({ghost_order.price}) >= Real ASK ({real_order_price}). Match!")
                else:
                    logger.debug(f"{log_prefix} Price Check: Ghost BID ({ghost_order.price}) < Real ASK ({real_order_price}). No match.")
            else: # Our ASK vs Real BID
                if ghost_order.price <= real_order_price:
                    is_price_match = True
                    logger.debug(f"{log_prefix} Price Check: Ghost ASK ({ghost_order.price}) <= Real BID ({real_order_price}). Match!")
                else:
                    logger.debug(f"{log_prefix} Price Check: Ghost ASK ({ghost_order.price}) > Real BID ({real_order_price}). No match.")

            if is_price_match:
                logger.info(f"{log_prefix} MATCH FOUND with Ghost Order (ID: {ghost_order.ghost_id[:8]}): {ghost_order}.")
                logger.info(f"  - Real Order:  {real_order_side_str} {real_order_qty} @ {real_order_price} - Pos: {stack_position} Time: {updated_time}")
                logger.info(f"  - Ghost Order: {ghost_order}")

                trade_quantity = min(ghost_order.remaining_quantity, real_order_qty)
                logger.debug(f"{log_prefix} DEBUG: Calculated trade quantity: min(Ghost Remaining Qty: {ghost_order.remaining_quantity}, Real Order Qty: {real_order_qty}) = {trade_quantity}")

                if trade_quantity <= 0:
                    logger.warning(f"{log_prefix} WARNING: Calculated trade quantity is zero or negative ({trade_quantity}). Skipping trade for this ghost order.")
                    continue

                if self.execute_trade(real_order, trade_quantity, ghost_order.side):
                    ghost_order.remaining_quantity -= trade_quantity
                    logger.info(f"{log_prefix} [FILLED] Ghost order (ID: {ghost_order.ghost_id[:8]}) updated. New remaining qty: {ghost_order.remaining_quantity}")

                    if ghost_order.remaining_quantity <= 0:
                        logger.info(f"{log_prefix} Ghost order (ID: {ghost_order.ghost_id[:8]}) fully filled. Removing from order book.")
                        self.ghost_order_book[real_order_market_key][our_side_str].remove(ghost_order)

                match_found = True
                break 
            else:
                logger.debug(f"{log_prefix} Price mismatch for current ghost order (ID: {ghost_order.ghost_id[:8]}). Due to sorted list, no further ghost orders for this side will match. Breaking from loop.")
                break

        if not match_found:
            logger.info(
                f"{log_prefix} No suitable ghost order found for Real "
                f"{real_order_side_str} @ {real_order_price} after checking "
                f"candidate(s) for market '{repr(real_order_market_key)}'."
            )


    def _get_market_key_from_contract(self, contract: sphere_sdk_types_pb2.ContractDto) -> tuple | None:
        """Determines the unique market key for an incoming real contract."""
        instrument_name = contract.instrument_name.upper()
        expiry_type = contract.expiry_type

        if expiry_type == sphere_sdk_types_pb2.EXPIRY_TYPE_OUTRIGHT:
            if contract.expiry:
                generated_key = (InternalOrderType.FLAT, instrument_name, contract.expiry.upper())
                logger.debug(f"DEBUG: _get_market_key_from_contract: Generated FLAT key: {repr(generated_key)}")
                return generated_key
            else:
                logger.warning(f"Flat contract (OUTRIGHT) for '{instrument_name}' missing expiry. Skipping.")
                return None
        elif expiry_type == sphere_sdk_types_pb2.EXPIRY_TYPE_SPREAD:
            if len(contract.legs) == 2:
                sell_leg_expiry = None
                buy_leg_expiry = None

                for leg in contract.legs:
                    if leg.spread_side == sphere_sdk_types_pb2.SPREAD_SIDE_TYPE_SELL:
                        sell_leg_expiry = leg.expiry.upper()
                    elif leg.spread_side == sphere_sdk_types_pb2.SPREAD_SIDE_TYPE_BUY:
                        buy_leg_expiry = leg.expiry.upper()
        
                if sell_leg_expiry is not None and buy_leg_expiry is not None:
                    generated_key = (InternalOrderType.SPREAD, instrument_name, sell_leg_expiry, buy_leg_expiry)
                    logger.debug(f"DEBUG: _get_market_key_from_contract: Generated SPREAD key (SELL Leg then BUY Leg): {repr(generated_key)}")
                    return generated_key
                else:
                    logger.warning(f"Spread contract for '{instrument_name}' has incomplete spread_side information. Skipping.")
                    return None
            else:
                logger.warning(f"Spread contract for '{instrument_name}' has unexpected number of legs ({len(contract.legs)}). Skipping.")
                return None
        elif expiry_type == sphere_sdk_types_pb2.EXPIRY_TYPE_FLY:
            if len(contract.legs) == 3:
                first_expiry = contract.legs[0].expiry.upper()
                second_expiry = contract.legs[1].expiry.upper()
                third_expiry = contract.legs[2].expiry.upper()
                generated_key = (InternalOrderType.FLY, instrument_name, first_expiry, second_expiry, third_expiry)
                logger.debug(f"DEBUG: _get_market_key_from_contract: Generated FLY key: {repr(generated_key)}")
                return generated_key
            else:
                logger.warning(f"Fly contract for '{instrument_name}' has unexpected number of legs ({len(contract.legs)}). Skipping.")
                return None
        elif expiry_type == sphere_sdk_types_pb2.EXPIRY_TYPE_STRIP:
            # For STRIPs, we primarily use the Contract.Expiry if available (e.g., Q1-25)
            # If not, or if it's a range, we fall back to constituents.
            
            front_expiry_key: str = None
            back_expiry_key: str = None

            if contract.expiry:
                # If Contract.Expiry is like "Q1-25", use it directly for consistency
                front_expiry_key = contract.expiry.upper()
                back_expiry_key = contract.expiry.upper()
                logger.debug(f"DEBUG: _get_market_key_from_contract: STRIP detected with Contract.Expiry '{contract.expiry}'. Using it for both front and back key components.")
            
            # If contract.expiry wasn't set, or if we want to confirm the range from constituents,
            # we can still check constituents. This part is more for "Jan-26-Mar-26" type strips
            # where contract.expiry might be generic or absent, and constituents define the range.
            if contract.constituents:

                if len(contract.constituents) > 1 and \
                   (not front_expiry_key or
                    front_expiry_key == contract.constituents[0].expiry.upper() and back_expiry_key == contract.constituents[0].expiry.upper() ): # If current key is a single expiry based on contract.expiry, but constituents show a range
                    
                    # This logic handles cases where Contract.Expiry might be empty or generic,
                    # and the actual range is only defined by multiple constituents.
                    # Or, if Contract.Expiry was 'Q1-25', but the user provided Jan-25 to Mar-25,
                    # this would allow it to match if the user ghost order was also constituent-based.
                    # Given your SDK output for Q1-25, `contract.expiry` is probably the *canonical* representation.

                    if not contract.expiry:
                        front_expiry_key = contract.constituents[0].expiry.upper()
                        back_expiry_key = contract.constituents[-1].expiry.upper()
                        logger.debug(f"DEBUG: _get_market_key_from_contract: STRIP detected from constituents (no top-level Expiry). Front: '{front_expiry_key}', Back: '{back_expiry_key}'.")
                
            if front_expiry_key and back_expiry_key:
                generated_key = (InternalOrderType.STRIP, instrument_name, front_expiry_key, back_expiry_key)
                logger.debug(f"DEBUG: _get_market_key_from_contract: Generated STRIP key: {repr(generated_key)}")
                return generated_key
            else:
                logger.warning(f"Strip contract for '{instrument_name}' has insufficient expiry information (Contract.Expiry or Constituents). Skipping.")
                return None
        else:
            logger.warning(f"Unhandled ExpiryType for real order contract: {sphere_sdk_types_pb2.ExpiryType.Name(expiry_type)}. Skipping.")
            return None


    def execute_trade(self, real_order: sphere_sdk_types_pb2.OrderDto, quantity: Decimal, our_side: sphere_sdk_types_pb2.OrderSide) -> bool:
        """
        Creates and sends a trade request for a given real order, using appropriate
        trading terminology in logs.
        """
        if our_side == sphere_sdk_types_pb2.ORDER_SIDE_BID: # We are buying from a real ASK
            action_pp = "Lifting"
            action_verb = "lift"
            target_str = "offer"
        else: # We are selling to a real BID
            action_pp = "Hitting"
            action_verb = "hit"
            target_str = "bid"

        logger.info(f"--- {action_pp} the {target_str}: Trading {quantity} against real order instance ID: {real_order.instance_id[:8]} ---")
        try:
            trade_request = sphere_sdk_types_pb2.TradeOrderRequestDto(
                order_instance_id=real_order.instance_id,
                quantity=str(quantity),
                idempotency_key=str(uuid.uuid4())
            )

            self.sdk.trade_order(trade_request)

            logger.info(f"[SUCCESS] '{action_verb.capitalize()}' request for order instance ID {real_order.instance_id[:8]} submitted successfully.")
            return True

        except TradeOrderFailedError as e:
            logger.error(f"[FAILURE] Failed to {action_verb} the {target_str} on order instance ID {real_order.instance_id[:8]}. Reason: {e}")
            return False
        except Exception as e:
            logger.error(f"[UNEXPECTED] An error occurred while {action_pp.lower()}ing the {target_str} on order instance ID {real_order.instance_id[:8]}: {e}", exc_info=True)
            return False


def main():
    """
    Main function to initialize the SDK, log in, and run the ghost trading bot.
    """
    logger.info("Starting Sphere Ghost Trader Script...")
    sdk_instance = None
    try:
        sdk_instance = SphereTradingClientSDK()
        logger.info("SDK initialized.")

        username = input("Enter username: ")
        password = getpass.getpass("Enter password: ")
        sdk_instance.login(username, password)
        logger.info(f"Login successful for user '{username}'.")

        ghost_trader = GhostTrader(sdk_instance)
        ghost_trader.prompt_for_ghost_orders()

        logger.info("Subscribing to order events...")
        sdk_instance.subscribe_to_order_events(ghost_trader.on_order_event)
        logger.info("Successfully subscribed. Listening for matching orders...")
        logger.info("Press Ctrl+C to stop the bot and logout.")

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("\nCtrl+C detected. Shutting down gracefully...")
    except (SDKInitializationError, LoginFailedError, NotLoggedInError, TradingClientError) as e:
        logger.error(f"A critical SDK error occurred: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        if sdk_instance and sdk_instance._is_logged_in:
            logger.info("Unsubscribing from event streams...")
            try:
                if hasattr(sdk_instance, '_user_order_callback') and sdk_instance._user_order_callback: 
                    sdk_instance.unsubscribe_from_order_events()
                else:
                    logger.debug("DEBUG: _user_order_callback not set, skipping unsubscribe.")
            except TradingClientError as e:
                logger.warning(f"Could not cleanly unsubscribe from events: {e}")

            logger.info("Logging out...")
            sdk_instance.logout()
            logger.info("Logout complete.")

        logger.info("Sphere Ghost Trader Script has finished.")


if __name__ == "__main__":
    main()