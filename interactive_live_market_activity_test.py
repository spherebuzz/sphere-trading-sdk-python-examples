import sys
import os
import logging
import getpass
import time
import threading
import datetime
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

# --------------------------------------------------------------------------- #
# Local state (needed to work out what is actually NEW between updates), plus
# a print lock so two payloads arriving on different threads (one order, one
# trade) can't interleave their lines/dividers with each other.
# --------------------------------------------------------------------------- #

_state_lock = threading.Lock()
_print_lock = threading.Lock()
# contract_key -> {order_id: OrderDto}
_order_book_state = {}

# Silence watchdog (Q1): updated every time ANY order/trade event is received,
# including SNAPSHOT. Checked once a second in the main loop.
_last_event_time = time.time()
_last_event_lock = threading.Lock()
_silence_warned = False
SILENCE_WARNING_SECONDS = 30 * 60  # 30 minutes, per client instruction

PAYLOAD_DIVIDER = "=" * 100  # separates one incoming SDK payload from the next
EVENT_DIVIDER = "-" * 100    # separates individual events within the same payload
# Note: this divider line is made up of repeated "=" characters, same as the
# "Key=Value" separator used within each event. A machine parser is not at
# risk of confusing the two: a divider line never contains a "|"-separated
# "Key=Value" pair, so anything that parses by splitting on " | " first will
# still treat this line as a single, non-matching token and can safely
# ignore or detect it as a separator.
# Neither divider character is "=", so a machine parser scanning for "Key=Value"
# pairs can never mistake a divider line for data.


def _mark_event_received():
    global _silence_warned
    with _last_event_lock:
        globals()['_last_event_time'] = time.time()
        _silence_warned = False


def _check_silence():
    """Called once a second from the main loop. Logs one warning if nothing at
    all has been received (order or trade, snapshot or otherwise) for longer
    than SILENCE_WARNING_SECONDS, so a dead connection doesn't look identical
    to a genuinely quiet market. Only warns once per silence period - it
    resets as soon as a new event arrives."""
    global _silence_warned
    with _last_event_lock:
        elapsed = time.time() - _last_event_time
        if elapsed > SILENCE_WARNING_SECONDS and not _silence_warned:
            _silence_warned = True
            minutes = int(elapsed // 60)
            test_logger.warning(
                f"No order or trade events received in over {minutes} minute(s). "
                f"This may mean the market is genuinely quiet, or that the connection has been lost "
                f"without an error being raised. Check the connection if this persists."
            )


def _enum_name(enum_type, value, prefix):
    """Safe wrapper around <EnumType>.Name(value): falls back to a labelled
    placeholder instead of raising if the server ever sends an enum value this
    version of the SDK doesn't recognise yet (e.g. after a platform update)."""
    try:
        return enum_type.Name(value).replace(prefix, '')
    except ValueError:
        return f"UNKNOWN({value})"


def _local_time_field() -> str:
    """
    A single value combining a machine-parseable timestamp (ISO 8601 with a
    numeric UTC offset) and a human-readable zone abbreviation in parentheses,
    e.g. '2026-09-16T14:04:42+01:00 (BST)'. Uses the machine's own local
    timezone. This is a RECEIPT timestamp - when this script saw the event -
    which is different from any event/execution timestamp elsewhere on the
    same line (those are the platform's own UTC timestamps, e.g. Updated=
    or Time=).

    The parenthesised abbreviation is for human readability only; a machine
    should parse the ISO 8601 portion before the first space, which is always
    present and always unambiguous regardless of what abbreviation (if any)
    the local operating system can supply.
    """
    now = datetime.datetime.now().astimezone()
    iso = now.strftime('%Y-%m-%dT%H:%M:%S%z')
    iso = iso[:-2] + ':' + iso[-2:]  # 09:12:04+0100 -> 09:12:04+01:00
    tz_label = now.strftime('%Z') or 'local'
    return f"{iso} ({tz_label})"


def _contract_key(contract):
    legs_key = tuple(
        (leg.instrument_name, leg.expiry, leg.expiry_type, leg.spread_side, tuple(c.expiry for c in leg.constituents))
        for leg in contract.legs
    )
    constituents_key = tuple(c.expiry for c in contract.constituents)
    return (
        contract.instrument_name,
        contract.expiry,
        contract.expiry_type,
        contract.side,
        contract.instrument_type,
        constituents_key,
        legs_key,
    )
    # Note: this key is built from descriptive fields rather than a single
    # canonical contract ID. If the schema ever exposes a dedicated
    # contract/instrument identifier, switch to that instead - it would be
    # strictly safer. No such field is visible anywhere in the scripts or
    # schema surface used so far, so nothing has been changed here.


def _contract_fields(contract, include_side: bool = True) -> list:
    """Labelled contract fields shared by both order and trade lines, as a
    list of already-formatted 'Key=Value' strings."""
    inst_type_str = _enum_name(sphere_sdk_types_pb2.InstrumentType, contract.instrument_type, 'INSTRUMENT_TYPE_')
    expiry_type_str = _enum_name(sphere_sdk_types_pb2.ExpiryType, contract.expiry_type, 'EXPIRY_TYPE_')

    fields = [
        f"Instrument={contract.instrument_name} ({inst_type_str})",
        f"Expiry={contract.expiry} ({expiry_type_str})",
    ]

    if include_side:
        side_str = _enum_name(sphere_sdk_types_pb2.OrderSide, contract.side, 'ORDER_SIDE_')
        fields.append(f"Side={side_str}")

    if contract.constituents:
        fields.append(f"Constituents={', '.join(c.expiry for c in contract.constituents)}")

    if contract.legs:
        leg_parts = []
        for leg in contract.legs:
            leg_side_str = _enum_name(sphere_sdk_types_pb2.SpreadSideType, leg.spread_side, 'SPREAD_SIDE_TYPE_')
            leg_instrument_name = leg.instrument_name or 'N/A'
            leg_expiry = leg.expiry or 'N/A'
            leg_parts.append(f"{leg_side_str}:{leg_instrument_name}@{leg_expiry}")
        fields.append(f"Legs={', '.join(leg_parts)}")
        # Note: legs are kept as one semi-structured field rather than fully
        # flattened into indexed keys (Leg1Side=, Leg2Side=, ...). A machine
        # consumer that needs full per-leg granularity will need one extra,
        # small parsing step for this field specifically.

    return fields


def _order_price_summary(order) -> str:
    unit_str = _enum_name(sphere_sdk_types_pb2.Unit, order.price.units, 'UNIT_')
    unit_period_str = _enum_name(sphere_sdk_types_pb2.UnitPeriod, order.price.unit_period, 'UNIT_PERIOD_')
    qty_str = f"{order.price.quantity}"
    if unit_str != 'NONE':
        qty_str += f" {unit_str}"
        if unit_period_str not in ['NONE', 'NOT_APPLICABLE', 'TOTAL_VOLUME']:
            qty_str += f"/{unit_period_str}"
        elif unit_period_str == 'TOTAL_VOLUME':
            qty_str += " (Total Volume)"
    return qty_str


def _order_party_fields(order) -> list:
    """Each fact about who's behind an order gets its own Key=Value field,
    rather than one combined sentence - easier for a machine to pick out a
    single fact (e.g. just the company code) without parsing prose."""
    fields = []
    if order.HasField('parties'):
        if order.parties.HasField('indicative_sender'):
            s = order.parties.indicative_sender
            company_type_str = _enum_name(sphere_sdk_types_pb2.CompanyType, s.company_type, 'COMPANY_TYPE_')
            fields.append(f"IndicativeSenderName={s.full_name}")
            fields.append(f"IndicativeSenderCompany={s.company_name}")
            fields.append(f"IndicativeSenderCompanyCode={s.company_code}")
            fields.append(f"IndicativeSenderCompanyType={company_type_str}")
        if order.parties.HasField('initiator_trader'):
            t = order.parties.initiator_trader
            if t.full_name or t.company_name:
                fields.append(f"InitiatorTraderName={t.full_name}")
                fields.append(f"InitiatorTraderCompany={t.company_name}")
        if order.parties.HasField('initiator_broker'):
            b = order.parties.initiator_broker
            if b.company_name:
                fields.append(f"InitiatorBrokerCompany={b.company_name}")
        if order.parties.brokers:
            codes = [b.code for b in order.parties.brokers if b.code]
            if codes:
                fields.append(f"Brokers={', '.join(codes)}")
    if order.clearing_company_codes:
        fields.append(f"Clearing={', '.join(order.clearing_company_codes)}")
    return fields


def _format_order_ticker_line(action: str, contract, order) -> str:
    """One single line per event, entirely as 'Key=Value' pairs joined by
    ' | '. No Stack Position field. LocalTime is always first."""
    interest_type_str = _enum_name(sphere_sdk_types_pb2.InterestType, order.interest_type, 'INTEREST_TYPE_')
    if hasattr(sphere_sdk_types_pb2, 'PriceSource'):
        price_source_str = _enum_name(sphere_sdk_types_pb2.PriceSource, order.price_source, 'PRICE_SOURCE_')
    else:
        price_source_str = str(getattr(order, 'price_source', ''))
    tradability_str = _enum_name(sphere_sdk_types_pb2.Tradability, order.tradability, 'TRADABILITY_')

    fields = [
        f"LocalTime={_local_time_field()}",
        f"EventType={action}",
        "Category=PRICE",
    ]
    fields += _contract_fields(contract, include_side=True)
    fields += [
        f"ID={order.id}",
        f"InstanceID={order.instance_id}",
        f"Qty={_order_price_summary(order)}",
        f"Price={order.price.per_price_unit}",
        f"Interest={interest_type_str}",
        f"PriceSource={price_source_str}",
        f"Tradable={tradability_str}",
        f"Updated={order.updated_time}",
    ]
    fields += _order_party_fields(order)

    # message_id is read via getattr rather than HasField: HasField() only
    # works on message-type/oneof/explicit-optional fields, and raises for an
    # ordinary scalar field. getattr() is safe either way and simply returns
    # the protobuf default (an empty string) if the field was never set.
    message_id = getattr(order, 'message_id', '')
    if message_id:
        fields.append(f"MessageID={message_id}")

    return " | ".join(fields)


def _format_trade_ticker_line(action: str, contract, trade) -> str:
    unit_str = _enum_name(sphere_sdk_types_pb2.Unit, trade.price.units, 'UNIT_')
    unit_period_str = _enum_name(sphere_sdk_types_pb2.UnitPeriod, trade.price.unit_period, 'UNIT_PERIOD_')
    interest_type_str = _enum_name(sphere_sdk_types_pb2.InterestType, trade.interest_type, 'INTEREST_TYPE_')

    qty_str = f"{trade.price.quantity}"
    if unit_str != 'NONE':
        qty_str += f" {unit_str}"
        if unit_period_str not in ['NONE', 'TOTAL_VOLUME']:
            qty_str += f"/{unit_period_str}"
        elif unit_period_str == 'TOTAL_VOLUME':
            qty_str += " (Total Volume)"

    fields = [
        f"LocalTime={_local_time_field()}",
        f"EventType={action}",
        "Category=TRADE",
    ]
    fields += _contract_fields(contract, include_side=False)
    fields += [
        f"TradeID={trade.id}",
        f"OrderInstanceID={trade.order_instance_id}",
        f"ClearingCo={trade.clearing_company_code}",
        f"Price={trade.price.per_price_unit}",
        f"Qty={qty_str}",
        f"Time={trade.created_time}",
        f"Interest={interest_type_str}",
    ]

    if hasattr(trade, 'broker') and trade.broker.code:
        fields.append(f"BrokerCode={trade.broker.code}")

    if trade.HasField('parties') and trade.parties.HasField('indicative_sender'):
        s = trade.parties.indicative_sender
        company_type_str = _enum_name(sphere_sdk_types_pb2.CompanyType, s.company_type, 'COMPANY_TYPE_')
        fields.append(f"SenderName={s.full_name}")
        fields.append(f"SenderCompany={s.company_name}")
        fields.append(f"SenderCompanyCode={s.company_code}")
        fields.append(f"SenderCompanyType={company_type_str}")

    return " | ".join(fields)


def _numeric_value(value):
    """
    Safely coerce a price/quantity field to a float for numeric comparison.
    The SDK's own docs describe per_price_unit and quantity as 'string/number' -
    they aren't guaranteed to always arrive as a numeric type. Returns None if
    the value can't be converted, so the caller can fall back to a plain
    equality check instead of raising (this is what caused the real-world
    TypeError: 'unsupported operand type(s) for -: str and str' when the
    platform sent these fields as strings).
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _values_differ(old_value, new_value) -> bool:
    """
    Compares two price/quantity values, whether they arrive as numbers or as
    strings. If both sides convert cleanly to float, compares numerically with
    a small tolerance (guards against floating-point serialisation noise on an
    otherwise-identical value). If either side can't be converted, falls back
    to a direct equality check rather than crashing.
    """
    old_num = _numeric_value(old_value)
    new_num = _numeric_value(new_value)
    if old_num is not None and new_num is not None:
        return abs(old_num - new_num) > 1e-9
    return old_value != new_value


def _order_changed(old_order, new_order) -> bool:
    """
    An order counts as amended only if its instance, price, or quantity has
    moved on. Stack position is deliberately NOT compared, so a pure
    queue-position shift no longer produces a line.

    Price and quantity are compared with a small tolerance when both values
    are numeric, in case these are transmitted as floating-point numbers - an
    identical value re-sent by the platform can occasionally differ at the
    last significant digit purely from serialisation, which would otherwise
    cause a false AMENDED line for a price that never actually moved. When a
    value isn't numeric (the SDK docs allow price fields to arrive as either
    a string or a number), a plain equality check is used instead.
    """
    if old_order.instance_id != new_order.instance_id:
        return True
    if _values_differ(old_order.price.per_price_unit, new_order.price.per_price_unit):
        return True
    if _values_differ(old_order.price.quantity, new_order.price.quantity):
        return True
    return False


def _order_is_stale(old_order, new_order) -> bool:
    """
    Guards against an out-of-order delivery: if the incoming order's own
    Updated timestamp is not later than the one already stored, this update
    is older than what we've already applied and is skipped rather than
    processed as if it were the latest state. ISO 8601 UTC ('...Z') timestamps
    sort correctly as plain strings, so no date parsing is needed.

    This only covers orders, which carry their own Updated timestamp. Trades
    don't carry a comparable "last changed" timestamp separate from their
    original execution time, so an equivalent staleness check isn't currently
    possible on the trade side with the fields available.
    """
    return bool(new_order.updated_time) and bool(old_order.updated_time) and new_order.updated_time < old_order.updated_time


def _print_payload(lines):
    """Print one payload's worth of events: big divider, events separated by
    a mini divider, with the whole block printed atomically so it can't
    interleave with a payload arriving on the other subscription's thread at
    the same time."""
    if not lines:
        return
    with _print_lock:
        print(PAYLOAD_DIVIDER)
        for i, line in enumerate(lines):
            print(line)
            if i < len(lines) - 1:
                print(EVENT_DIVIDER)


# --------------------------------------------------------------------------- #
# Order (price) ticker
# --------------------------------------------------------------------------- #

def on_order_event_received(order_data: sphere_sdk_types_pb2.OrderStacksDto):
    try:
        _mark_event_received()
        event_type_str = _enum_name(sphere_sdk_types_pb2.OrderStacksEventType, order_data.event_type, 'ORDER_STACKS_EVENT_TYPE_')

        if event_type_str == 'SNAPSHOT':
            total_orders = 0
            with _state_lock:
                # Clear everything first: SNAPSHOT is documented as the full
                # current book, so any contract not present in it genuinely
                # has no live orders right now. Without this clear, a
                # contract whose book emptied out while disconnected could be
                # left holding stale orders in local memory indefinitely.
                _order_book_state.clear()
                for stack in order_data.body:
                    key = _contract_key(stack.contract)
                    _order_book_state[key] = {o.id: o for o in stack.orders}
                    total_orders += len(stack.orders)
            test_logger.info(
                f"Order book initialized from SNAPSHOT: {len(order_data.body)} contract stack(s), "
                f"{total_orders} order(s) total. Ticker will report changes from here."
            )
            return

        # SNAPSHOT_AMENDED: diff against local state, collect every line this
        # payload produces (across every contract stack it touched), then
        # print the whole payload as one block.
        lines = []

        for stack in order_data.body:
            contract = stack.contract
            key = _contract_key(contract)
            current = {o.id: o for o in stack.orders}

            with _state_lock:
                previous = _order_book_state.get(key, {})

                added_ids = current.keys() - previous.keys()
                removed_ids = previous.keys() - current.keys()
                common_ids = current.keys() & previous.keys()

                for oid in added_ids:
                    lines.append(_format_order_ticker_line("NEW", contract, current[oid]))

                for oid in removed_ids:
                    lines.append(_format_order_ticker_line("REMOVED", contract, previous[oid]))

                for oid in common_ids:
                    if _order_is_stale(previous[oid], current[oid]):
                        test_logger.warning(
                            f"Skipped an out-of-order update for order id {oid}: incoming Updated="
                            f"{current[oid].updated_time} is older than the last applied Updated="
                            f"{previous[oid].updated_time}."
                        )
                        continue
                    if _order_changed(previous[oid], current[oid]):
                        lines.append(_format_order_ticker_line("AMENDED", contract, current[oid]))

                _order_book_state[key] = current

        _print_payload(lines)

    except Exception:
        # A single malformed/unexpected message should never be able to
        # silently kill this callback (and with it, all further order
        # processing). Log the full error and keep the subscription alive.
        test_logger.error("Unexpected error while processing an order event - this event was skipped.", exc_info=True)


# --------------------------------------------------------------------------- #
# Trade ticker
# --------------------------------------------------------------------------- #

def on_trade_event_received(trade_data: sphere_sdk_types_pb2.TradeMessageDto):
    try:
        _mark_event_received()
        event_type_str = _enum_name(sphere_sdk_types_pb2.TradeEventType, trade_data.event_type, 'TRADE_EVENT_TYPE_')

        if event_type_str == 'SNAPSHOT':
            test_logger.info(
                f"Trade history initialized from SNAPSHOT: {len(trade_data.body)} trade(s). "
                f"Ticker will report new activity from here."
            )
            return

        # EXECUTED / AMENDED / VOIDED: the body already only contains the
        # affected trades - collect them all and print this payload as one
        # block.
        lines = [_format_trade_ticker_line(event_type_str, trade.contract, trade) for trade in trade_data.body]
        _print_payload(lines)

    except Exception:
        test_logger.error("Unexpected error while processing a trade event - this event was skipped.", exc_info=True)


# --------------------------------------------------------------------------- #
# Session (login, subscribe, listen) - wrapped separately so main() can retry
# it exactly once on an unexpected failure without asking for credentials
# again.
# --------------------------------------------------------------------------- #

def _run_session(username, password, attempt, max_attempts):
    """
    Runs one full login -> subscribe -> listen -> logout session.
    Returns True if the session ended because the person pressed Ctrl+C
    (an intentional, clean stop - never retried), and False if it ended
    because of an unexpected error (eligible for the single automatic
    reconnect in main()).
    """
    sdk_instance = None
    clean_exit = False

    try:
        sdk_instance = SphereTradingClientSDK()
        test_logger.info("SDK Initialized successfully.")

        test_logger.info(f"Attempting login for user '{username}' (attempt {attempt}/{max_attempts})...")
        sdk_instance.login(username, password)
        test_logger.info(f"Login successful for '{username}'.")

        try:
            test_logger.info("Subscribing to order events...")
            sdk_instance.subscribe_to_order_events(on_order_event_received)
            test_logger.info("Successfully subscribed to order events.")

            test_logger.info("Subscribing to trade events...")
            sdk_instance.subscribe_to_trade_events(on_trade_event_received)
            test_logger.info("Successfully subscribed to trade events.")

            test_logger.info("Ticker is live - only new prices, removed prices, amendments, and trades will print.")
            test_logger.info(f"A warning will be logged if nothing is received for over {SILENCE_WARNING_SECONDS // 60} minutes.")
            test_logger.info("Press Ctrl+C to logout and exit.")

            _mark_event_received()  # start the silence clock from "now", not from process start
            while True:
                time.sleep(1)
                _check_silence()

        except KeyboardInterrupt:
            test_logger.info("\nCtrl+C detected. Proceeding to logout...")
            clean_exit = True
        finally:
            if sdk_instance and sdk_instance._is_logged_in and getattr(sdk_instance, '_user_order_callback', None):
                test_logger.info("Unsubscribing from order events...")
                try:
                    sdk_instance.unsubscribe_from_order_events()
                except TradingClientError as e:
                    test_logger.warning(f"Error during explicit unsubscription from order events: {e}")

            if sdk_instance and sdk_instance._is_logged_in and getattr(sdk_instance, '_user_trade_callback', None):
                test_logger.info("Unsubscribing from trade events...")
                try:
                    sdk_instance.unsubscribe_from_trade_events()
                except TradingClientError as e:
                    test_logger.warning(f"Error during explicit unsubscription from trade events: {e}")

    except (SDKInitializationError, LoginFailedError, TradingClientError) as e:
        test_logger.error(f"A critical SDK error occurred: {e}")
    except KeyboardInterrupt:
        # In case Ctrl+C lands before the inner try/except above is active
        # (e.g. during login itself).
        test_logger.info("\nCtrl+C detected during startup. Exiting...")
        clean_exit = True
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

    return clean_exit


def main():
    test_logger.info("Starting Interactive SDK Activity Ticker Test Script...")

    username = input("Enter username: ")
    password = getpass.getpass("Enter password: ")

    max_attempts = 2  # the original attempt, plus exactly one automatic reconnect
    attempt = 1
    while attempt <= max_attempts:
        clean_exit = _run_session(username, password, attempt, max_attempts)

        if clean_exit:
            break  # the person chose to stop - never retry a deliberate exit

        if attempt < max_attempts:
            test_logger.warning("Session ended unexpectedly. Attempting one automatic reconnect in 5 seconds...")
            time.sleep(5)
            attempt += 1
        else:
            test_logger.error(
                "The automatic reconnect attempt also failed. Exiting - please check the connection and/or "
                "credentials, then restart the script manually."
            )
            break

    test_logger.info("Interactive SDK Activity Ticker Test Script finished.")


if __name__ == "__main__":
    main()
