"""
Broker Curve Monitor

Logs in, prints your company's Broker Curve Manager curve, then checks it every second and prints each value
that changes, appears or goes. Each line names the instrument, the expiry, the value as the Sphere app displays
it, and where the value comes from:

    PROVIDER             the company's client curve, as its provider publishes it
    CUSTOM               an override entered by a user at the company in Broker Curve Manager
    CALCULATED_MIDPOINT  derived from the provider values and overrides

The curve is available to broker users with Broker Curve Manager enabled; for anyone else it is empty.
Press Ctrl+C to log out and exit.
"""
import sys
import os
import logging
import getpass
import time
import datetime

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
        TradingClientError,
        GetCurveFailedError
    )
    from sphere_sdk import sphere_sdk_types_pb2
except ImportError as e:
    print(f"Error importing SDK modules: {e}")
    print(f"Please ensure 'sphere_sdk' is in PYTHONPATH or the structure is correct.")
    print(f"Attempted to add '{src_dir}' to sys.path.")
    sys.exit(1)

logger = logging.getLogger("broker_curve_monitor")
logging.basicConfig(level=logging.INFO, format='[BROKER_CURVE %(levelname)s] %(asctime)s: %(message)s')

POLL_SECONDS = 1


def _source_name(value):
    """The source without its enum prefix, or a labelled placeholder for a value this SDK version does not know."""
    try:
        return sphere_sdk_types_pb2.CurveValueSource.Name(value).replace('CURVE_VALUE_SOURCE_', '')
    except ValueError:
        return f"UNKNOWN({value})"


def curve_by_cell(points):
    """{(instrument, expiry): (display value, source)}, keeping the order the SDK returned them in."""
    return {
        (point.contract.instrument_name, point.contract.expiry): (point.display_value, _source_name(point.source))
        for point in points
    }


def changes_between(previous, current):
    """[(instrument, expiry, old, new)] for every cell that changed, appeared (old None) or went (new None).

    old and new are (display value, source); a change of source alone counts, since it says the value now
    comes from somewhere else even where the number is the same.
    """
    changes = []
    for cell, now in current.items():
        before = previous.get(cell)
        if before != now:
            changes.append((cell[0], cell[1], before, now))
    for cell, before in previous.items():
        if cell not in current:
            changes.append((cell[0], cell[1], before, None))
    return changes


def format_change(instrument, expiry, old, new):
    if old is None:
        return f"{instrument} | {expiry} | new {new[0]} ({new[1]})"
    if new is None:
        return f"{instrument} | {expiry} | gone, was {old[0]} ({old[1]})"
    source = new[1] if old[1] == new[1] else f"{old[1]} -> {new[1]}"
    return f"{instrument} | {expiry} | {old[0]} -> {new[0]} ({source})"


def print_curve(curve):
    if not curve:
        logger.info("The broker curve is empty: Broker Curve Manager is not enabled for this user, or the "
                    "provider's curve has not arrived yet.")
        return

    logger.info(f"Broker curve: {len(curve)} values.")
    for (instrument, expiry), (value, source) in curve.items():
        print(f"{instrument} | {expiry} | {value} ({source})")


def monitor(sdk_instance):
    """Prints the curve, then each change to it, until interrupted."""
    previous = None
    last_failure = None

    while True:
        try:
            current = curve_by_cell(sdk_instance.get_broker_curve())
        except GetCurveFailedError as e:
            # The curve cannot be calculated at the moment (the SDK reconnecting, say); asking again is safe.
            if str(e) != last_failure:
                logger.warning(f"{e}. Trying again every {POLL_SECONDS}s.")
                last_failure = str(e)
            time.sleep(POLL_SECONDS)
            continue

        if last_failure is not None:
            logger.info("The broker curve is available again.")
            last_failure = None

        if previous is None:
            print_curve(current)
            logger.info("Watching for changes. Press Ctrl+C to logout and exit.")
        else:
            changes = changes_between(previous, current)
            if changes:
                stamp = datetime.datetime.now().strftime('%H:%M:%S')
                for change in changes:
                    print(f"{stamp} | {format_change(*change)}")

        previous = current
        time.sleep(POLL_SECONDS)


def main():
    logger.info("Starting Broker Curve Monitor...")

    sdk_instance = None

    try:
        sdk_instance = SphereTradingClientSDK()
        logger.info("SDK initialized.")

        username = input("Enter username: ")
        password = getpass.getpass("Enter password: ")
        sdk_instance.login(username, password)
        logger.info(f"Login successful for user '{username}'.")

        monitor(sdk_instance)

    except KeyboardInterrupt:
        logger.info("\nCtrl+C detected. Shutting down...")
    except (SDKInitializationError, LoginFailedError, NotLoggedInError, TradingClientError) as e:
        logger.error(f"A critical SDK error occurred: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred in the main script: {e}", exc_info=True)
    finally:
        if sdk_instance and sdk_instance._is_logged_in:
            logger.info("\nLogging out...")
            sdk_instance.logout()
            logger.info("Logout complete.")

        logger.info("Broker Curve Monitor has finished.")


if __name__ == "__main__":
    main()
