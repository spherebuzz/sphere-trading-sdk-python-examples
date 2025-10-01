import unittest
import os
import threading
import uuid
from venv import create
import requests
import json
import sys
from decimal import Decimal # Import Decimal for consistent price/quantity handling in tests

from test_common import (
    SphereTradingClientSDK,
    SDKInitializationError,
    LoginFailedError,
    TradingClientError,
    UpdateOrderFailedError,
    NotLoggedInError,
    sphere_sdk_types_pb2,
    VALID_USERNAME,
    VALID_PASSWORD
)

class TestUpdateOrderE2E(unittest.TestCase):
    sdk_instance = None
    base_url = None

    @classmethod
    def setUpClass(cls):
        """
        Initializes the SDK and retrieves the base URL for testing.
        Skips all tests in this class if initialization fails.
        """
        try:
            cls.sdk_instance = SphereTradingClientSDK()
            cls.base_url = os.environ.get('SPHERE_BACKEND_BASE_URL')
            if not cls.base_url:
                raise unittest.SkipTest("SPHERE_BACKEND_BASE_URL environment variable is not set.")
        except SDKInitializationError as e:
            raise unittest.SkipTest(f"SDK Initialization failed, skipping E2E tests: {e}.")
        except Exception as e:
            raise unittest.SkipTest(f"An unexpected error occurred during SDK setup: {e}")

    def setUp(self):
        """
        Logs in before each test and prepares resources for handling async callbacks.
        """
        if self.sdk_instance and self.sdk_instance._is_logged_in:
            self.sdk_instance.logout()

        try:
            self.sdk_instance.login(VALID_USERNAME, VALID_PASSWORD)
        except LoginFailedError as e:
            self.fail(f"Login is a prerequisite for this test and it failed: {e}")
        self.assertTrue(self.sdk_instance._is_logged_in, "SDK must be logged in for this test.")

        self.received_event = threading.Event()
        self.received_order_data = {}

    def tearDown(self):
        """
        Unsubscribes from events and logs out after each test to ensure isolation.
        """
        if self.sdk_instance:
            if hasattr(self.sdk_instance, '_user_order_callback') and self.sdk_instance._user_order_callback:
                try:
                    self.sdk_instance.unsubscribe_from_order_events()
                except (TradingClientError, NotLoggedInError) as e:
                    print(f"WARNING: Harmless error during unsubscription in tearDown: {e}", file=sys.stderr)
            if self.sdk_instance._is_logged_in:
                try:
                    self.sdk_instance.logout()
                except (TradingClientError, NotLoggedInError) as e:
                    print(f"WARNING: Harmless error during logout in tearDown: {e}", file=sys.stderr)

    def test_update_trader_flat_order_succeeds(self):
        """
        Tests that updating a flat order for a trader succeeds and sends the correct
        payload to the backend, including all nested DTOs.
        """

        # 1. Define unique IDs for the test entities to ensure isolation.
        order_id = f"order_{uuid.uuid4().hex}"
        order_instance_id = f"orderinstance_{uuid.uuid4().hex}"
        price_id = f"price_{uuid.uuid4().hex}"

        # 2. Define the price/order to be injected into the system.
        # This is equivalent to pre-loading the order into the client's cache.
        test_price_payload = {
            "prices": [{
                "id": price_id,
                "externalId": order_id,
                "externalInstanceId": order_instance_id,
                "expiryName": "Jun 25",
                "instrumentId": "instrument-id",
                "instrumentName": "testInstrumentPy",
                "side": "b",
                "value": 5.3,
                "quantity": 99,
                "priceType": "flat",
                "interestType": "Live",
                'units': 'kb',
                'unitPeriod': 'Month',
                "time": "2025-06-23T23:12:00+00:00",
                "expiries": [{
                    "id": "expiry-jun25-Id",
                    "shortName": "Jun-25",
                    "tradingEndDate": "2025-05-31T23:59:00+00:00",
                    "deliveryEndDate": "2025-06-30T23:59:00+00:00"
                }],
                "receiverType": "trader",
                "brokerCodes": ["BC1"],
                "clearingCompanyCodes": ["ICE"]
            }]
        }

        # 3. Stub the backend's order update execution API endpoint to return a successful response.
        stubbed_update_order_response = {
            "externalId": f"order_{uuid.uuid4().hex}"
        }

        stub_order_update_url = f"{self.base_url}/_testing/price/update?statusCode=201" 
        try:
            response = requests.post(stub_order_update_url, json=stubbed_update_order_response, timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.fail(f"Failed to stub order update endpoint '{stub_order_update_url}': {e}")

        # 4. Set up a subscription to wait for the injected price to arrive in the client.
        def on_order_event_received(order_data: sphere_sdk_types_pb2.OrderStacksDto):
            for stack in order_data.body:
                for order in stack.orders:
                    if order.id == order_id:
                        self.received_order_data['order'] = order
                        self.received_event.set()
                        return
        
        self.sdk_instance.subscribe_to_order_events(on_order_event_received)

        # 5. Inject the price using the test SignalR endpoint.
        inject_url = f"{self.base_url}/_testing/signalr/inject?method=ReceiveDataMessage&arg1=LIVE_PRICE&arg2=ADDED"
        try:
            response = requests.post(inject_url, json=test_price_payload, timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.fail(f"Failed to inject test order via HTTP endpoint '{inject_url}': {e}")
        
        # 6. Wait until the SDK has processed the new order.
        timeout_seconds = 5
        event_was_set = self.received_event.wait(timeout=timeout_seconds)
        self.assertTrue(event_was_set, f"Timeout: Did not receive test order (ID: {order_id}) within {timeout_seconds}s.")

        # 7. Execute the order update.                
        idempotency_key = f"idempotency_key_{uuid.uuid4().hex}"        
        quantity_value = Decimal("100")
        per_price_unit = Decimal("5.25")
        primary_broker_code = "BC1"
        secondary_broker_code = "BC2"
        clearing_option_code = "ice"        
            
        price_dto = sphere_sdk_types_pb2.OrderRequestPriceDto(
            per_price_unit=str(per_price_unit),
            quantity=str(quantity_value),
            ordered_clearing_options=[
                sphere_sdk_types_pb2.OrderRequestClearingOptionDto(code=clearing_option_code)
            ]
        )

        primary_broker_dto = sphere_sdk_types_pb2.OrderRequestBrokerDto(
            code=primary_broker_code
        )

        secondary_brokers_dtos = [
            sphere_sdk_types_pb2.OrderRequestBrokerDto(code=secondary_broker_code)
        ]

        parties_dto = sphere_sdk_types_pb2.TraderOrderRequestPartiesDto(
            primary_broker=primary_broker_dto,
            secondary_brokers=secondary_brokers_dtos
        )

        order_update_request = sphere_sdk_types_pb2.TraderUpdateFlatOrderRequestDto(
            instance_id=self.received_order_data['order'].instance_id,
            idempotency_key=idempotency_key,
            price=price_dto,
            parties=parties_dto
        )

        try:
            order_result: sphere_sdk_types_pb2.OrderResponseDto = self.sdk_instance.update_trader_flat_order(order_update_request)
            
            self.assertIsNotNone(order_result)
            self.assertIsInstance(order_result.id, str)
            self.assertEqual(order_result.id, stubbed_update_order_response["externalId"])
            self.assertIsInstance(order_result.instance_id, str)
            self.assertTrue(order_result.instance_id)
            
        except UpdateOrderFailedError as e:
            self.fail(f"SDK failed to update trader flat order: {e}")
        except Exception as e:
            self.fail(f"An unexpected error occurred during flat order update: {e}")
        
        #Check captured request from fake server
        capture_url = f"{self.base_url}/_testing/price/update" 
        try:
            captured_response = requests.get(capture_url, timeout=5)
            captured_response.raise_for_status()
            captured_body_json = captured_response.json()
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            self.fail(f"Failed to retrieve or parse captured flat order update request from '{capture_url}': {e}")
            
        expected_captured_body = {'expiryIds': ['expiry-jun25-Id'],
         'brokerCodes': ['BC1', 'BC2'],
         'priceType': 'Flat',
         'instrumentId': 'instrument-id',
         'interestType': 'Live',
         'value': 5.25,
         'quantity': 100,
         'units': 'Kb',
         'unitPeriod': 'Month',
         'side': 'b',
         'preferredClearingCompanyId': 'ice-id',
         'preferredClearingCompanyType': 'only',
         'goodUntil': 3}
        
        self.maxDiff = None
        self.assertEqual(captured_body_json["requestBody"], expected_captured_body)

    def test_update_trader_flat_order_fails(self):
        """
        Tests that updating a flat order for a trader succeeds and sends the correct
        payload to the backend, including all nested DTOs.
        """
        # 1. Define unique IDs for the test entities to ensure isolation.
        order_id = f"order_{uuid.uuid4().hex}"
        order_instance_id = f"orderinstance_{uuid.uuid4().hex}"
        price_id = f"price_{uuid.uuid4().hex}"

        # 2. Define the price/order to be injected into the system.
        # This is equivalent to pre-loading the order into the client's cache.
        test_price_payload = {
            "prices": [{
                "id": price_id,
                "externalId": order_id,
                "externalInstanceId": order_instance_id,
                "expiryName": "Jun 25",
                "instrumentId": "instrument-id",
                "instrumentName": "testInstrumentPy",
                "side": "b",
                "value": 5.3,
                "quantity": 99,
                "priceType": "flat",
                "interestType": "Live",
                'units': 'kb',
                'unitPeriod': 'Month',
                "time": "2025-06-23T23:12:00+00:00",
                "expiries": [{
                    "id": "expiry-jun25-Id",
                    "shortName": "Jun-25",
                    "tradingEndDate": "2025-05-31T23:59:00+00:00",
                    "deliveryEndDate": "2025-06-30T23:59:00+00:00"
                }],
                "receiverType": "trader",
                "brokerCodes": ["BC1"],
                "clearingCompanyCodes": ["ICE"]
            }]
        }

        # 3. Stub the backend's order update execution API endpoint to return a 500 response.
        stubbed_update_order_response = {
        }

        stub_order_creation_url = f"{self.base_url}/_testing/price?statusCode=500" 
        try:
            response = requests.post(stub_order_creation_url, json=stubbed_update_order_response, timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.fail(f"Failed to stub order update endpoint '{stub_order_creation_url}': {e}")
        
            # 4. Set up a subscription to wait for the injected price to arrive in the client.
        def on_order_event_received(order_data: sphere_sdk_types_pb2.OrderStacksDto):
            for stack in order_data.body:
                for order in stack.orders:
                    if order.id == order_id:
                        self.received_order_data['order'] = order
                        self.received_event.set()
                        return
        
        self.sdk_instance.subscribe_to_order_events(on_order_event_received)

        # 5. Inject the price using the test SignalR endpoint.
        inject_url = f"{self.base_url}/_testing/signalr/inject?method=ReceiveDataMessage&arg1=LIVE_PRICE&arg2=ADDED"
        try:
            response = requests.post(inject_url, json=test_price_payload, timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.fail(f"Failed to inject test order via HTTP endpoint '{inject_url}': {e}")
        
        # 6. Wait until the SDK has processed the new order.
        timeout_seconds = 5
        event_was_set = self.received_event.wait(timeout=timeout_seconds)
        self.assertTrue(event_was_set, f"Timeout: Did not receive test order (ID: {order_id}) within {timeout_seconds}s.")

        # 7. Execute the order update.                
        idempotency_key = f"idempotency_key_{uuid.uuid4().hex}"

        quantity_value = Decimal("100")
        per_price_unit = Decimal("5.25")
        primary_broker_code = "BC1"
        secondary_broker_code = "BC2"
        clearing_option_code = "ice"

        price_dto = sphere_sdk_types_pb2.OrderRequestPriceDto(
            per_price_unit=str(per_price_unit),
            quantity=str(quantity_value),
            ordered_clearing_options=[
                sphere_sdk_types_pb2.OrderRequestClearingOptionDto(code=clearing_option_code)
            ]
        )

        primary_broker_dto = sphere_sdk_types_pb2.OrderRequestBrokerDto(
            code=primary_broker_code
        )

        secondary_brokers_dtos = [
            sphere_sdk_types_pb2.OrderRequestBrokerDto(code=secondary_broker_code)
        ]

        parties_dto = sphere_sdk_types_pb2.TraderOrderRequestPartiesDto(
            primary_broker=primary_broker_dto,
            secondary_brokers=secondary_brokers_dtos
        )

        order_update_request = sphere_sdk_types_pb2.TraderUpdateFlatOrderRequestDto(
            instance_id=self.received_order_data['order'].instance_id,
            idempotency_key=idempotency_key,
            price=price_dto,
            parties=parties_dto
        )

        with self.assertRaises(UpdateOrderFailedError) as cm:
            self.sdk_instance.update_trader_flat_order(order_update_request)
        
        self.assertEqual(
            "Update flat order failed: Failed to execute order update.",
            str(cm.exception)
        )

    def test_update_trader_flat_order_invalid_broker_fails(self):
        """
        Tests that updating a flat order for a trader succeeds and sends an invalid broker
        """

        # 1. Define unique IDs for the test entities to ensure isolation.
        order_id = f"order_{uuid.uuid4().hex}"
        order_instance_id = f"orderinstance_{uuid.uuid4().hex}"
        price_id = f"price_{uuid.uuid4().hex}"

        # 2. Define the price/order to be injected into the system.
        # This is equivalent to pre-loading the order into the client's cache.
        test_price_payload = {
            "prices": [{
                "id": price_id,
                "externalId": order_id,
                "externalInstanceId": order_instance_id,
                "expiryName": "Jun 25",
                "instrumentId": "instrument-id",
                "instrumentName": "testInstrumentPy",
                "side": "b",
                "value": 5.3,
                "quantity": 99,
                "priceType": "flat",
                "interestType": "Live",
                'units': 'kb',
                'unitPeriod': 'Month',
                "time": "2025-06-23T23:12:00+00:00",
                "expiries": [{
                    "id": "expiry-jun25-Id",
                    "shortName": "Jun-25",
                    "tradingEndDate": "2025-05-31T23:59:00+00:00",
                    "deliveryEndDate": "2025-06-30T23:59:00+00:00"
                }],
                "receiverType": "trader",
                "brokerCodes": ["BC1"],
                "clearingCompanyCodes": ["ICE"]
            }]
        }        
        
        # 3. Set up a subscription to wait for the injected price to arrive in the client.
        def on_order_event_received(order_data: sphere_sdk_types_pb2.OrderStacksDto):
            for stack in order_data.body:
                for order in stack.orders:
                    if order.id == order_id:
                        self.received_order_data['order'] = order
                        self.received_event.set()
                        return
        
        self.sdk_instance.subscribe_to_order_events(on_order_event_received)

        # 4. Inject the price using the test SignalR endpoint.
        inject_url = f"{self.base_url}/_testing/signalr/inject?method=ReceiveDataMessage&arg1=LIVE_PRICE&arg2=ADDED"
        try:
            response = requests.post(inject_url, json=test_price_payload, timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.fail(f"Failed to inject test order via HTTP endpoint '{inject_url}': {e}")
        
        # 5. Wait until the SDK has processed the new order.
        timeout_seconds = 5
        event_was_set = self.received_event.wait(timeout=timeout_seconds)
        self.assertTrue(event_was_set, f"Timeout: Did not receive test order (ID: {order_id}) within {timeout_seconds}s.")

        # 6. Execute the order update.                
        idempotency_key = f"idempotency_key_{uuid.uuid4().hex}"

        quantity_value = Decimal("100")
        per_price_unit = Decimal("5.25")
        primary_broker_code = "INVALID"
        secondary_broker_code = "BC2"
        clearing_option_code = "ice"

        price_dto = sphere_sdk_types_pb2.OrderRequestPriceDto(
            per_price_unit=str(per_price_unit),
            quantity=str(quantity_value),
            ordered_clearing_options=[
                sphere_sdk_types_pb2.OrderRequestClearingOptionDto(code=clearing_option_code)
            ]
        )

        primary_broker_dto = sphere_sdk_types_pb2.OrderRequestBrokerDto(
            code=primary_broker_code
        )

        secondary_brokers_dtos = [
            sphere_sdk_types_pb2.OrderRequestBrokerDto(code=secondary_broker_code)
        ]

        parties_dto = sphere_sdk_types_pb2.TraderOrderRequestPartiesDto(
            primary_broker=primary_broker_dto,
            secondary_brokers=secondary_brokers_dtos
        )

        order_update_request = sphere_sdk_types_pb2.TraderUpdateFlatOrderRequestDto(
            instance_id=self.received_order_data['order'].instance_id,
            idempotency_key=idempotency_key,
            price=price_dto,
            parties=parties_dto
        )

        with self.assertRaises(UpdateOrderFailedError) as cm:
            self.sdk_instance.update_trader_flat_order(order_update_request)
        
        self.assertEqual(
            "Update flat order failed: Invalid Broker.",
            str(cm.exception)
        )

if __name__ == '__main__':
    unittest.main()