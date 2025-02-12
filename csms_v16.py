import asyncio
import logging
from datetime import datetime, timedelta
import redis.asyncio as redis
import json

try:
    import websockets
except ModuleNotFoundError:
    print("This example relies on the 'websockets' package.")
    print("Please install it by running: ")
    print()
    print(" $ pip install websockets")
    import sys

    sys.exit(1)

from ocpp.routing import on
from ocpp.v16 import ChargePoint as cp
from ocpp.v16 import call_result, call, enums


logging.basicConfig(level=logging.INFO)

# Create a global Redis client.
redis_client = redis.Redis(host='localhost', port=32769, decode_responses=True)

redis_ts_keys = {
    "charger" : {
        "id":[], 
        "tag":[
            "kw", "status", "voltage", "current", "frequency", "alert:x", "alert:y", "alert:z",
            # write
            "set:kw"
            ], 
        "location": "demo"},
}

valid_id_tags = ["A787A3F2"]
connected_clients = {}  # Dictionary to store connected clients

# charging point 充電樁
# connector 充電槍
charger_config = {
    "10768751": {"charge_point_id": "10768751", "connector_ids":[1, 2], "capacity":180}, 
    "10768752": {"charge_point_id": "10768752", "connector_ids":[1, 2], "capacity":360},
    }

cp_status_dict = {}
ChargePointStatus_lst = [status.value for status in enums.ChargePointStatus]
for index in range(len(ChargePointStatus_lst)):
    cp_status_dict[ChargePointStatus_lst[index]] = index
# print(cp_status_dict)

async def add_to_redis_ts_keys(resource, id):
    r = redis_client
    ts = redis_client.ts()
    retention_time = 60 * 60 * 24 * (100) # seconds

    for _key, _value in redis_ts_keys.items():
        if _key == resource:
            for _tag in _value["tag"]:
                key_name = f"{_key}:{id}:{_tag}"
                if "alert" in _tag:
                    labels = {"type":_key, "id":id, "tag":_tag.split(':')[0], "location":_value["location"]}
                    # redis_json_alert_key_set.enqueue(f"alert:{_key}:{id}:{_tag.split(':')[-1]}")
                else:
                    labels = {"type":_key, "id":id, "tag":_tag, "location":_value["location"]}
                # print(key_name)
                # print(labels)
                if not (await r.exists(key_name)):
                    await ts.create(key_name, retention_msecs=retention_time, labels=labels)
                if (await r.exists(key_name)):
                    await ts.alter(key_name, retention_msecs=retention_time, labels=labels)
                # redis_timeseries_key_set.enqueue(key_name)
        else:
            pass


class ChargePoint(cp):
    async def start(self):
        """Handle the connection and manage the dictionary of connected clients."""
        connected_clients[self.id] = self  # Add the client to the dictionary
        logging.info(f"Client {self.id} connected. Total clients: {len(connected_clients)}")

        try:
            while True:
                message = await self._connection.recv()
                logging.info(f"Message received from {self.id}: {message}")
                await self.route_message(message)
        except websockets.ConnectionClosed:
            logging.info(f"Client {self.id} disconnected.")
        finally:
            del connected_clients[self.id]  # Remove the client from the dictionary
            logging.info(f"Client {self.id} removed. Total clients: {len(connected_clients)}")
    
    @on(enums.Action.boot_notification)
    def on_boot_notification(
        self, charge_point_vendor: str, charge_point_model: str, **kwargs
    ):
        # print(charge_point_vendor)
        # print(charge_point_model)
        return call_result.BootNotification(
            current_time=datetime.utcnow().isoformat(),
            interval=10,
            status=enums.RegistrationStatus.accepted,
        )
    
    @on(enums.Action.heartbeat)
    async def on_heartbeat(self):
        """Handler for the Heartbeat action."""
        print(f"Heartbeat received at {datetime.utcnow()}")
        return call_result.Heartbeat(
            current_time=datetime.utcnow().isoformat()
        )

    @on(enums.Action.status_notification)
    async def on_status_notification(
        self, connector_id: int, status: str, error_code: str, **kwargs
    ):
        """Handler for the StatusNotification action."""
        # print(f"StatusNotification received:")
        print(f"  Connector ID: {connector_id}")
        print(f"  Status: {status}")
        # print(f"  Error Code: {error_code}")
        # print(f"  Additional Info: {kwargs}")
        
        now_timestamp = int(datetime.now().timestamp())
        status_code = cp_status_dict.get(status, -1)
        await redis_client.ts().add(f"charger:{self.id}_{connector_id}:status", now_timestamp, status_code)
        
        if status == "Preparing":
            asyncio.create_task(send_remote_start_transaction(self, connector_id=connector_id))
        # Return an empty response, as StatusNotification does not require additional fields.
        return call_result.StatusNotification()

    @on(enums.Action.meter_values)
    async def on_meter_values(self, connector_id: int, transaction_id: int, meter_value: list):
        """Handle MeterValues messages."""
        print("MeterValues received:")
        print(f"  Connector ID: {connector_id}")
        print(f"  Transaction ID: {transaction_id}")
        print(f"  Meter Values: {meter_value}")

        for value in meter_value:
            timestamp = value["timestamp"]
            sampled_values = value["sampled_value"]
            print(f"  Timestamp: {timestamp}")
            for sampled_value in sampled_values:
                print(f"    Sampled Value: {sampled_value}")

        # await redis_client.execute_command("JSON.SET", f"cp:{self.id}:{connector_id}", "$", json.dumps(status))

        # Return an empty response as MeterValues do not require a payload.
        return call_result.MeterValues()

    @on(enums.Action.start_transaction)
    async def on_start_transaction(self, connector_id, id_tag, timestamp, meter_start, **kwargs):
        """
        Handle StartTransaction requests from the charge point.
        """
        # Log the request details
        print(f"StartTransaction received from {self.id}:")
        print(f"Connector ID: {connector_id}")
        print(f"ID Tag: {id_tag}")
        print(f"Timestamp: {timestamp}")
        print(f"Meter Start: {meter_start}")

        # Validate the idTag or connectorId if necessary
        if id_tag != "remote":
            print(f"Transaction rejected: Invalid ID Tag {id_tag}")
            return call_result.StartTransaction(
                transaction_id=0,
                id_tag_info={"status": enums.AuthorizationStatus.invalid}
            )

        # Create a new transaction ID (you can replace this with your logic to generate transaction IDs)
        transaction_id = int(datetime.utcnow().timestamp())

        # Example: Log transaction information or update a database
        print(f"Transaction {transaction_id} started for connector {connector_id} with ID Tag {id_tag}.")

        # Respond to the charge point
        return call_result.StartTransaction(
            transaction_id=transaction_id,
            id_tag_info={"status": enums.AuthorizationStatus.accepted}
        )
    
    @on(enums.Action.stop_transaction)
    async def on_stop_transaction(self, transaction_id, id_tag, timestamp, meter_stop, **kwargs):
        """
        Handle StopTransaction requests from the charge point.
        """
        # Log the request details
        print(f"StopTransaction received from {self.id}:")
        print(f"Transaction ID: {transaction_id}")
        print(f"ID Tag: {id_tag}")
        print(f"Timestamp: {timestamp}")
        print(f"Meter Stop: {meter_stop}")

        # Example: Log transaction information or update a database
        print(f"Transaction {transaction_id} stopped at {timestamp} with final meter reading {meter_stop}.")

        # Optionally, validate the transaction ID or any other parameters
        if transaction_id <= 0:
            print(f"Invalid transaction ID: {transaction_id}")
            return call_result.StopTransaction(
                id_tag_info={"status": enums.AuthorizationStatus.invalid}
            )

        # Respond to the charge point
        return call_result.StopTransaction(
            id_tag_info={"status": enums.AuthorizationStatus.accepted}
        )
    
    @on(enums.Action.authorize)
    async def on_authorize(self, id_tag: str, **kwargs):
        """
        Handle the Authorize request from the charge point.
        """
        print(f"Authorize request received for ID Tag: {id_tag}")

        # Example validation logic
        if id_tag in valid_id_tags:
            print("Authorization successful.")
            status = enums.AuthorizationStatus.accepted
        else:
            print("Authorization failed.")
            status = enums.AuthorizationStatus.invalid

        # Respond to the charge point
        return call_result.Authorize(
            id_tag_info={
                "status": status,
                "expiryDate": None,  # Optional: Include expiry date if applicable
                "parentIdTag": None  # Optional: Include parent ID Tag if applicable
            }
        )
    
    @on(enums.Action.data_transfer)
    async def on_data_transfer(self, vendor_id: str, message_id: str = None, data: str = None):
        """
        Handle DataTransfer requests from the charge point.
        """
        print(f"DataTransfer received from {self.id}:")
        print(f"  Vendor ID: {vendor_id}")
        print(f"  Message ID: {message_id}")
        print(f"  Data: {data}")

        # Example: Handle specific vendor and message IDs
        if vendor_id == "rus.avt.cp" and message_id == "GetChargeInstruction":
            # Process the request and provide appropriate instructions
            response_data = "instruction: Charge at 50% capacity"
            print("Sending charge instruction response.")
            return call_result.DataTransfer(
                status=enums.DataTransferStatus.accepted,
                data=response_data
            )

        # If the vendor or message ID is not recognized
        print(f"DataTransfer rejected for vendor ID: {vendor_id} and message ID: {message_id}.")
        return call_result.DataTransfer(
            status="Rejected",
            data="Unknown vendor or message ID"
        )
    
    async def send_clear_charging_profile(self):
        """Send a ClearChargingProfile request to the client."""
        # Construct the ClearChargingProfile request
        request = call.ClearChargingProfile(
            id=None,  # Optional: ChargingProfileId (specific profile or None for all profiles)
            connector_id=None,  # Optional: Specific connector or None for all
            charging_profile_purpose=enums.ChargingProfilePurposeType.tx_profile  # Specify the profile purpose
        )

        try:
            # Send the request to the client
            response = await self.call(request)
            if response.status == enums.ClearChargingProfileStatus.accepted:
                logging.info("ClearChargingProfile successfully accepted by the client.")
            elif response.status == enums.ClearChargingProfileStatus.unknown:
                logging.warning("ClearChargingProfile was rejected by the client.")
            else:
                logging.error(f"Unexpected status received: {response.status}")
        except Exception as e:
            logging.error(f"Error sending ClearChargingProfile: {e}")
        
    async def get_composite_schedule(self, connector_id, duration):
        print('connector_id : ' + str(connector_id) + ', duration : ' + str(duration))
        response = await self.call(
            call.GetCompositeSchedule(
                connector_id=int(connector_id),
                duration=int(duration)
            )
        )
        logging.info(response)

async def on_connect(websocket):
    """For every new charge point that connects, create a ChargePoint
    instance and start listening for messages.
    """
    print(websocket)
    # print(websocket.request.path)
    
    try:
        if not websocket.subprotocol:
            logging.warning("Client hasn't requested any Subprotocol. Closing Connection")
            return await websocket.close()

        logging.info(f"Protocols Matched: {websocket.subprotocol}")
    except Exception as e:
        logging.error(f"Error during connection handling: {e}")
        return await websocket.close()

    charge_point_id = websocket.request.path.strip("/")
    
    if charge_point_id not in charger_config:
        logging.warning(f"Connection rejected for ID: {charge_point_id}. Not in charger_config.")
        await websocket.close()
        return

    cp = ChargePoint(charge_point_id, websocket)
    logging.info(f"Charge point {charge_point_id} connected.")
    
    for connector_id in charger_config[charge_point_id]["connector_ids"]:
        await add_to_redis_ts_keys("charger", f"{charge_point_id}_{connector_id}")
    logging.info(f"Charge point {charge_point_id} redis ts keys creation.")
    # Create a stop event to manage task cancellation
    # stop_event = asyncio.Event()
    # update_task = asyncio.create_task(single_charging_profile_update(cp, stop_event))

    try:
        await cp.start()  # Main event loop for the charge point
    except Exception as e:
        logging.error(f"Error in charge point {charge_point_id}: {e}")
    finally:
        logging.info(f"Charge point {charge_point_id} disconnected.")
        # stop_event.set()  # Signal the update task to stop
        # await update_task  # Wait for the task to finish

def generate_charging_profile(watt,):
    return {
        "charging_profile_id": 1,
        "stack_level": 1,
        "charging_profile_purpose": enums.ChargingProfilePurposeType.tx_profile,
        "charging_profile_kind": enums.ChargingProfileKindType.absolute,
        "recurrency_kind": enums.RecurrencyKind.daily,
        "valid_from": datetime.utcnow().isoformat(),
        "valid_to": (datetime.utcnow() + timedelta(seconds=60)).isoformat(),
        "charging_schedule": {
            "duration": 60,
            "start_schedule": datetime.utcnow().isoformat(),
            "charging_rate_unit": "W",
            "charging_schedule_period": [
                {"start_period": 0, "limit": watt, "number_phases": 3},
            ],
        },
    }

async def send_set_charging_profile(charge_point, connector_id, watt=7000):
    charging_profile = generate_charging_profile(watt)
    request = call.SetChargingProfile(
        connector_id=connector_id,
        cs_charging_profiles=charging_profile,
    )
    try:
        response = await charge_point.call(request)
        if response.status == enums.ChargingProfileStatus.accepted:
            print(f"Charging profile set successfully for {charge_point.id}")
        else:
            print(f"Charging profile was rejected: {response.status}")
    except Exception as e:
        print(f"Error sending charging profile: {e}")

async def get_client_configurations(charge_point):
    """Query charge point configuration for status-related keys."""
    request = call.GetConfiguration(key=["ConnectorStatus"])
    response = await charge_point.call(request)
    print(f"Configuration for {charge_point.id}: {response}")

async def single_charging_profile_update(charge_point, stop_event):
    """Update the charging profile every 60 seconds."""
    while not stop_event.is_set():  # Check if stop_event is triggered
        connector_id = 1
        await send_set_charging_profile(charge_point, connector_id)
        await asyncio.sleep(60)  # Run every 60 seconds

async def send_remote_start_transaction(charge_point, id_tag="remote", connector_id=None):
    """
    Send a RemoteStartTransaction request to the charge point.

    :param charge_point: The ChargePoint instance to send the request to.
    :param id_tag: The ID Tag used for authentication.
    :param connector_id: The specific connector to start (optional).
    """
    request = call.RemoteStartTransaction(
        id_tag=id_tag,
        connector_id=connector_id  # Optional: Set this to None if targeting any available connector
    )

    try:
        response = await charge_point.call(request)
        # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        if response.status == enums.RemoteStartStopStatus.accepted:
            print(f"Remote start transaction request accepted by {charge_point.id}.")
        else:
            print(f"Remote start transaction request rejected by {charge_point.id}: {response.status}")
    except Exception as e:
        print(f"Failed to send remote start request to {charge_point.id}: {e}")

async def send_remote_stop_transaction(charge_point, id_tag="remote", connector_id=None):
    """
    Send a RemoteStopTransaction request to the charge point.

    :param charge_point: The ChargePoint instance to send the request to.
    :param id_tag: The ID Tag used for authentication.
    :param connector_id: The specific connector to stop (optional).
    """
    request = call.RemoteStopTransaction(
        id_tag=id_tag,
        connector_id=connector_id  # Optional: Set this to None if targeting any available connector
    )

    try:
        response = await charge_point.call(request)
        # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        if response.status == enums.RemoteStartStopStatus.accepted:
            print(f"Remote stop transaction request accepted by {charge_point.id}.")
        else:
            print(f"Remote stop transaction request rejected by {charge_point.id}: {response.status}")
    except Exception as e:
        print(f"Failed to send remote stop request to {charge_point.id}: {e}")

async def site_charging_profile_update():
    """Update the charging profile every 60 seconds."""
    while True:
        if connected_clients:
            print("site_charging_profile_update")  
            demand_cap = 300
            total_cap = 0
            for charge_point_id, charge_point in connected_clients.items():
                # await get_client_configurations(charge_point)
                total_cap += charger_config[charge_point_id]["capacity"]
            
            if total_cap > 0:    
                for charge_point_id, charge_point in connected_clients.items():
                    # await get_client_configurations(charge_point)
                    _cap = charger_config[charge_point_id]["capacity"]
                    connector_ids = charger_config[charge_point_id]["connector_ids"]
                    _cap_connector = demand_cap * (_cap / total_cap) / len(connector_ids)
                    print(_cap_connector)
                    for i in connector_ids:
                        await send_set_charging_profile(charge_point, i, watt=_cap_connector)
        await asyncio.sleep(30)

async def main():
    server = await websockets.serve(
        on_connect, "0.0.0.0", 9000, subprotocols=["ocpp1.6"]
    )

    asyncio.create_task(site_charging_profile_update())

    logging.info("Server Started listening to new connections...")

    await server.wait_closed()

if __name__ == "__main__": 
    asyncio.run(main())