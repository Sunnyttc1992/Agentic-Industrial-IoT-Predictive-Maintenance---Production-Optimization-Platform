import json
import os

from awscrt import mqtt
from awsiot import mqtt_connection_builder
from dotenv import load_dotenv


load_dotenv()


class MQTTPublisher:

    def __init__(self):

        self.endpoint = os.getenv("AWS_IOT_ENDPOINT")
        self.cert_path = os.getenv("AWS_IOT_CERT")
        self.private_key_path = os.getenv("AWS_IOT_PRIVATE_KEY")
        self.root_ca_path = os.getenv("AWS_IOT_ROOT_CA")
        self.client_id = os.getenv("AWS_IOT_CLIENT_ID")

        if not self.endpoint:
            raise ValueError("AWS_IOT_ENDPOINT is missing.")

        if not self.cert_path:
            raise ValueError("AWS_IOT_CERT is missing.")

        if not self.private_key_path:
            raise ValueError("AWS_IOT_PRIVATE_KEY is missing.")

        if not self.root_ca_path:
            raise ValueError("AWS_IOT_ROOT_CA is missing.")

        if not self.client_id:
            raise ValueError("AWS_IOT_CLIENT_ID is missing.")

        self.connection = (
            mqtt_connection_builder.mtls_from_path(
                endpoint=self.endpoint,
                cert_filepath=self.cert_path,
                pri_key_filepath=self.private_key_path,
                ca_filepath=self.root_ca_path,
                client_id=self.client_id,
                clean_session=False,
                keep_alive_secs=30,
            )
        )


    def connect(self):

        print("Connecting to AWS IoT Core...")

        connect_future = self.connection.connect()

        connect_future.result()

        print("Connected to AWS IoT Core.")


    def publish(self, telemetry: dict):

        machine_id = telemetry["machine_id"]

        topic = (
            f"sunny/factory1/machines/"
            f"{machine_id}/telemetry"
        )

        payload = json.dumps(telemetry) + "\n"

        publish_future, _ = self.connection.publish(
            topic=topic,
            payload=payload,
            qos=mqtt.QoS.AT_LEAST_ONCE,
        )

        publish_future.result()

        print(
            f"Published {machine_id} -> {topic}"
        )


    def disconnect(self):

        print("Disconnecting from AWS IoT Core...")

        disconnect_future = self.connection.disconnect()

        disconnect_future.result()

        print("Disconnected.")

####----This class has one responsibility----------

# Send telemetry to AWS.
