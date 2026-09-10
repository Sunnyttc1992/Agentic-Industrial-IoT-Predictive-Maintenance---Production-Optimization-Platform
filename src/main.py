import json
import time

from .simulator.factory import Factory
from .ingestion.mqtt_publisher import MQTTPublisher


def main():

    # Create the simulated factory
    factory = Factory()

    # Create MQTT publisher
    publisher = MQTTPublisher()

    # Connect to AWS IoT Core
    publisher.connect()

    print("Sunny Industrial IoT Factory Simulator")
    print("-" * 50)

    try:

        while True:

            # Generate telemetry from all 10 machines
            telemetry_data = factory.generate_all_telemetry()

            for telemetry in telemetry_data:

                # Show telemetry locally
                print(
                    json.dumps(
                        telemetry,
                        indent=4
                    )
                )

                # Send telemetry to AWS IoT Core
                publisher.publish(telemetry)

            print("=" * 50)

            # Wait 5 seconds before next batch
            time.sleep(5)

    except KeyboardInterrupt:

        print("\nSimulation stopped.")

    finally:

        # Close MQTT connection safely
        publisher.disconnect()


if __name__ == "__main__":
    main()
