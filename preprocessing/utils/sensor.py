import time

from preprocessing.utils.general import timeit


class Sensor:
    def __init__(self):
        pass

    @staticmethod
    @timeit
    def wait_for_service(func, *args, **kwargs):
        print("Waiting for service...")

        while True:
            result = func(*args, **kwargs)  # Call the function with provided arguments
            print(f"Function result: {result}")

            if result:  # Example condition to stop waiting
                print("Service is ready!")
                break

            time.sleep(10)  # Poll every 5 seconds
