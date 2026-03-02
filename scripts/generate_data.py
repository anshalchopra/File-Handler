import faker
import requests

class DataGenerator:
    """
    A class to generate synthetic user data and send it to a local endpoint.
    """

    def __init__(self, num_records: int = 100):
        """
        Initialize the generator with the specified number of records.
        """
        self.num_records = num_records
        self.faker = faker.Faker()

    def generate_data(self):
        """
        Generate a list of synthetic user records using the Faker library.
        """
        data = []
        for _ in range(self.num_records):
            record = {
                "first_name": self.faker.first_name(),
                "last_name": self.faker.last_name(),
                "age": self.faker.random_int(min=18, max=99),
                "country": self.faker.country()
            }
            data.append(record)
        return data

    def send_data(self):
        """
        Produce the data and send all records in bulk via a POST request.
        """
        data = self.generate_data()
        url = "http://localhost:8000/receive-bulk-data"

        try:
            # Send the JSON records list to the bulk endpoint
            response = requests.post(url, json=data)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Failed to send records: {e}")

