import csv
from collections import defaultdict
from multiprocessing import Pool


# Map function for passenger flight data
def map_function(filename):
    """
    This function will map each row to a (passenger_id, 1) key-value pair.
    Each (passenger_id, 1) pair indicates one flight for the passenger.
    """
    passenger_flights = defaultdict(int)
    with open(filename, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            try:
                passenger_id = row[0]  # Assuming passenger ID is in the first column
                passenger_flights[passenger_id] += 1
            except IndexError:
                continue  # Skip rows that do not have valid data
    return passenger_flights


# Reduce function to aggregate results
def reduce_function(all_flights, file_flights):
    """
    This function will reduce the flight counts from different files.
    It will sum up all the flights for each passenger across different files.
    """
    for passenger_id, num_flights in file_flights.items():
        all_flights[passenger_id] += num_flights
    return all_flights


# Find the passenger with the most flights in a given dataset
def find_max_passenger(passenger_flights):
    """
    This function finds the passenger with the most flights from the given dictionary.
    """
    max_passenger = max(passenger_flights, key=passenger_flights.get)
    max_flights = passenger_flights[max_passenger]
    return max_passenger, max_flights


# Main function to process multiple files and compute the final results
def process_data():
    # List of files to process
    data_files = [
        'data/AComp_Passenger_data.csv',
        'data/AComp_Passenger_data_no_error.csv',
        'data/AComp_Passenger_data_no_error_DateTime.csv'
    ]

    # Use multiprocessing to map and reduce across files
    with Pool(processes=len(data_files)) as pool:
        # Step 1: Apply map function across all files in parallel
        results = pool.map(map_function, data_files)

    # Step 2: Combine the results from all files using the reduce function
    all_flights = defaultdict(int)
    for file_flights in results:
        all_flights = reduce_function(all_flights, file_flights)

    # Output results for each file
    for i, file_flights in enumerate(results):
        file_max_passenger, file_max_flights = find_max_passenger(file_flights)
        print(
            f"The passenger with the most flights in file {data_files[i]} is: {file_max_passenger}, Flights: {file_max_flights}")

    # Step 3: Find the passenger with the most flights across all files
    max_passenger, max_flights = find_max_passenger(all_flights)
    print(f"\nThe passenger with the most flights across all files is: {max_passenger}, Total flights: {max_flights}")


if __name__ == '__main__':
    process_data()