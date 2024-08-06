import requests
import csv

# Replace 'YOUR_API_KEY' with your OpenWeatherMap API key
api_key = 'YOUR_API_KEY'
location = 'New York, US'  # Replace with the location you want to fetch weather data for
url = f'http://api.openweathermap.org/data/2.5/weather?q={location}&appid={api_key}'

# Make an API request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    
    # Extract relevant weather information
    temperature = data['main']['temp']
    humidity = data['main']['humidity']
    weather_description = data['weather'][0]['description']

    # Create a CSV file and write the data
    with open('weather_data.csv', 'w', newline='') as csv_file:
        fieldnames = ['Location', 'Temperature (°C)', 'Humidity (%)', 'Weather Description']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header row
        writer.writeheader()

        # Write the data for the location
        writer.writerow({
            'Location': location,
            'Temperature (°C)': temperature,
            'Humidity (%)': humidity,
            'Weather Description': weather_description
        })

    print(f"Data for {location} has been saved to 'weather_data.csv'")
else:
    print(f"Failed to retrieve data. Status code: {response.status_code}")
