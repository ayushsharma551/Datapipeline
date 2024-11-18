
import requests
import csv

# Constants
API_KEY = "ln0vq8liSoorAF13jBUdyyiIFelqXbm8"  
BASE_URL = "https://api.nytimes.com/svc/books/v3/lists/current"
CATEGORY = "hardcover-fiction"  
OUTPUT_FILE = "nytimes_best_sellers.csv"  

# Fetch data from the API
def fetch_best_sellers(api_key, category):
    url = f"{BASE_URL}/{category}.json"
    params = {"api-key": api_key}
    response = requests.get(url, params=params)

    if response.status_code == 200:
        return response.json()['results']['books']
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return []
    
# Write data to a CSV file
def write_to_csv(data, filename):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Write header
        writer.writerow(["Rank", "Title", "Author", "Publisher", "Description"])
        # Write book data
        for book in data:
            writer.writerow([
                book.get("rank"),
                book.get("title"),
                book.get("author"),
                book.get("publisher"),
                book.get("description")
            ])
    print(f"Data successfully written to {filename}")

# Main script
def main():
    print("Fetching data from NYTimes Books API...")
    books = fetch_best_sellers(API_KEY, CATEGORY)
    if books:
        print(f"Fetched {len(books)} books. Writing to CSV...")
        write_to_csv(books, OUTPUT_FILE)
    else:
        print("No data available.")

if __name__ == "__main__":
    main()