import pandas as pd
import requests
import sqlite3
from datetime import datetime

class VaccinationDataETL:
    def __init__(self, url):
        self.url = url
        self.raw_data = None
        self.processed_data = None

    def extract(self):
        """
        Extract COVID-19 vaccination data from Our World in Data API
        """
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            self.raw_data = pd.read_csv(self.url)
            print(f"Extracted {len(self.raw_data)} records")
        except Exception as e:
            print(f"Error during extraction: {e}")
            return False
        return True

    def transform(self):
        """
        Transform and clean the vaccination data
        """
        if self.raw_data is None:
            print("No data to transform")
            return False
        
        print("Available Columns :",list(self.raw_data.columns))

        try:
            # Select and rename relevant columns
            self.processed_data = self.raw_data[[
                'location', 'date', 
                'people_vaccinated', 
                'people_fully_vaccinated', 
                'population'
            ]].copy()
        except KeyError as e:
            print("Missing column: ",{e})
            print("Please check the column name in the database")
            return False

        # Clean and process data
        self.processed_data['date'] = pd.to_datetime(self.processed_data['date'])
        
        # Calculate vaccination metrics
        self.processed_data['vaccination_rate'] = (
            self.processed_data['people_fully_vaccinated'] / 
            self.processed_data['population'] * 100
        ).round(2)

        # Handle missing values
        self.processed_data.fillna({
            'people_vaccinated': 0,
            'people_fully_vaccinated': 0,
            'vaccination_rate': 0
        }, inplace=True)

        # Remove rows with invalid data
        self.processed_data.dropna(subset=['location'], inplace=True)

        print(f"Transformed {len(self.processed_data)} records")
        return True

    def load(self, db_path='vaccination_data.db'):
        """
        Load processed data into SQLite database
        """
        if self.processed_data is None:
            print("No processed data to load")
            return False

        try:
            # Connect to SQLite database
            conn = sqlite3.connect(db_path)
            
            # Create table and load data
            self.processed_data.to_sql(
                'vaccination_data', 
                conn, 
                if_exists='replace', 
                index=False
            )

            # Create an index for faster querying
            conn.execute(
                'CREATE INDEX idx_location_date ON vaccination_data (location, date)'
            )

            conn.close()
            print(f"Data loaded to {db_path}")
        except Exception as e:
            print(f"Error during loading: {e}")
            return False
        
        return True

    def run_etl_pipeline(self):
        """
        Execute full ETL pipeline
        """
        print("Starting COVID-19 Vaccination Data ETL Pipeline")
        start_time = datetime.now()

        extract_success = self.extract()
        if not extract_success:
            return False

        transform_success = self.transform()
        if not transform_success:
            return False

        load_success = self.load()
        
        end_time = datetime.now()
        print(f"ETL Pipeline completed in {end_time - start_time}")
        
        return load_success

def main():
    # URL for COVID-19 vaccination data
    url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv'
    
    etl_pipeline = VaccinationDataETL(url)
    etl_pipeline.run_etl_pipeline()

if __name__ == '__main__':
    main()