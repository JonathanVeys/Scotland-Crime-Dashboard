import pandas as pd
import os

def crime_data_ingestion(dir_path):
    data = pd.DataFrame()
    for file in os.listdir(dir_path):
        absolute_path = dir_path + '/' + file

        if not absolute_path.endswith('.xlsx'):
            continue
        
        print(f'Loading: {file}')
        yearly_data = pd.read_excel(absolute_path)
        data = pd.concat([data, yearly_data], ignore_index=True)
    return(data)


def crime_data_processing(data):
    data = data[['COUNCIL NAME', 'CALENDAR YEAR', 'CALENDAR MONTH', 'CRIME CLASSIFICATION DESCRIPTION', 'CRIME GROUP', 'GROUP DESCRIPTION', 'DETECTED CRIME']]
    return(data)


if __name__ == '__main__':
    crime_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Raw Yearly Crime Data'
    unprocessed_crime_data = crime_data_ingestion(crime_path)
    processed_crime_data = crime_data_processing(unprocessed_crime_data)
    print(processed_crime_data['CALENDAR YEAR'].unique())

