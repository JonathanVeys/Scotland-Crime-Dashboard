import pandas as pd
import csv

def benefits_data_ingestion(data_path:str) -> pd.DataFrame:
    '''

    '''
    # Load benefits data
    try:
        data = pd.read_csv(data_path, skiprows=7)
        print(data)
    except Exception as e:
        raise ValueError(f'{e}')
    
    # with open(data_path, 'r', newline='') as f:
    #     reader = csv.reader(f)
    #     print(type(reader))
            



if __name__ == '__main__':
    data_path = '/Users/jonathancrocker/Downloads/5383272121555033.csv'
    processed_benefits_data = benefits_data_ingestion(data_path)