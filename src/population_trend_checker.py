import matplotlib.pyplot as plt
import pandas as pd
from population_processing import population_data_ingestion


population_path = '/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/data/Raw Yearly Population Data/Scotland_Population_Data_1981-2023.xlsx'
processed_population_data = population_data_ingestion(population_path)

