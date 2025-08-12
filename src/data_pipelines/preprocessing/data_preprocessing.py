import pandas as pd

def interpolate_column(df:pd.DataFrame, column_to_interpolate:str, grouping_column:str):
    '''
    Function that interpolates a column in a dataframe to fill in missing data
    '''
    df = df.set_index('Date')

    interpolated_df = (
        df[column_to_interpolate]
        .groupby(df[grouping_column])
        .resample('MS')
        .mean()
        .interpolate(method='linear')
        .reset_index()
    )

    return interpolated_df


def ward_pop_processing(ward_pop:dict) -> pd.DataFrame:
    '''
    Data pipeline that takes the raw ward population data and create a time series, creating data for each month
    '''
    processed_ward_pop = {k:df[['Electoral Ward 2022 Name', 'Electoral Ward 2022 Code', 'Sex', 'Total']].assign(Date = k) for k,df in ward_pop.items() if str(k).isdigit()}

    processed_ward_pop = pd.concat(processed_ward_pop.values(), ignore_index=True)
    
    processed_ward_pop['Date'] = pd.to_datetime(processed_ward_pop['Date'])

    processed_ward_pop = interpolate_column(processed_ward_pop, 'Total', 'Electoral Ward 2022 Code')

    return processed_ward_pop


def ward_area_processing(data:pd.DataFrame) -> pd.DataFrame:
    '''
    Reads and pre-processes the ward area data and returns the processed data from the scotland wards.

    Parameters:
    -data_path: Path pointing to the raw area data.

    Output:
    -data: Returns a dataframe with each ward and its respective area in km².
    '''
    
    data = data[['WD24CD', 'WD24NM', 'Shape__Area']]

    data = data[data['WD24CD'].str.startswith('S')].reset_index(drop=True)

    data['Shape__Area'] = data['Shape__Area'] / 1e+6

    data.rename(columns={'WD24CD':'Electoral Ward 2022 Code', 'WD24NM':'Ward_Name'}, inplace=True)

    return data


def crime_rates_processing(data:pd.DataFrame) -> pd.DataFrame:
    data.rename(columns={'COUNCIL NAME':'council_name','PSOS_MMW_Name':'ward_name', 'CALENDAR YEAR':'year', 'CALENDAR MONTH':'month', 'CRIME GROUP':'crime_group', 'GROUP DESCRIPTION':'group_description', 'DETECTED CRIME':'detected_crime'}, inplace=True)
    data = data[['council_name' ,'ward_name', 'year', 'month', 'crime_group', 'group_description', 'detected_crime']]

    data = data.groupby(['council_name','ward_name', 'year', 'month', 'crime_group', 'group_description']).sum().reset_index()

    return data