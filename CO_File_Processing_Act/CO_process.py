import pandas as pd
import numpy as np

def CO_Process_first(path):

    result = pd.read_csv(path)

    # Convert columns to appropriate data types
    result['reporttime'] = pd.to_datetime(result['reporttime'], errors='coerce')
    result['spantime'] = pd.to_datetime(result['spantime'], errors='coerce')

    # Sort DataFrame by 'reporttime' for the logic to work correctly
    result.sort_values(by=['reporttime'],ascending=True, inplace=True)

    # Define a function to calculate LastSpanType
    def calculate_last_span_type(group):
        group['LastSpanType'] = group['spantype'].shift(1)
        return group

    # Apply the function to create the 'LastSpanType' column
    result = result.groupby('imono').apply(calculate_last_span_type)

    # Reset the index after the groupby operation
    result.reset_index(drop=True, inplace=True)
    result['reporttime'] = result['reporttime'].replace('NaT','')
    result['spantime'] = result['spantime'].replace('NaT','')

    # Define the PropHours logic using a function
    def calculate_prop_hours(row):
        if pd.isna(row['spantime']):
            return ''
        elif (
            (row['spantype'] == 3 and row['LastSpanType'] == 3) or 
            (row['LastSpanType'] == 3 and not row['spantype'] == 3)
        ):
            return '24:00:00' if row['spantime'].time() == pd.Timestamp('00:00:00').time() else str(row['spantime'].time())
        else:
            return None

    # Apply the PropHours logic to create a new column
    result['PropHours'] = result.apply(calculate_prop_hours, axis=1)

    # Drop the 'LastSpanType' column if not needed in the final result
    #result.drop('LastSpanType', axis=1, inplace=True)

    return result

def CO_Process_Second(result):
    
    def extract_hours(value):
        try:
            # Extract hours with minutes as decimal
            timedelta_value = pd.to_timedelta(value)
            if not pd.isna(timedelta_value):
                hours_with_minutes = timedelta_value.total_seconds() / 3600
                return hours_with_minutes
            else:
                return pd.NaT
        except (ValueError, TypeError):
            # Handle NaN, NaT, or invalid values
            return pd.NaT

    def replace_NaT_with_0(value):
        return 0 if pd.isna(value) else value

    def add_cumulative_prop_hours(result):
        # Sort the DataFrame by 'imono' and 'reporttime'
        result = result.sort_values(by=['imono', 'reporttime']).reset_index(drop=True)

        # Extract hours from 'PropHours' and check data types
        result['NewPropHours'] = result['PropHours'].apply(extract_hours)

        # Replace 'NaT' with 0 in 'PropHours'
        result['NewPropHours'] = result['NewPropHours'].apply(replace_NaT_with_0)

        # Calculate cumulative propelling hours
        result['CumuPropHours'] = (
            result
            .groupby('imono')['NewPropHours']
            .cumsum()
        )

        # Replace 'NaT' with 0 in cumulative sum
        result['CumuPropHours'] = result['CumuPropHours'].replace(pd.NaT, 0).astype(float)

        return result

    # Assuming your data is stored in a variable named 'result'
    final_data = add_cumulative_prop_hours(result)

    return final_data