import os
import numpy as np
import pandas as pd
from pathlib import Path

num_samples = 1000000
num_shards = 50

if __name__ == "__main__":
    # Load the CSV file into a DataFrame
    df = pd.read_csv(os.path.join(Path(__file__).parent.parent, "data", "input", "weather_features.csv"))

    X = df.drop(['dt_iso'], axis=1)  # input variables

    # Generate synthetic data for the numerical columns
    numeric_cols = ['temp', 'temp_min', 'temp_max', 'pressure', 'humidity', 'wind_speed', 'wind_deg', 'rain_1h', 'rain_3h', 'snow_3h', 'clouds_all']
    synthetic_numeric_data = {}
    for col in numeric_cols:
        synthetic_numeric_data[col] = np.random.normal(loc=X[col].mean(), scale=X[col].std(), size=len(X) + num_samples)

    # Generate synthetic data for the categorical columns
    categorical_cols = ['city_name', 'weather_main', 'weather_description', 'weather_icon']
    synthetic_categorical_data = {}
    for col in categorical_cols:
        unique_vals = X[col].unique()
        synthetic_categorical_data[col] = np.random.choice(unique_vals, size=len(df) + num_samples)

    new_data = pd.DataFrame(synthetic_numeric_data)
    new_data[categorical_cols] = pd.DataFrame(synthetic_categorical_data)

    # # Add a new timestamp column to the new_data DataFrame
    end_date = pd.to_datetime(df['dt_iso'].max())

    new_times = pd.date_range(start="1980-01-01 00:00:00+01:00",
                              periods=len(df) + num_samples,
                              freq='5T')

    new_data['dt_iso'] = new_times

    new_data = new_data.sort_values(by='dt_iso', ascending=True)

    # Make the time column the first column in the dataframe
    new_data = new_data.reindex(columns=['dt_iso'] + [col for col in new_data.columns if col != "dt_iso"])

    # new_data = new_data.iloc[1:]

    # final_df = pd.concat([df, new_data])

    # Save the generated data to a new CSV file

    # Split dataframe into shards
    shards = np.array_split(new_data, num_shards)

    for i, shard in enumerate(shards):
        shard_path = os.path.join(Path(__file__).parent.parent, "data", "output", "weather_features", f"part-{i}")
        os.makedirs(shard_path, exist_ok=True)
        shard.to_csv(os.path.join(shard_path, f'part-{i}.csv'), index=False)
