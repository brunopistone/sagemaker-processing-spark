import numpy as np
import os
import pandas as pd
from pathlib import Path
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression

num_samples = 1000000
num_shards = 50

print(Path(__file__).parent.parent)

if __name__ == "__main__":
    # Load the dataset from the CSV file
    data = pd.read_csv(os.path.join(Path(__file__).parent.parent, "data", "input", "energy_dataset.csv"))

    # Extract the input variables and output variable
    X = data.drop(['time', 'price actual'], axis=1) # input variables
    y = data['price actual'] # output variable

    imp = SimpleImputer(missing_values=np.nan, strategy='mean')
    X_imputed = imp.fit_transform(X)

    # Train a linear regression model on the original data
    model = LinearRegression()
    model.fit(X_imputed, y)

    # Generate new input data for which to predict prices
    new_data = pd.DataFrame(np.random.rand(num_samples, len(X.columns)), columns=X.columns)

    # # Add a new timestamp column to the new_data DataFrame
    start_date = pd.to_datetime("1980-01-01 00:00:00+01:00")
    end_date = pd.to_datetime(data['time'].max())

    new_times = pd.date_range(start=start_date,
                              periods=num_samples,
                              freq='5T')

    new_data['time'] = new_times

    new_data_imputed = imp.transform(new_data.drop('time', axis=1))

    # Predict prices for the new input data
    new_prices = model.predict(new_data_imputed)

    # Add the new prices to the new_data DataFrame
    new_data['price actual'] = new_prices

    new_data = new_data.sort_values(by='time', ascending=True)

    # Make the time column the first column in the dataframe
    new_data = new_data.reindex(columns=['time'] + [col for col in new_data.columns if col != 'time'])

    # new_data = new_data.iloc[1:]

    # final_df = pd.concat([data, new_data])

    # Split dataframe into shards
    shards = np.array_split(new_data, num_shards)

    for i, shard in enumerate(shards):
        print(f"\rSaving shard {i+1}/{num_shards}...", end='', flush=True)
        shard_path = os.path.join(Path(__file__).parent.parent, "data", "output", "energy_dataset", f"part-{i}")
        os.makedirs(shard_path, exist_ok=True)
        shard.to_csv(os.path.join(shard_path, f'part-{i}.csv'), index=False)
    print("\nDone!")
