import pandas as pd
 
def add_headers_to_temperature(file_path):
    df = pd.read_csv(file_path, header=None)
    df.columns = [
        "city", "year", "number_month", "month",
        "avg_max_temp", "avg_min_temp", "avg_mean_temp",
        "max_temp", "min_temp", "days_recorded"
    ]
    df.to_csv(file_path, index=False, header=True)
    print(f"Headers added to {file_path}")
 
def add_headers_to_precipitation(file_path):
    df = pd.read_csv(file_path, header=None)
    df.columns = [
        "city", "year", "season",
        "total_precipitation", "avg_monthly_precipitation",
        "max_monthly_precipitation", "total_rainy_days",
        "months_in_season"
    ]
    df.to_csv(file_path, index=False, header=True)
    print(f"Headers added to {file_path}")
 
if __name__ == "__main__":
    temp_path = "data/processed/temperature_results.csv"
    precip_path = "data/processed/precipitation_results.csv"
    add_headers_to_temperature(temp_path)
    add_headers_to_precipitation(precip_path)