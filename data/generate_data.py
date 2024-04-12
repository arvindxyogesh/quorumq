"""
Synthetic energy consumption dataset.
Hourly electricity demand with daily, weekly, and annual seasonality,
plus temperature correlation, special events, and realistic noise.
"""
import numpy as np
import pandas as pd
from pathlib import Path


def generate_energy_consumption(
    n_days: int = 730,
    seed: int = 42,
    output_path: str = "data/energy_consumption.csv",
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n = n_days * 24
    idx = pd.date_range("2022-01-01", periods=n, freq="h")
    t = np.arange(n)

    # Yearly trend (energy use grows ~2% annually)
    trend = 300 + 0.00004 * t

    # Yearly seasonal (higher in winter, lower in summer — northern hemisphere)
    yearly = -60 * np.cos(2 * np.pi * t / (365.25 * 24))

    # Weekly seasonal (lower on weekends)
    weekday = idx.dayofweek.values
    weekly_map = np.array([5, 5, 5, 5, 0, -15, -15])     # Mon–Sun
    weekly = weekly_map[weekday]

    # Daily seasonal (peak morning and evening)
    hour = idx.hour.values
    daily = (
        30 * np.sin(2 * np.pi * (hour - 7) / 24)
        + 20 * np.sin(4 * np.pi * (hour - 4) / 24)
    )

    # Simulated temperature correlation
    temp = 15 - 20 * np.cos(2 * np.pi * t / (365.25 * 24)) + rng.normal(0, 3, n)
    heating_cooling = 1.5 * np.abs(temp - 18)   # HVAC demand spikes away from 18°C

    # Noise
    noise = rng.normal(0, 8, n)

    consumption = (trend + yearly + weekly + daily + heating_cooling + noise).clip(min=50).round(1)

    df = pd.DataFrame({
        "timestamp": idx,
        "consumption": consumption,
        "temperature": temp.round(1),
        "hour": hour,
        "dayofweek": weekday,
    }).set_index("timestamp")

    # Resample to daily for classical/DL forecasting baseline
    daily_df = df[["consumption"]].resample("D").mean().round(2)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    daily_df.to_csv(output_path)
    print(f"Generated {len(daily_df)} daily samples → {output_path}")
    return daily_df


if __name__ == "__main__":
    df = generate_energy_consumption()
    print(df.describe())
