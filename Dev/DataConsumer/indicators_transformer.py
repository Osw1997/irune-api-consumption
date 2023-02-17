from fileinput import filename
import pandas as pd

# Let's convert pivot table into a reguilar table for all the interesting indicators

indicators = {
    "vulnerability": ["ecosystems.csv", "habitat.csv", "infrastructure.csv"],
    "readiness": ["economic.csv", "governance.csv", "social.csv"]
}

for indicator in indicators:
    for file in indicators[indicator]:
        df = pd.read_csv(f"../../data/resources/{indicator}/{file}")
        # Make the magic!
        melted_df = df.melt(
            id_vars=["ISO3", "Name"],
            var_name="Year",
            value_name="value"
        )
        # Create a flag that indicates if value is real or mean
        melted_df["no_value"] = melted_df["value"].isna()
        # Get mean by year and replace nan values by mean
        mean_df = melted_df.groupby(["Year"])[["value"]].mean().reset_index()
        mean_df.rename(columns={"value": "mean"}, inplace=True)
        final_df = pd.merge(melted_df, mean_df, how="left", on="Year")
        final_df.loc[final_df["value"].isna(), "value"] = final_df["mean"]
        del final_df["mean"]
        # Save result to CSV
        melted_filename = f"melted_{file}"
        final_df.to_csv(f"../../data/resources/{indicator}/{melted_filename}", index=False)

