# Correr en la terminal pip install langid
import pandas as pd

def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', 100)

    # Load datasets
    reviews_df = pd.read_csv('steam_reviews_reduced.csv')
    games_df = pd.read_csv('games_reduced.csv', header=None, skiprows=1)

    # Add column names to games_df
    games_df_column_names = ['AppID', 'Name', 'Release date', 'Estimated owners', 'Peak CCU',
                             'Required age', 'Price', 'Unknown', 'DiscountDLC count', 'About the game',
                             'Supported languages', 'Full audio languages', 'Reviews', 'Header image',
                             'Website', 'Support url', 'Support email', 'Windows', 'Mac',
                             'Linux', 'Metacritic score', 'Metacritic url', 'User score',
                             'Positive', 'Negative', 'Score rank', 'Achievements',
                             'Recommendations', 'Notes', 'Average playtime forever',
                             'Average playtime two weeks', 'Median playtime forever',
                             'Median playtime two weeks', 'Developers', 'Publishers',
                             'Categories', 'Genres', 'Tags', 'Screenshots', 'Movies']
    games_df.columns = games_df_column_names

    # Dataframes cleaning
    games_df_columns = ['AppID', 'Name', 'Windows', 'Mac', 'Linux', 'Genres', 'Release date', 'Average playtime forever', 'Positive', 'Negative']
    games_df_cleaned = games_df.dropna(subset=games_df_columns)[games_df_columns].copy()
    games_df_cleaned["Genres"] = games_df_cleaned["Genres"].str.lower()

    reviews_df_columns = ['app_id', 'review_text', 'review_score']
    reviews_df_cleaned = reviews_df.dropna(subset=reviews_df_columns)[reviews_df_columns].copy()
    reviews_df_cleaned['review_text'] = reviews_df_cleaned['review_text'].astype(str)

    # Save cleaned dataframes
    games_df_cleaned.to_csv('games_reduced_cleaned.csv', index=False)
    reviews_df_cleaned.to_csv("steam_reviews_reduced_cleaned.csv", index=False)

if __name__ == "__main__":
    main()