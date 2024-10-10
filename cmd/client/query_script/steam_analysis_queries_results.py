# Correr en la terminal pip install langid
import numpy as np
import pandas as pd
import langid
import time

def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', 100)

    # Load datasets
    reviews_df_cleaned = pd.read_csv('steam_reviews_reduced_cleaned.csv')
    games_df_cleaned = pd.read_csv('games_reduced_cleaned.csv')

    # Queries resolution

    # Q1: Cantidad de juegos soportados en cada plataforma (Windows, Linux, MAC)
    windows_supported_games = games_df_cleaned[games_df_cleaned["Windows"] == True]
    linux_supported_games = games_df_cleaned[games_df_cleaned["Linux"] == True]
    mac_supported_games = games_df_cleaned[games_df_cleaned["Mac"] == True]

    print("Total de juegos: " + str(games_df_cleaned.shape[0]))
    print("Total de juegos soportados en Windows: " + str(windows_supported_games.shape[0]))
    print("Total de juegos soportados en Linux: " + str(linux_supported_games.shape[0]))
    print("Total de juegos soportados en Mac: " + str(mac_supported_games.shape[0]))

    # Q2: Nombre del top 10 de juegos del género "Indie" publicados en la década del 2010 con más tiempo promedio histórico de juego
    games_indie = games_df_cleaned[games_df_cleaned["Genres"].str.contains("indie")]
    games_indie_2010_decade = games_indie[games_indie["Release date"].str.contains("201")]
    q2_result = games_indie_2010_decade.sort_values(by='Average playtime forever', ascending=False).head(10)
    print(q2_result[['Name', 'Average playtime forever']])

    # Q3: Nombre de top 5 juegos del género "Indie" con más reseñas positivas
    games_indie_reduced = games_indie[["AppID", "Name"]]
    reviews_reduced_q3 = reviews_df_cleaned[["app_id", "review_score"]]
    games_indie_reviews = pd.merge(games_indie_reduced, reviews_reduced_q3, left_on='AppID', right_on='app_id', how='inner')

    def positive_score(score):
        return 1 if score > 0 else 0

    games_indie_reviews['positive_score'] = games_indie_reviews['review_score'].apply(positive_score)
    q3_result = games_indie_reviews.groupby('Name')['positive_score'].sum().sort_values(ascending=False).head(5)
    print(q3_result.head(10))

    # Q4: Nombre de juegos del género "action" con más de 5.000 reseñas negativas en idioma inglés
    games_action = games_df_cleaned[games_df_cleaned["Genres"].str.contains("action")]
    games_action_reduced = games_action[["AppID", "Name"]]

    def negative_score(score):
        return 1 if score < 0 else 0

    reviews_q4 = reviews_df_cleaned.copy()
    reviews_q4["negative_score"] = reviews_q4["review_score"].apply(negative_score)
    reviews_q4_negatives = reviews_q4[reviews_q4["negative_score"] == 1].copy()
    reviews_count = reviews_q4_negatives.groupby('app_id').size().reset_index(name='count')
    reviews_count_more_than_5000 = reviews_count[reviews_count["count"] > 5000]

    games_action_with_5000_negative_reviews = pd.merge(games_action_reduced, reviews_count_more_than_5000, left_on='AppID', right_on="app_id", how='inner')
    reviews_count_more_than_5000_with_text = pd.merge(reviews_q4, games_action_with_5000_negative_reviews, left_on='app_id', right_on="AppID", how='inner')
    reviews_count_more_than_5000_with_text = reviews_count_more_than_5000_with_text[["app_id", "review_text"]]

    def detect_language(texto):
        language, _ = langid.classify(texto)
        return language

    start_time = time.time()
    reviews_count_more_than_5000_with_text["review_language"] = reviews_count_more_than_5000_with_text['review_text'].apply(detect_language)
    elapsed_time = time.time() - start_time
    print(f"Execution time on {reviews_count_more_than_5000_with_text.shape[0]} rows: {elapsed_time:.2f} seconds")

    reviews_count_more_than_5000_with_text_english = reviews_count_more_than_5000_with_text[reviews_count_more_than_5000_with_text["review_language"] == "en"]
    q4_results_app_ids = reviews_count_more_than_5000_with_text_english.groupby('app_id').size().reset_index(name='count')
    q4_results_app_ids = q4_results_app_ids[q4_results_app_ids["count"] > 5000]
    q4_results_games_names = pd.merge(q4_results_app_ids, games_action_with_5000_negative_reviews, left_on='app_id', right_on="AppID", how='inner')["Name"]
    print(q4_results_games_names.head(25))

    # Q5: Nombre de juegos del género "action" dentro del percentil 90 en cantidad de reseñas negativas
    reviews_q5 = reviews_df_cleaned.copy()
    reviews_q5["negative_score"] = reviews_q5["review_score"].apply(negative_score)
    reviews_q5_negative_score = reviews_q5[reviews_q5["negative_score"] == 1]
    reviews_q5_negative_score_action = pd.merge(reviews_q5_negative_score, games_action_reduced, left_on='app_id', right_on="AppID", how='inner')
    reviews_q5_negative_score_action_by_app_id = reviews_q5_negative_score_action.groupby('app_id').size().reset_index(name='count')

    percentil_90 = reviews_q5_negative_score_action_by_app_id['count'].quantile(0.90)
    q5_result = reviews_q5_negative_score_action_by_app_id[reviews_q5_negative_score_action_by_app_id['count'] >= percentil_90]
    q5_result_with_game_names = pd.merge(q5_result, games_action_reduced, left_on='app_id', right_on="AppID", how='inner')
    print(q5_result_with_game_names[["app_id", "Name"]].head(10))

if __name__ == "__main__":
    main()