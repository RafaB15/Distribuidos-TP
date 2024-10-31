# Correr en la terminal pip install langid
import numpy as np
import pandas as pd
import langid
import time

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 100)

# Load datasets
reviews_df_cleaned = pd.read_csv('../client_data/steam_reviews_500k.csv')
games_df_cleaned = pd.read_csv('../client_data/games_90k.csv')

"""# Queries resolution

## Q1: Cantidad de juegos soportados en cada plataforma (Windows, Linux, MAC)
"""

print("\nQuery 1:")

windows_supported_games = games_df_cleaned[games_df_cleaned["Windows"] == True]
linux_supported_games = games_df_cleaned[games_df_cleaned["Linux"] == True]
mac_supported_games = games_df_cleaned[games_df_cleaned["Mac"] == True]

print("Linux: " + str(linux_supported_games.shape[0]))
print("Windows: " + str(windows_supported_games.shape[0]))
print("Mac: " + str(mac_supported_games.shape[0]))

"""## Q2: Nombre del top 10 de juegos del género "Indie" publicados en la década del 2010 con más tiempo promedio histórico de juego"""

print("\nQuery 2:")

games_indie = games_df_cleaned[games_df_cleaned["Genres"].str.contains("indie")]
games_indie_2010_decade = games_indie[games_indie["Release date"].str.contains("201")]

q2_result = games_indie_2010_decade.sort_values(by='Average playtime forever', ascending=False).head(10)

for index, row in q2_result.iterrows():
    app_id = row['AppID']
    release_year = row['Release date'][-4:]  # Extract the year from the release date
    avg_playtime_forever = row['Average playtime forever']
    print(f"AppId: {app_id} ReleaseYear: {release_year} AvgPlaytimeForever: {avg_playtime_forever}")

"""## Q3: Nombre de top 5 juegos del género "Indie" con más reseñas positivas"""

print("\nQuery 3:")

games_indie_reduced = games_indie[["AppID", "Name"]]
reviews_reduced_q3 = reviews_df_cleaned[["app_id", "review_score"]]
games_indie_reviews = pd.merge(games_indie_reduced, reviews_reduced_q3, left_on='AppID', right_on='app_id', how='inner')

def positive_score(score):
    return 1 if score > 0 else 0

games_indie_reviews['positive_score'] = games_indie_reviews['review_score'].apply(positive_score)
q3_result = games_indie_reviews.groupby(['AppID', 'Name'])['positive_score'].sum().sort_values(ascending=False).head(5)

for app_id, name, positive_reviews in q3_result.reset_index().values:
    print(f"AppID: {app_id}, GameName: {name}, PositiveReviews: {positive_reviews}")

"""## Q4: Nombre de juegos del género "action" con más de 5.000 reseñas negativas en idioma inglés"""

print("\nQuery 4:")

# Nos quedanmos con los juegos de acción
games_action = games_df_cleaned[games_df_cleaned["Genres"].str.contains("action", case=False, na=False)]

# Nos quedamos con las que tengan reviews negativs
negative_reviews = reviews_df_cleaned[reviews_df_cleaned["review_score"] == -1]

# CPU INTENSIVE #############################
def detect_language(texto):
    language, _ = langid.classify(texto)
    return language
#############################################

# Filtramos por lenguaje
negative_reviews = negative_reviews.copy()  # Create a copy to avoid SettingWithCopyWarning
negative_reviews["language"] = negative_reviews["review_text"].apply(detect_language)
negative_reviews_english = negative_reviews[negative_reviews["language"] == "en"]

# Mergeamos las reviews con los juegos filtrados
games_action_reviews = pd.merge(games_action, negative_reviews_english, left_on='AppID', right_on='app_id', how='inner')

# Hacemos group by para contar
negative_reviews_count = games_action_reviews.groupby(['AppID', 'Name']).size().reset_index(name='negative_review_count')

# Nos quedamos con los que tienen más de 5000 reviews
games_with_5000_negative_reviews = negative_reviews_count[negative_reviews_count['negative_review_count'] > 1000]


# Printeamos resultados
for index, row in games_with_5000_negative_reviews.iterrows():
    app_id = row['AppID']
    game_name = row['Name']
    negative_review_count = row['negative_review_count']
    print(f"AppID: {app_id}, GameName: {game_name}, NegativeReviews: {negative_review_count}")



"""
# Juegos de acción
games_action = games_df_cleaned[games_df_cleaned["Genres"].str.contains("action")]
games_action_reduced = games_action[["AppID", "Name"]]

reviews_q4 = reviews_df_cleaned.copy()

# Reviews con mas de 5000 comentarios negativos


reviews_q4["negative_score"] = reviews_q4["review_score"].apply(negative_score)
reviews_q4_negatives = reviews_q4[reviews_q4["negative_score"] == 1].copy()
reviews_count = reviews_q4_negatives.groupby('app_id').size().reset_index(name='count')
reviews_count_more_than_5000 = reviews_count[reviews_count["count"] > 5000]

# De las reviews con mas de 5000 comentarios negativos, nos quedamos con aquellas que sean sobre juegos de acción
games_action_with_5000_negative_reviews = pd.merge(games_action_reduced, reviews_count_more_than_5000, left_on='AppID', right_on="app_id", how='inner')
games_action_with_5000_negative_reviews = games_action_with_5000_negative_reviews[["AppID", "Name"]]

# Enriquecemos con el texto de la review
reviews_count_more_than_5000_with_text = pd.merge(reviews_q4, games_action_with_5000_negative_reviews, left_on='app_id', right_on="AppID", how='inner')
reviews_count_more_than_5000_with_text = reviews_count_more_than_5000_with_text[["app_id", "review_text"]]

# CPU INTENSIVE #############################
def detect_language(texto):
    language, _ = langid.classify(texto)
    return language
#############################################

# Calculo del idioma sobre las reviews
start_time = time.time()
reviews_count_more_than_5000_with_text["review_language"] = reviews_count_more_than_5000_with_text['review_text'].apply(detect_language)
elapsed_time = time.time() - start_time
print(f"Execution time on {reviews_count_more_than_5000_with_text.shape[0]} rows: {elapsed_time:.2f} seconds")

# Nos quedamos con aquellas reviews que estan en idioma inglés
reviews_count_more_than_5000_with_text_english = reviews_count_more_than_5000_with_text[reviews_count_more_than_5000_with_text["review_language"] == "en"]

# Nos quedamos con aquellos juegos que tengan mas de 5000 reseñas negativas en inglés
q4_results_app_ids = reviews_count_more_than_5000_with_text_english.groupby('app_id').size().reset_index(name='count')
q4_results_app_ids = q4_results_app_ids[q4_results_app_ids["count"] > 5000]

# Enriquecemos con el nombre de esos juegos
q4_results_games_names = pd.merge(q4_results_app_ids, games_action_with_5000_negative_reviews, left_on='app_id', right_on="AppID", how='inner')["Name"]
print(q4_results_games_names)
"""

"""## Q5: Nombre de juegos del género "action" dentro del percentil 90 en cantidad de reseñas negativas"""

print("\nQuery 5:")

def negative_score(score):
    return 1 if score < 0 else 0

# Calculate negative scores for all reviews
reviews_q5 = reviews_df_cleaned.copy()
reviews_q5 = reviews_q5[["app_id", "review_score"]]
reviews_q5["negative_score"] = reviews_q5["review_score"].apply(negative_score)
reviews_q5_negative_score = reviews_q5[reviews_q5["negative_score"] == 1]

# Group by app_id to get the count of negative reviews for each game
reviews_q5_negative_score_by_app_id = reviews_q5_negative_score.groupby('app_id').size().reset_index(name='count')

# Calculate the 90th percentile over all reviews
percentil_90 = reviews_q5_negative_score_by_app_id['count'].quantile(0.90)

# Filter games that are in the 90th percentile or higher
q5_result = reviews_q5_negative_score_by_app_id[reviews_q5_negative_score_by_app_id['count'] >= percentil_90]

# Filter the games to only include those in the "action" genre
games_action = games_df_cleaned[games_df_cleaned["Genres"].str.contains("action")]
games_action_reduced = games_action[["AppID", "Name"]]

q5_result_with_game_names = pd.merge(q5_result, games_action_reduced, left_on='app_id', right_on="AppID", how='inner')

for index, row in q5_result_with_game_names.iterrows():
    app_id = row['app_id']
    game_name = row['Name']
    negative_reviews = row['count']
    print(f"AppID: {app_id}, GameName: {game_name}, NegativeReviews: {negative_reviews}")