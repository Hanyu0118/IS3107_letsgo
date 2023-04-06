# streamlit_app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import datetime

# Page config
st.set_page_config(layout="wide")



# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
# @st.cache_data(ttl=600)
# @st.experimental_memo
@st.cache_data # it shows warning using experimental_memo

def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows


# DATA
track_rows = run_query('SELECT * FROM `snappy-boulder-378707.TrackClearInfo.TrackClearInfo` ORDER BY popularity desc LIMIT 10')
track_df = pd.DataFrame(track_rows)

genre_rows = run_query('SELECT * FROM `snappy-boulder-378707.GenrePopularity.GenrePopularity`')
genre_df = pd.DataFrame(genre_rows)
genre_df['mean_popularity'] = genre_df['popularity'] / genre_df['total_tracks']
genres = genre_df['genre'].unique()

feature_rows = run_query('SELECT * FROM `snappy-boulder-378707.AudioFeatures.AudioFeatures`')
feature_df = pd.DataFrame(feature_rows)
track_feature_df = pd.merge(feature_df, track_df, on="id", how="inner")
track_feature_df = track_feature_df.sort_values(by=['popularity'],ascending=False)

artist_rows = run_query('SELECT * FROM `snappy-boulder-378707.TrackClearInfo.ArtistInfo` ORDER BY popularity desc LIMIT 5')
artist_df = pd.DataFrame(artist_rows)

st.title("SpotifyPlus")

# tabs
tab1, tab2 = st.tabs(["Genre & Popularity Coverage", "Prediction"])


# tab - visualisation
with tab1:
    
    c1 = st.container()
    
    with c1:
        col1, col2 = st.columns([7,4])
        with col1:
            st.header("Genre Distribution")
            fig, ax = plt.subplots(figsize=(10,4))
            sns.barplot(y=genre_df['genre'], x=genre_df['total_tracks'], ax=ax)
            st.pyplot(fig)
        with col2:
            st.header("Popularity of Genres")
            fig, ax = plt.subplots(figsize=(25,2))
            pop_sum = sum(genre_df['mean_popularity'])
            genre_df['pop_percent'] = genre_df['mean_popularity'] / pop_sum
            genre_df['label'] = genre_df.apply((lambda x: x['genre'] + ' ' + str(round(x['pop_percent']*100, 2)) + '%'), axis=1)
            patches, l_text = plt.pie(genre_df['mean_popularity'], labels = genre_df['label'], labeldistance=1.1)
            for t in l_text:
                t.set_size(3.5)
            st.pyplot(plt)
    
    c2 = st.container()
    
    with c2:
        st.header("Popularity Distribution")
        col1, col2, col3, col4 = st.columns([1,4,1,4])
        with col1:
            st.subheader("Filter:")
        with col2:
            genre_chosen = st.multiselect("Choose genre types",genres)
        with col3:
            time_chosen = st.date_input("Release time", datetime.datetime.now())
        with col4:
            pop_threshold = st.slider("maximum poplarity", 0, 100, 100)
            
            
    

# Tracks in terms of popularity (/danceability/energy/singer/team etc) in a genre - scatter plot - 2
    c3 = st.container()
    
    with c3:
        col1, col2 = st.columns([1,1])
        with col1:
            st.header("Top Tracks Teatures")
            feature_option = st.selectbox('Choose a feature to plot', ('Popularity','Danceability', 'Energy', 'Loudness', 'Speechiness', 'Acousticness', 'Instrumentalness', 'Liveness', 'Valence','Tempo', 'Duration_ms','Available_Markets'))
            fig, ax = plt.subplots(figsize=(15,7))
            sns.barplot(y=track_feature_df[feature_option.lower()][:5], x=track_feature_df['name'][:5], width = 0.6)
            ax.set_xlabel('Track',fontsize = 14)
            ax.set_ylabel(feature_option,fontsize = 14)
            st.pyplot(fig)

        with col2:
            st.header("Top Singers Info")
            attribute_option = st.selectbox('Choose an attribute to plot', ('Popularity','Followers'))
            fig, ax = plt.subplots(figsize=(15,7))
            sns.barplot(y=artist_df[attribute_option.lower()][:5], x=artist_df['name'][:5], width= 0.6)
            ax.set_xlabel('Singer',fontsize = 14)
            ax.set_ylabel(attribute_option,fontsize = 14)
            st.pyplot(fig)

# Singers in terms of popularity/followers/no. of trending songs in general/genre -3