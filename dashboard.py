# streamlit_app.py
from Prediction import pop_predict
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from PIL import Image

# Page config
st.set_page_config(page_icon=":musical_note:", page_title="SpotifyPlus", layout="wide")



# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
# @st.cache_data(ttl=600)
@st.experimental_memo
#@st.cache_data # it shows warning using experimental_memo

def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows


hide_default_format = """
       <style>
       #MainMenu {visibility: hidden; }
       footer {visibility: hidden;}
       </style>
       """
st.markdown(hide_default_format, unsafe_allow_html=True)


# DATA
track_rows = run_query('SELECT * FROM `snappy-boulder-378707.TrackClearInfo.TrackClearInfo` ORDER BY popularity desc')
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

track_genre_rows = run_query('SELECT * FROM `snappy-boulder-378707.TrackGenre.Trackgenre`')
track_genre_df = pd.DataFrame(track_genre_rows)
track_feature_genre_df = pd.merge(track_feature_df, track_genre_df, left_on="id", right_on = "track_id",how="inner")
track_feature_genre_df = track_feature_genre_df.sort_values(by=['popularity'],ascending=False)

spotify_logo = Image.open("D:/y3s2/IS3107_letsgo/spotify_logo.png")

color_palette = sns.color_palette("Paired").pop(2)


# page config

st.title("SpotifyPlus")

st.sidebar.image(spotify_logo, width = 150)

# pre-defined functions

def plot_config(fig, ax):
    fig.patch.set_alpha(0)
    ax.patch.set_alpha(0)
    ax.spines['bottom'].set_color('white')
    ax.spines['top'].set_color('white')
    ax.spines['left'].set_color('white')
    ax.spines['right'].set_color('white')
    ax.xaxis.label.set_color('white')
    ax.yaxis.label.set_color('white')
    ax.title.set_color('white')
    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')
    return fig, ax





# main content

page = st.sidebar.radio(
    "Select your interested page",
    ('Genre & Popularity Coverage', 'Newly Released Prediction', 'User Prediction'))


# tab - visualisation
if page == 'Genre & Popularity Coverage':
    
    c1 = st.container()
    
    with c1:
        col1, col2 = st.columns([6,3.5])
        with col1:
            st.header("Genre Distribution")
            fig, ax = plt.subplots(figsize=(10,4))
            sns.barplot(y=genre_df['genre'], x=genre_df['total_tracks'], ax=ax, palette="Set2", errorbar=None)
            fig, ax = plot_config(fig, ax)
            st.pyplot(fig)
        with col2:
            st.header("Popularity of Genres")
            fig, ax = plt.subplots(figsize=(30,2))
            # pop_sum = sum(genre_df['mean_popularity'])
            # genre_df['pop_percent'] = genre_df['mean_popularity'] / pop_sum
            # genre_df['label'] = genre_df.apply((lambda x: x['genre'] + ' ' + str(round(x['pop_percent']*100, 2)) + '%'), axis=1)
            patches, l_text, p_text= plt.pie(genre_df['mean_popularity'], labels = genre_df['genre'], labeldistance=1.05, autopct='%1.1f%%')
            for t in l_text:
                t.set_size(3.5)
            for p in p_text:
                p.set_size(3)
            fig, ax = plot_config(fig, ax)
            plt.tight_layout()
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
# Singers in terms of popularity/followers/no. of trending songs in general/genre -3
    c3 = st.container()
    
    with c3:
        col1, col2 = st.columns([1,1])
        with col1:
            st.header("Top Tracks Teatures")
            feature_option = st.selectbox('Choose a feature to plot', ('Popularity','Danceability', 'Energy', 'Loudness', 'Speechiness', 'Acousticness', 'Instrumentalness', 'Liveness', 'Valence','Tempo', 'Duration_ms','Available_Markets'))
            fig, ax = plt.subplots(figsize=(15,7))
            sns.barplot(y=track_feature_df[feature_option.lower()][:5], x=track_feature_df['name'][:5], width = 0.6, palette="Set2")
            ax.set_xlabel('Track',fontsize = 14)
            ax.set_ylabel(feature_option,fontsize = 14)
            fig, ax = plot_config(fig, ax)
            st.pyplot(fig)

        with col2:
            st.header("Top Singers Info")
            attribute_option = st.selectbox('Choose an attribute to plot', ('Popularity','Followers'))
            fig, ax = plt.subplots(figsize=(15,7))
            sns.barplot(y=artist_df[attribute_option.lower()][:5], x=artist_df['name'][:5], width= 0.6, palette="Set2")
            ax.set_xlabel('Singer',fontsize = 14)
            ax.set_ylabel(attribute_option,fontsize = 14)
            fig, ax = plot_config(fig, ax)
            st.pyplot(fig)

# Top Tracks Features in Genre
    c4 = st.container()
    with c4:
        st.header("Top Tracks Features in Genre")
        col1, col2, col3 = st.columns([4,1,6])
        with col1:
            genre_option = st.selectbox('Choose a genre', (genre_df['genre']))
            genre_feature_option = st.selectbox('Choose a feature', ('Popularity','Danceability', 'Energy', 'Loudness', 'Speechiness', 'Acousticness', 'Instrumentalness', 'Liveness', 'Valence','Tempo', 'Duration_ms','Available_Markets'))
        
        with col3:
            fig, ax = plt.subplots(figsize=(18,7))
            filter_by_genre = track_feature_genre_df[track_feature_genre_df[genre_option] == 1]
            sns.barplot(y=filter_by_genre[genre_feature_option.lower()][:5], x=filter_by_genre['name'][:5].apply(lambda x: x[:40]), width = 0.6,palette="Set2")
            ax.set_title(genre_feature_option + ' of Top 5 Tracks in Genre ' + genre_option, fontsize = 20)
            ax.set_xlabel('Track',fontsize = 14)
            ax.set_ylabel(genre_feature_option,fontsize = 14)
            fig, ax = plot_config(fig, ax)
            st.pyplot(fig)




# tab - prediction
if page == 'Newly Released Prediction':
    c1 = st.container()
    c2 = st.container()
    with c1:
        st.title('Track Popularity Predictor')
        col1, col, col2 = st.columns([1,2,1])
        with col:
            st.image("""https://pyxis.nymag.com/v1/imgs/3a3/b1f/2141226b8ab1ae07afe4b541ee0d2b0825-11-yic-pop-essay.rsocial.w1200.jpg""")
        col1, col, col2 = st.columns([7,1,7])
        with col1:
            released_date = st.date_input('Released Date:',datetime.datetime.now())
            danceability = st.number_input('Danceability:', min_value=0.0, max_value=1.0)
            energy = st.number_input('Energy:', min_value=0.0, max_value=1.0)
            key = st.selectbox('Key:', range(0,12))
            loudness = st.number_input('Loudness:', min_value=-60.0, max_value=1.0)
            mode = st.selectbox('Mode:', [0,1])
            speechiness = st.number_input('Speechiness:', min_value=0.0, max_value=1.0)
            acousticness = 	st.number_input('Acousticness:', min_value=0.0, max_value=1.0)
            instrumentalness = 	st.number_input('Instrumentalness:', min_value=0.0, max_value=1.0)
        with col2:
            liveness = st.number_input('Liveness:', min_value=0.0, max_value=1.0)
            valence = st.number_input('Valence:', min_value=0.0, max_value=1.0)
            tempo = st.number_input('Tempo:', min_value=0.0, max_value=500.0)
            duration_ms = st.number_input('Duration (ms):', min_value=0.0, max_value=1.0 * 10**10)
            time_signature = st.selectbox('Time signature:', range(3,8))
            explicit = 	st.selectbox('Explicit:', range(0,2))
            available_markets = st.number_input('Available markets:', min_value=0, max_value=500)
            followers = st.number_input('Followers:', min_value=0, max_value=1 * 10**10)
            popularity_artist = st.number_input('Artist popularity:', min_value=0.0, max_value=100.0)

        vars = [released_date,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,duration_ms,time_signature,explicit,available_markets,followers,popularity_artist]
        if st.button('Predict Popularity'):
            popularity = pop_predict(vars)
            st.success(f'The predicted popularity of the track is {popularity[0]:.2f}')
            col1, col, col2 = st.columns([2,5,2])
            with col:
                fig, ax = plt.subplots(figsize=(15,8))
                sns.kdeplot(data=track_feature_df, x="popularity",fill = True,alpha=0.5, color = '#4CC9F0')
                plt.axvline(popularity[0]*100, color = 'orange',linewidth = 6)
                fig, ax = plot_config(fig, ax)
                st.pyplot(fig)
                


if page == 'User Prediction':
        st.write("hi")