import streamlit as st
import datetime
from src.utils import (
    get_people_info,
    get_collection_info,
    get_top_5_movies, 
    get_rev_over_time, 
    get_all_unique_genres, 
    get_movie_details, 
    get_popularity_over_time, 
    calculate_director_producer_profit_margin, 
    merge_movie_collection, 
    merge_movie_weekly_performance, 
    calculate_avg_rev_by_actor, 
    include_profit_in_df, 
    calculate_roi, 
    merge_movie_video_stats
)
from src.bigquery_trial import predict_revenue, find_value
import pandas as pd
import altair as alt
import ast
from datetime import timedelta


## get the people information
people_df = get_people_info()
# print(people_df)
people_names = people_df['name'].tolist()
# print(people_names)
people_names.append("Others")

## get the collection information
col_df = get_collection_info()
col_names = col_df['name'].tolist()
col_names.append("Others")
col_names.append("None")

## Set list of movie genres
gen_list = ["Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy","History","Horror","Music","Mystery","Romance","Science_Fiction"]

def input_fields():
    st.title("🎩 Input Movie Data To Predict")

    all_required_fields_filled = True


    # Text Input
    movie_title = st.text_input("Enter movie title:", "", key="movie_title")
    st.write("You entered:", movie_title)

    if not movie_title:
        all_required_fields_filled = False
        st.warning('Please enter a movie title.')

    # Number Input
    budget = st.number_input("Enter movie budget (in USD):", min_value=0, value=0, step=1)
    st.write("Movie budget is:", budget)
    if not budget:
        all_required_fields_filled = False
        st.warning('Please enter a movie budget.')


    runtime = st.number_input("Enter movie runtime (in minutes):", min_value=0, max_value=300, value=0, step=1)
    st.write("Movie runtime is:", runtime)
    if not runtime:
        all_required_fields_filled = False
        st.warning('Please enter a movie runtime.')    

    # Date Picker
    release_date = st.date_input("Enter movie release date:", value=None, min_value=None, max_value=None, key=None, help=None, on_change=None, args=None, kwargs=None, format="YYYY-MM-DD", disabled=False, label_visibility="visible")
    st.write("Movie release date is:", release_date)
    if not release_date:
        release_date = datetime.date.today()
    # print(type(release_date))  ## data type datetime.date

    # Dropdown Select
    yes_no_list = ["Yes", "No"]
    is_adult = st.selectbox("Is the movie adult only?", yes_no_list)
    st.write("You selected:", is_adult)

    is_adapt = st.selectbox("Is the movie an adaptation?", yes_no_list)
    st.write("You selected:", is_adapt)

    # Multiselect
    genre_list = ["Romance", "Action", "Horror"]
    genres = st.multiselect("Select the movie genre:", gen_list, default=None)
    # st.write("You selected:", selected_option)

    # People Information
    cast1 = st.selectbox("Select movie cast 1:", people_names, placeholder="Please select an Actor", index=None)
    st.write("The movie cast 1 is:", cast1)

    cast2 = st.selectbox("Select movie cast 2:", people_names, placeholder="Please select an Actor", index=None)
    st.write("The movie cast 2 is:", cast2)

    producer = st.selectbox("Select movie producer:", people_names, placeholder="Please select your Producer", index=None)
    st.write("The movie producer is:", producer)

    director = st.selectbox("Select movie director:", people_names, placeholder="Please select your Director", index=None)
    st.write("The movie director is:", director)

    # Slider Input
    # budget = st.slider("Select your movie budget:", min_value=0, max_value=10000000, value=50000, step=1000)
    # st.write("Your movie budget is:", budget)

    ## Trailer Information

    trailer_view = st.number_input("Please enter your movie trailer view count:", min_value=0, value=0, step=1)
    st.write("Your movie trailer view count is:", trailer_view)

    like_count = st.number_input("Please enter your movie trailer like count:", min_value=0, value=0, step=1)
    st.write("Your movie trailer like count is:", like_count)

    comment_count = st.number_input("Please enter your movie trailer comment count:", min_value=0, value=0, step=1)
    st.write("Your movie trailer comment count is:", comment_count)

    ## Series Information
    preseries = st.selectbox("Select movie series:", col_names, placeholder="Please select the movie series name", index=None)
    st.write("The movie belongs to series:", preseries)
    
    is_button_disabled = not all_required_fields_filled

    
    if st.button("Predict", disabled=is_button_disabled, key = "go_to_dashboard"):
        print("Predict button clicked")

        ## Getting relevant information from database for revenue prediction
        cast1_popularity = find_value(people_df, cast1, 'name', 'tmdb_popularity')
        cast2_popularity = find_value(people_df, cast2, 'name', 'tmdb_popularity')
        producer_popularity = find_value(people_df, producer, 'name', 'tmdb_popularity')
        director_popularity = find_value(people_df, director, 'name', 'tmdb_popularity')
        collection_popularity = find_value(col_df, preseries, 'name', 'avg_popularity_before_2020')

        ## Input transform
        if is_adult == "Yes":
            is_adult = 1
        else:
            is_adult = 0
        
        if is_adapt == "Yes":
            is_adapt = 1
        else:
            is_adapt = 0
        
        if release_date != None:
            release_at = release_date.strftime("%Y-%m-%d")
            
        


        print("check if relevant values are retrieved")
        print(collection_popularity)
        output = predict_revenue(budget, release_at, runtime, is_adult, is_adapt, cast1_popularity, cast2_popularity, director_popularity, producer_popularity, trailer_view, like_count, comment_count, collection_popularity)
        print(output)
        lower_bound = round(output*0.7, 2)
        upper_bound = round(output*1.3, 2)
        st.subheader('Your predicted box office is', divider='rainbow')
        outcome = "(" + str(lower_bound) + " - " + str(upper_bound) + " USD)"
        st.subheader(output)
        st.markdown(f'<h5>{outcome}</h5>', unsafe_allow_html=True)
        # st.session_state['page'] = "Dashboard"
        print("Session state page set to Dashboard")

        # graph
        st.markdown('<h3>Budget VS Revenue</h3>', unsafe_allow_html=True)
        budget_vs_rev = st.selectbox(
            'Choose Revenue or Profit',
            options=['Revenue', 'Profit'],
            index=0,
            key="budget_vs_rev_or_profit_predict"
        )
        movie_df = get_movie_details()
        movie_df = include_profit_in_df(movie_df)
        scatter_chart = alt.Chart(movie_df).mark_circle(size=60).encode(
            x=alt.X('budget:Q', axis=alt.Axis(title='Budget')),
            y=alt.Y(f'{budget_vs_rev.lower()}:Q', axis=alt.Axis(title=budget_vs_rev)),
            tooltip=['budget', f'{budget_vs_rev.lower()}']
        ).interactive()

        regression_line = scatter_chart.transform_regression(
            'budget', f'{budget_vs_rev.lower()}', method='linear'
        ).mark_line(color='red')

        # Highlight the new point
        new_point = {'budget': budget, 'revenue': output, 'profit': output-budget}
        highlight = alt.Chart(pd.DataFrame([new_point])).mark_circle(color='red', size=200).encode(
            x='budget',
            y=f'{budget_vs_rev.lower()}'
        )

        final_chart = scatter_chart + regression_line + highlight
        st.altair_chart(final_chart, use_container_width=True)

        ### Add in potential competitors
        st.markdown('<h4>Potential Competitors</h4>', unsafe_allow_html=True)
        filtered_df = movie_df[movie_df['genres'].apply(lambda x: any(item in x for item in genres))]
        start = release_date - timedelta(days=45)
        end = release_date + timedelta(days=45)
        start = pd.Timestamp(start)
        end = pd.Timestamp(end)
        filtered_df['release_date'] = pd.to_datetime(filtered_df['release_date'])
        filtered_df = filtered_df[(filtered_df['release_date'] > start) & (filtered_df['release_date'] < end)]
        print(filtered_df)
        mapping = {'revenue':'Revenue',
                   'budget':'Budget',
                   'tmdb_popularity':'TMDB Popularity',
                   'tmdb_vote_average':'TMDB Vote Average',
                   'tmdb_vote_count':'TMDB Vote Count',
                   'genres':'Genres',
                   'title':'Movie Title',
                   'release_date':'Release Date',
                   }

        filtered_df = filtered_df.rename(columns=mapping)
        filtered_df['Release Date'] = filtered_df['Release Date'].dt.date
        filtered_df = filtered_df.sort_values(by='Release Date')
        st.markdown(
            """
            <style>
            .stDataFrame {
                width: 100%;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        st.dataframe(filtered_df,
                     column_order = ('Movie Title', 'Revenue', 'Budget', 'Release Date', 'TMDB Popularity', 'TMDB Vote Average', 'Genres'),
                    hide_index = True)

        