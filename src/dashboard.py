import streamlit as st
import pandas as pd
from src.utils import (
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
import altair as alt
import ast


def dashboard():

    st.title('🎥 Movie Performance Dashboard')
    st.markdown('<style>div.block-container{padding-top:2rem;}</style>',unsafe_allow_html=True)



    # graph
    video_stats_df = merge_movie_video_stats()
    highest_revenue_movie = video_stats_df.loc[video_stats_df['revenue'].idxmax()]
    highest_vote_average_movie = video_stats_df.loc[video_stats_df['tmdb_vote_average'].idxmax()]
    highest_view_count_movie = video_stats_df.loc[video_stats_df['view_count'].idxmax()]

    lowest_revenue_movie = video_stats_df.loc[video_stats_df['revenue'].idxmin()]
    lowest_vote_average_movie = video_stats_df.loc[video_stats_df['tmdb_vote_average'].idxmin()]
    lowest_view_count_movie = video_stats_df.loc[video_stats_df['view_count'].idxmin()]

    st.markdown('<h3>Top Movie Metrics</h3>', unsafe_allow_html=True)
    st.write(
        """
        <style>
        [data-testid="stMetricDelta"] svg {
            display: none;
        }
        [data-testid="stMetricValue"] {
            font-size: 24px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            label=":green[Highest Revenue]",
            value=highest_revenue_movie['original_title'],
            delta="${:,.2f}".format(highest_revenue_movie['revenue']),
            delta_color="normal"
        )
        st.metric(
            label=":red[Lowest Revenue]",
            value=lowest_revenue_movie['original_title'],
            delta="${:,.2f}".format(lowest_revenue_movie['revenue']),
            delta_color="inverse"
        )
    with col2:
        st.metric(
            label=":green[Highest TMDB Vote Average]",
            value=highest_vote_average_movie['original_title'],
            delta="{:.1f}".format(highest_vote_average_movie['tmdb_vote_average']),
            delta_color="normal"
        )
        st.metric(
            label=":red[Lowest TMDB Vote Average] ",
            value=lowest_vote_average_movie['original_title'],
            delta="{:.1f}".format(lowest_vote_average_movie['tmdb_vote_average']),
            delta_color="inverse"        
        )

    with col3:
        st.metric(
            label=":green[Highest View Count]",
            value=highest_view_count_movie['original_title'],
            delta="{:,.0f}".format(highest_view_count_movie['view_count']),
            delta_color="normal"
        )
        st.metric(
            label=":red[Lowest View Count]",
            value=lowest_view_count_movie['original_title'],
            delta="{:,.0f}".format(lowest_view_count_movie['view_count']),
            delta_color="inverse"
        )



    # graph
    st.markdown('<style>div.block-container{padding-top:2rem;}</style>',unsafe_allow_html=True)
    st.markdown('<h3>Total Box Office Revenue By Genre Over Time</h3>', unsafe_allow_html=True)
    rev_or_profit = st.selectbox(
        'Choose Revenue or Profit',
        options=['Revenue', 'Profit'],
        index=0,
        key="rev_over_time_selectbox"
    )
    all_genres = get_all_unique_genres()
    selected_genres = st.multiselect('Select Genre(s):', all_genres, default=all_genres, key="genre_rev_over_time_multiselect")
    if not selected_genres:
        st.warning('Please select at least one genre to display the chart.')
        empty_chart = alt.Chart(pd.DataFrame()).mark_line().encode(
            x=alt.X('week_end_date:T', axis=alt.Axis(title='Week End Date')),
            y=alt.Y(f'{rev_or_profit.lower()}:Q', axis=alt.Axis(title=f'{rev_or_profit}'))
        ).properties(
            width=700,
            height=400
        )
        st.altair_chart(empty_chart, use_container_width=True)
    else:
        df_rev_over_time = get_rev_over_time(rev_or_profit)
        filtered_data = df_rev_over_time[df_rev_over_time['genres'].isin(selected_genres)]

        pivoted_data = filtered_data.pivot(index='week_end_date', columns='genres', values=rev_or_profit.lower()).reset_index()

        melted_data = pivoted_data.melt('week_end_date', var_name='Genre', value_name=rev_or_profit.lower())

        if not melted_data[rev_or_profit.lower()].any():
            st.warning(f'No {rev_or_profit.lower()} data available for the selected genre(s).')
            empty_chart = alt.Chart(pd.DataFrame()).mark_line().encode(
                x=alt.X('week_end_date:T', axis=alt.Axis(title='Week End Date')),
                y=alt.Y(f'{rev_or_profit.lower()}:Q', axis=alt.Axis(title=f'{rev_or_profit}'))
            ).properties(
                width=700,
                height=400
            )
            st.altair_chart(empty_chart, use_container_width=True)
        else:
            # Check if there are multiple unique values for the revenue or profit
            unique_values = melted_data[rev_or_profit.lower()].unique()
            if len(unique_values) > 1:
                y_value_range = st.slider(
                    f'Select the range of {rev_or_profit} values',
                    min_value=int(melted_data[rev_or_profit.lower()].min()), 
                    max_value=int(melted_data[rev_or_profit.lower()].max()),
                    value=(int(melted_data[rev_or_profit.lower()].min()), int(melted_data[rev_or_profit.lower()].max())),
                    key="genre_rev_over_time_slider"
                )
            elif len(unique_values) == 1:
                y_value_range = (unique_values[0], unique_values[0])

            melted_data = melted_data[(melted_data[rev_or_profit.lower()] >= y_value_range[0]) & 
                                    (melted_data[rev_or_profit.lower()] <= y_value_range[1])]
            line_chart = alt.Chart(melted_data).mark_line(point=True).encode(
                x=alt.X('week_end_date:T', axis=alt.Axis(title='Week End Date')),
                y=alt.Y(f'{rev_or_profit.lower()}:Q', axis=alt.Axis(title=f'{rev_or_profit}')),
                color='Genre:N',
                tooltip=['week_end_date', f'{rev_or_profit.lower()}', 'Genre']
            ).properties(
                width=700,
                height=400
            ).configure_legend(
                strokeColor='gray',
                fillColor='#EEEEEE',
                padding=10,
                cornerRadius=10,
                orient='right'
            )
            st.altair_chart(line_chart, use_container_width=True)



    # graph
    st.markdown('<h3>Total Box Office Popularity By Genre Over Time</h3>', unsafe_allow_html=True)
    y_axis_metric = st.selectbox(
        'Choose the Y-axis metric:',
        options=['TMDB Popularity', 'TMDB Vote Average', 'TMDB Vote Count'],
        index=0,
        key="y_axis_metric_selectbox"
    )
    y_metric_mapping = {'TMDB Popularity': 'tmdb_popularity', 'TMDB Vote Average': 'tmdb_vote_average', 'TMDB Vote Count': 'tmdb_vote_count'}

    df_pop_over_time = get_popularity_over_time(y_metric_mapping[y_axis_metric])
    df_pop_over_time[y_metric_mapping[y_axis_metric]] = pd.to_numeric(df_pop_over_time[y_metric_mapping[y_axis_metric]], errors='coerce')

    selected_genres = st.multiselect(
        'Select Genre(s):', all_genres, default=all_genres, key="genre_pop_over_time_multiselect"
    )
    if not selected_genres:
        st.warning('Please select at least one genre to display the chart.')
        empty_chart = alt.Chart(pd.DataFrame()).mark_line().encode(
            x=alt.X('week_end_date:T', axis=alt.Axis(title='Week End Date')),
            y=alt.Y(f'{y_metric_mapping[y_axis_metric]}:Q', axis=alt.Axis(title=f'{y_axis_metric}'))
        ).properties(
            width=700,
            height=400
        )
        st.altair_chart(empty_chart, use_container_width=True)
    else:

        filtered_data = df_pop_over_time[df_pop_over_time['genres'].isin(selected_genres)]
        pivoted_data = filtered_data.pivot(index='week_end_date', columns='genres', values=y_metric_mapping[y_axis_metric]).reset_index()
        melted_data = pivoted_data.melt('week_end_date', var_name='Genre', value_name=y_metric_mapping[y_axis_metric])
        melted_data = melted_data.dropna(subset=[y_metric_mapping[y_axis_metric]])
        
        if not melted_data[y_metric_mapping[y_axis_metric]].any():
            st.warning(f'No {y_axis_metric} data available for the selected genre(s).')
            empty_chart = alt.Chart(pd.DataFrame()).mark_line().encode(
                x=alt.X('week_end_date:T', axis=alt.Axis(title='Week End Date')),
                y=alt.Y(f'{y_metric_mapping[y_axis_metric]}:Q', axis=alt.Axis(title=f'{y_axis_metric}'))
            ).properties(
                width=700,
                height=400
            )
            st.altair_chart(empty_chart, use_container_width=True)
        else:
            unique_values = melted_data[y_metric_mapping[y_axis_metric]].unique()
            if len(unique_values) > 1:
                y_value_range = st.slider(
                    f'Select the range of {y_axis_metric} values',
                    min_value=int(melted_data[y_metric_mapping[y_axis_metric]].min()), 
                    max_value=int(melted_data[y_metric_mapping[y_axis_metric]].max()),
                    value=(int(melted_data[y_metric_mapping[y_axis_metric]].min()), int(melted_data[y_metric_mapping[y_axis_metric]].max())),
                    key="genre_pop_over_time_slider"
                )
            elif len(unique_values) == 1:
                y_value_range = (unique_values[0], unique_values[0])

            melted_data = melted_data[(melted_data[y_metric_mapping[y_axis_metric]] >= y_value_range[0]) & 
                                    (melted_data[y_metric_mapping[y_axis_metric]] <= y_value_range[1])]
            line_chart = alt.Chart(melted_data).mark_line(point=True).encode(
                x=alt.X('week_end_date:T', axis=alt.Axis(title='Week End Date')),
                y=alt.Y(f'{y_metric_mapping[y_axis_metric]}:Q', scale=alt.Scale(domain=y_value_range), axis=alt.Axis(title=y_axis_metric)),
                color='Genre:N',
                tooltip=['week_end_date', y_metric_mapping[y_axis_metric], 'Genre']
            ).properties(
                width=700,
                height=400
            ).configure_legend(
                strokeColor='gray',
                fillColor='#EEEEEE',
                padding=10,
                cornerRadius=10,
                orient='right'
            )
            st.altair_chart(line_chart, use_container_width=True)
            


    # graph
    st.markdown('<h3>Budget VS Revenue</h3>', unsafe_allow_html=True)
    budget_vs_rev = st.selectbox(
        'Choose Revenue or Profit',
        options=['Revenue', 'Profit'],
        index=0,
        key="budget_vs_rev_or_profit"
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
    final_chart = scatter_chart + regression_line
    st.altair_chart(final_chart, use_container_width=True)



    # graph
    st.markdown('<h3>Profitability by Director/Producer</h3>', unsafe_allow_html=True)

    person = st.selectbox(
        'Choose director or producer:',
        options=['Director', 'Producer'],
        index=0,
        key="person_selectbox"
    )
    director_profit_margin = calculate_director_producer_profit_margin(person)
    director_profit_margin = director_profit_margin.sort_values('profit_margin', ascending=False)
    bar_chart = alt.Chart(director_profit_margin).mark_bar().encode(
        x=alt.X('name:N', sort='-y', axis=alt.Axis(title=f'{person} Name')), 
        y=alt.Y('profit_margin:Q', axis=alt.Axis(title='Profit Margin')),
        tooltip=['name', 'profit_margin']
    ).properties(
        width=700,
        height=400
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    )

    st.altair_chart(bar_chart, use_container_width=True)



    # graph
    st.markdown('<h3>Film Collection Performance</h3>', unsafe_allow_html=True)
    df_collections = merge_movie_collection()
    cumulative_revenue = df_collections.groupby('collection_id')['revenue'].sum().reset_index()
    cumulative_revenue = pd.merge(cumulative_revenue, df_collections[['collection_id', 'name']], on='collection_id', how='left')
    cumulative_revenue = cumulative_revenue.sort_values('revenue', ascending=False)

    y_value_range = st.slider(
        'Select the range of Cumulative Revenue values',
        min_value=int(cumulative_revenue['revenue'].min()), 
        max_value=int(cumulative_revenue['revenue'].max()),
        value=(int(cumulative_revenue['revenue'].min()), int(cumulative_revenue['revenue'].max())),
        key="cumulative_rev_collection"
    )
    cumulative_revenue = cumulative_revenue[(cumulative_revenue['revenue'] >= y_value_range[0]) & 
                            (cumulative_revenue['revenue'] <= y_value_range[1])]
    
    bar_chart = alt.Chart(cumulative_revenue).mark_bar().encode(
        x=alt.X('name:N', sort='-y', title='Film Collection'),
        y=alt.Y('revenue:Q', title='Cumulative Revenue'),
        color=alt.Color('name:N', legend=None) 
    ).properties(
        width=alt.Step(40) 
    )
    st.altair_chart(bar_chart, use_container_width=True)


    # graph
    st.markdown('<h3>Domestic vs. International Revenue</h3>', unsafe_allow_html=True)
    movie_weekly_performance = merge_movie_weekly_performance()
    movie_weekly_performance['international_revenue'] = movie_weekly_performance['revenue'] - movie_weekly_performance['domestic_gross']
    df_long = movie_weekly_performance.melt(id_vars=['original_title'], 
                                            value_vars=['domestic_gross', 'international_revenue'], 
                                            var_name='Revenue Type', 
                                            value_name='Revenue')

    domestic_min, domestic_max = int(movie_weekly_performance['domestic_gross'].min()), int(movie_weekly_performance['domestic_gross'].max())
    international_min, international_max = int(movie_weekly_performance['international_revenue'].min()), int(movie_weekly_performance['international_revenue'].max())

    domestic_range = st.slider('Select Domestic Gross Range', min_value=domestic_min, max_value=domestic_max, value=(domestic_min, domestic_max), key='domestic')
    international_range = st.slider('Select International Revenue Range', min_value=international_min, max_value=international_max, value=(international_min, international_max), key='international')

    filtered_df = movie_weekly_performance.copy()
    filtered_df = filtered_df[(filtered_df['domestic_gross'] >= domestic_range[0]) & (filtered_df['domestic_gross'] <= domestic_range[1])]
    filtered_df = filtered_df[(filtered_df['international_revenue'] >= international_range[0]) & (filtered_df['international_revenue'] <= international_range[1])]

    df_long_filtered = filtered_df.melt(id_vars=['original_title'], 
                                    value_vars=['domestic_gross', 'international_revenue'], 
                                    var_name='Revenue Type', 
                                    value_name='Revenue')

    chart = alt.Chart(df_long_filtered).mark_bar().encode(
        x=alt.X('original_title:N', title="Movie Title"),
        y=alt.Y('Revenue:Q', stack='zero'), 
        color='Revenue Type:N',
        tooltip=['original_title', 'Revenue Type', 'Revenue']
    ).properties(
        width=700,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)


    # graph
    st.markdown('<h3>Cast Influence on Revenue</h3>', unsafe_allow_html=True)
    actor_df = calculate_avg_rev_by_actor()
    bubble_chart = alt.Chart(actor_df).mark_circle(size=100).encode(
        x=alt.X('actor:N', title='Actor'),
        y=alt.Y('revenue:Q', title='Average Revenue'),
        tooltip=['actor:N', 'revenue:Q']
    ).properties(
        width=800,
        height=400
    ).configure_axis(
        labelFontSize=12,
        titleFontSize=14
    )
    st.altair_chart(bubble_chart, use_container_width=True)



    # graph
    st.markdown('<h3>Distribution of ROI for All Films</h3>', unsafe_allow_html=True)
    roi_df = calculate_roi()
    roi_percentile_01 = roi_df['ROI'].quantile(0.01)
    roi_percentile_99 = roi_df['ROI'].quantile(0.99)
    df_filtered = roi_df[(roi_df['ROI'] >= roi_percentile_01) & (roi_df['ROI'] <= roi_percentile_99)]
    histogram = alt.Chart(df_filtered).mark_bar().encode(
        x=alt.X('ROI:Q', bin=alt.Bin(maxbins=50), title='ROI'),
        y=alt.Y('count()', title='Number of Films')
    )
    density_plot = alt.Chart(df_filtered).transform_density(
        'ROI',
        as_=['ROI', 'density'],
    ).mark_area().encode(
        x='ROI:Q',
        y='density:Q',
    )
    st.altair_chart(histogram, use_container_width=True)

    st.altair_chart(density_plot, use_container_width=True)


    # graph
    st.markdown('<h3>Audience Engagement Metrics by Movie</h3>', unsafe_allow_html=True)
    video_stats_df = merge_movie_video_stats()
    view_min, view_max = int(video_stats_df['view_count'].min()), int(video_stats_df['view_count'].max())
    like_min, like_max = int(video_stats_df['like_count'].min()), int(video_stats_df['like_count'].max())
    comment_min, comment_max = int(video_stats_df['comment_count'].min()), int(video_stats_df['comment_count'].max())

    view_range = st.slider('View Count Range', min_value=view_min, max_value=view_max, value=(view_min, view_max), key='view')
    like_range = st.slider('Like Count Range', min_value=like_min, max_value=like_max, value=(like_min, like_max), key='like')
    comment_range = st.slider('Comment Count Range', min_value=comment_min, max_value=comment_max, value=(comment_min, comment_max), key='comment')

    filtered_movies = video_stats_df[
        (video_stats_df['view_count'] >= view_range[0]) & (video_stats_df['view_count'] <= view_range[1]) &
        (video_stats_df['like_count'] >= like_range[0]) & (video_stats_df['like_count'] <= like_range[1]) &
        (video_stats_df['comment_count'] >= comment_range[0]) & (video_stats_df['comment_count'] <= comment_range[1])
    ]
    df_long_filtered = filtered_movies.melt(id_vars='original_title', 
                                            value_vars=['view_count', 'like_count', 'comment_count'], 
                                            var_name='Metric', 
                                            value_name='Count')

    audience_engagement_chart = alt.Chart(df_long_filtered).mark_bar().encode(
        x=alt.X('original_title:N', title="Movie Title"),
        y=alt.Y('Count:Q', stack=None), 
        color='Metric:N',
        tooltip=['original_title', 'Metric', 'Count']
    ).properties(
        title='Audience Engagement Metrics by Movie'
    )
    st.altair_chart(audience_engagement_chart, use_container_width=True)


    # graph
    st.markdown('<h3>Top 5 Movies by Box Office Revenue</h3>', unsafe_allow_html=True)
    df = get_top_5_movies()   
    df['Genres'] = df['Genres'].apply(lambda x: ', '.join(ast.literal_eval(x)) if isinstance(x, str) else ', '.join(x))
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
    st.dataframe(df.style.
                 highlight_max(subset='Revenue', color='#A6E0B5').
                 highlight_max(subset='TMDB Popularity', color='#A6E0B5').
                 highlight_max(subset='TMDB Vote Average', color='#A6E0B5').
                 highlight_min(subset='Revenue', color='#FC8D8D').
                 highlight_min(subset='TMDB Popularity', color='#FC8D8D').
                 highlight_min(subset='TMDB Vote Average', color='#FC8D8D'), 
                 column_order = ('Movie Title', 'Revenue', 'TMDB Popularity', 'TMDB Vote Average', 'Genres'),
                 hide_index = True)