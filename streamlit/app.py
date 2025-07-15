# app.py

import streamlit as st
import pandas as pd
import s3fs
import altair as alt

# Load your transformed data
df = pd.read_parquet("s3://raw-youtube-data-9/youtube_processed/", engine="pyarrow", storage_options={"anon": False})


st.title("📊 YouTube Popularity Forecast Dashboard")
st.markdown("""
Explore YouTube trending videos related to Generative AI with insights on content categories, freshness, and publishing trends.
""")

#Adding sidebar filters for user interaction
# Categories filter
categories = df['content_category'].unique().tolist()
selected_categories = st.sidebar.multiselect(
    "Select Content Categories",
    options=categories,
    default=categories
)

# Date range filter
df['publish_date'] = pd.to_datetime(df['publish_date'].astype(str), errors='coerce')

min_date = df['publish_date'].min()
max_date = df['publish_date'].max()
start_date, end_date = st.sidebar.date_input(
    "Select Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Filter dataframe based on sidebar selections
# Convert Python date to pandas Timestamp (at midnight)
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

filtered_df = df[
    (df['content_category'].isin(selected_categories)) &
    (df['publish_date'] >= start_date) &
    (df['publish_date'] <= end_date)
]

#displaying key metrics with filtered data
st.subheader("📌 Key Metrics")
col1, col2, col3 = st.columns(3)
col1.metric("Total Videos", filtered_df.shape[0])
col2.metric("Unique Videos", filtered_df['video_id'].nunique())
col3.metric("Unique Channels", filtered_df['channel_title'].nunique())


#Show Publishing Trend Over Time (Line Chart)
st.subheader("📈 Videos Published Over Time")

time_series = (
    filtered_df.groupby('publish_date')
    .size()
    .reset_index(name='video_count')
    .sort_values('publish_date')
)

line_chart = alt.Chart(time_series).mark_line(point=True).encode(
    x=alt.X('publish_date:T', title='Publish Date'),
    y=alt.Y('video_count:Q', title='Number of Videos'),
    tooltip=['publish_date', 'video_count']
).properties(width=700, height=300)

st.altair_chart(line_chart, use_container_width=True)

#Show Video Count by Category (Bar Chart)
st.subheader("📊 Video Count by Content Category")

category_count = (
    filtered_df['content_category']
    .value_counts()
    .reset_index()
    .rename(columns={'index': 'content_category', 'content_category': 'count'})
)
category_count.columns = ['content_category', 'video_count']

bar_chart = alt.Chart(category_count).mark_bar().encode(
    x=alt.X('content_category:N', sort='-y', title='Category'),
    y=alt.Y('video_count:Q', title='Video Count'),
    tooltip=['content_category', 'video_count'],
    color='content_category:N'
).properties(width=700, height=300)

st.altair_chart(bar_chart, use_container_width=True)

#Show Freshness Distribution (Histogram of days since published)
st.subheader("⏳ Distribution of Video Freshness (Days Since Published)")

hist_data = filtered_df[['days_since_published']].dropna()

hist_chart = alt.Chart(hist_data).mark_bar().encode(
    alt.X("days_since_published:Q", bin=alt.Bin(maxbins=30), title="Days Since Published"),
    y='count()',
    tooltip=['count()']
).properties(width=700, height=300)

st.altair_chart(hist_chart, use_container_width=True)

# Top Channels by Video Count (Bar Chart)
st.subheader("🏆 Top Channels by Number of Videos")

top_channels = (
    filtered_df['channel_title']
    .value_counts()
    .reset_index()
)
top_channels.columns = ['channel_title', 'video_count']

channels_chart = alt.Chart(top_channels).mark_bar().encode(
    y=alt.Y('channel_title:N', sort='-x', title='Channel'),
    x=alt.X('video_count:Q', title='Number of Videos'),
    tooltip=['channel_title', 'video_count'],
    color='channel_title:N'
).properties(width=700, height=400)

st.altair_chart(channels_chart, use_container_width=True)


# Processed data preview
with st.expander("🔍 View Processed Data"):
    st.dataframe(df)

#Search Box for Titles
st.subheader("🔎 Search Videos by Title")

search_text = st.text_input("Enter keywords (comma-separated):").lower()
if search_text:
    keywords = [kw.strip() for kw in search_text.split(',')]
    mask = filtered_df['title'].str.lower().apply(
        lambda x: any(k in x for k in keywords)
    )
    search_results = filtered_df[mask]
    st.write(f"Found {search_results.shape[0]} videos matching your search:")
    st.dataframe(search_results[['title', 'channel_title', 'publish_date', 'content_category']].head(50))

