"""
Analytics page for MVR Crime Bulletin - Map visualizations
Optimized for performance with caching and multi-language support.
"""
import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from folium.plugins import HeatMap, MarkerCluster
from datetime import datetime, timedelta
import hashlib

from macedonia_coords import get_coords, get_population, DEFAULT_POPULATION, MACEDONIA_COORDS
from translations import t, get_language_name, get_available_languages
import numpy as np

# Center of Macedonia
MACEDONIA_CENTER = [41.6081, 21.7453]

# Crime type to color/icon mapping
# Keywords should be ordered from most specific to least specific
CRIME_CATEGORIES = {
    # Drugs - must come before other keywords
    ('наркотичн', 'дрог', 'марихуан', 'кокаин', 'хероин', 'амфетамин', 'психотропн'): 
        {'color': 'purple', 'icon': 'fa-leaf', 'label_key': 'cat_drugs'},
    
    # Theft/Robbery
    ('кражб', 'крад', 'грабеж', 'разбој', 'ограби'): 
        {'color': 'orange', 'icon': 'fa-gem', 'label_key': 'cat_theft'},
    
    # Violence/Assault - must catch variations like безобзирн, бедобзирн etc
    ('насил', 'напад', 'тешк', 'повред', 'бие', 'физичк', 'удар', 'безобзирн', 'безо$бзирн', 'тупаница'): 
        {'color': 'red', 'icon': 'fa-fist-raised', 'label_key': 'cat_violence'},
    
    # Traffic/Accidents
    ('сообраќај', 'несреќ', 'возило', 'мотор', 'автомобил', 'пешачк', 'сообр', 'пат'): 
        {'color': 'blue', 'icon': 'fa-car', 'label_key': 'cat_traffic'},
    
    # Weapons
    ('оруж', 'пиштол', 'пушк', 'гранат', 'експлозив', 'артилериск'): 
        {'color': 'black', 'icon': 'fa-crosshairs', 'label_key': 'cat_weapons'},
    
    # Arson/Fire
    ('пален', 'пожар', 'горе', 'пламен'): 
        {'color': 'darkorange', 'icon': 'fa-fire', 'label_key': 'cat_arson'},
    
    # Fraud
    ('измам', 'превар', 'фалсифик', 'лажн'): 
        {'color': 'gray', 'icon': 'fa-money-bill', 'label_key': 'cat_theft'},
}


def get_crime_config(crime_type, lang='mk'):
    """Get color, icon, and label for a crime type."""
    if not crime_type:
        return 'lightgray', 'fa-question', t('cat_other', lang)
    
    crime_lower = crime_type.lower()
    
    for keywords, config in CRIME_CATEGORIES.items():
        for keyword in keywords:
            if keyword in crime_lower:
                return config['color'], config['icon'], t(config['label_key'], lang)
    
    return 'lightblue', 'fa-exclamation-circle', t('cat_other', lang)


@st.cache_data(ttl=3600)
def _get_incidents_cached():
    """Load incidents from database with caching."""
    from sqlalchemy import create_engine, text
    from config import get_settings
    settings = get_settings()
    engine = create_engine(
        settings.database_url,
        connect_args={"check_same_thread": False} if "sqlite" in settings.database_url else {}
    )
    
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT * FROM crime_incidents"), conn)
    
    # Pre-process dates
    if 'crime_date' in df.columns:
        df['crime_date'] = pd.to_datetime(df['crime_date'], errors='coerce')
    
    return df


def add_coords_to_df(df):
    """Add lat/lon columns using precise coords when available."""
    df = df.copy()
    
    # Try precise coordinates first
    if 'precise_lat' in df.columns:
        df['lat'] = df['precise_lat']
    else:
        df['lat'] = None

    if 'precise_lon' in df.columns:
        df['lon'] = df['precise_lon']
    else:
        df['lon'] = None

    # Fill missing with city coordinates (vectorized)
    if df['lat'].isna().any():
        city_coords = {city: coords for city, coords in MACEDONIA_COORDS.items()}
        df['lat'] = df['lat'].fillna(df['location_city'].map(lambda c: city_coords.get(c, (None, None))[0] if c else None))
        df['lon'] = df['lon'].fillna(df['location_city'].map(lambda c: city_coords.get(c, (None, None))[1] if c else None))
    
    return df


@st.cache_data(ttl=3600)
def _get_crime_aggregates():
    """Pre-compute crime aggregates for performance."""
    df = _get_incidents_cached()
    
    # City counts
    city_counts = df['location_city'].value_counts().to_dict()
    
    # Crime type counts
    crime_counts = df['crime_type'].value_counts().to_dict()
    
    # Date range
    date_min = df['crime_date'].min()
    date_max = df['crime_date'].max()
    
    return {
        'city_counts': city_counts,
        'crime_counts': crime_counts,
        'date_min': date_min,
        'date_max': date_max,
        'total': len(df),
        'cities': sorted(df['location_city'].dropna().unique().tolist()),
        'crime_types': sorted(df['crime_type'].dropna().unique().tolist())
    }


def create_filters():
    """Create filter sidebar and return filtered dataframe."""
    
    # Get language from session state
    lang = st.session_state.get('lang', 'mk')
    
    # Load aggregates for filter options
    aggregates = _get_crime_aggregates()
    
    st.sidebar.header(t('filters', lang))
    
    # Population normalization toggle
    normalize = st.sidebar.checkbox(
        t('normalize_pop', lang),
        value=False
    )
    
    # Date range
    st.sidebar.subheader(t('date_range', lang))
    
    date_min = aggregates['date_min']
    date_max = aggregates['date_max']
    
    if pd.notna(date_min) and pd.notna(date_max):
        default_start = max(date_min, datetime.now() - timedelta(days=365))
        default_end = min(date_max, datetime.now())
        
        date_range = st.sidebar.date_input(
            t('date_range', lang),
            value=(default_start, default_end),
            min_value=date_min,
            max_value=date_max
        )
    else:
        date_range = st.sidebar.date_input(
            t('date_range', lang),
            value=(datetime.now() - timedelta(days=30), datetime.now())
        )
    
    # Crime type filter
    crime_types = [""] + [str(c)[:50] for c in aggregates['crime_types']]
    crime_type_display = [t('all_cities', lang)] + crime_types[1:]
    crime_type = st.sidebar.selectbox(
        t('crime_types', lang),
        crime_type_display,
        index=0
    )
    
    # City filter
    cities = [""] + aggregates['cities']
    cities_display = [t('all_cities', lang)] + cities[1:]
    city = st.sidebar.selectbox(
        t('cities', lang),
        cities_display,
        index=0
    )
    
    # Gender filter
    gender = st.sidebar.selectbox(
        t('gender', lang),
        ["", "male", "female"]
    )
    
    # Get fresh data and apply filters
    df = _get_incidents_cached()
    
    # Apply date filter
    if date_range and len(date_range) == 2:
        start_date, end_date = date_range
        df = df[
            (df['crime_date'] >= pd.to_datetime(start_date)) &
            (df['crime_date'] <= pd.to_datetime(end_date))
        ]
    
    # Apply crime type filter
    if crime_type and crime_type != t('all_cities', lang):
        df = df[df['crime_type'].str.contains(crime_type[:50], na=False, case=False)]
    
    # Apply city filter
    if city and city != t('all_cities', lang):
        df = df[df['location_city'] == city]
    
    # Apply gender filter
    if gender:
        df = df[df['perpetrator_gender'] == gender]
    
    # Store settings in session state
    st.session_state['normalize'] = normalize
    
    return df


def tab_heatmap(filtered_df, lang='mk'):
    """1. Heatmap - show crime density"""
    st.subheader(t('tab_heatmap', lang))
    
    normalize = st.session_state.get('normalize', False)
    
    if normalize:
        st.caption(t('heatmap_caption_normalized', lang))
    else:
        st.caption(t('heatmap_caption_raw', lang))
    
    df_with_coords = add_coords_to_df(filtered_df)
    df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
    
    if df_with_coords.empty:
        st.info(t('no_geocoded', lang))
        return
    
    m = folium.Map(location=MACEDONIA_CENTER, zoom_start=8, tiles='cartodbpositron')
    
    # Count crimes per city (vectorized)
    city_counts = df_with_coords.groupby(['location_city', 'lat', 'lon']).size().reset_index(name='count')
    
    # Normalize if needed
    if normalize:
        city_counts['population'] = city_counts['location_city'].apply(lambda x: get_population(str(x)))
        city_counts['rate'] = (city_counts['count'] / city_counts['population'] * 100000).round(1)
        heat_data = [[row['lat'], row['lon'], max(row['rate'], 1)] for _, row in city_counts.iterrows()]
    else:
        heat_data = [[row['lat'], row['lon'], row['count']] for _, row in city_counts.iterrows()]
    
    HeatMap(heat_data, radius=25, blur=15).add_to(m)
    
    st_folium(m, width=800, height=500)


def tab_bubble_map(filtered_df, lang='mk'):
    """2. Bubble Map - circle size = crime count or rate"""
    st.subheader(t('tab_bubble', lang))
    
    normalize = st.session_state.get('normalize', False)
    
    if normalize:
        st.caption(t('bubble_caption_normalized', lang))
    else:
        st.caption(t('bubble_caption_raw', lang))
    
    df_with_coords = add_coords_to_df(filtered_df)
    df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
    
    if df_with_coords.empty:
        st.info(t('no_data_map', lang))
        return
    
    m = folium.Map(location=MACEDONIA_CENTER, zoom_start=8, tiles='cartodbpositron')
    
    # Aggregate by city (vectorized)
    city_counts = df_with_coords.groupby(['location_city', 'lat', 'lon']).size().reset_index(name='count')
    city_counts['population'] = city_counts['location_city'].apply(lambda x: get_population(str(x)))
    city_counts['rate'] = (city_counts['count'] / city_counts['population'] * 100000).round(1)
    
    max_count = city_counts['count'].max() if normalize else city_counts['count'].max()
    
    for _, row in city_counts.iterrows():
        if normalize:
            size = 10 + (row['rate'] / max_count * 40)
            popup_text = f"<b>{row['location_city']}</b><br>Rate: {row['rate']} per 100k<br>Crimes: {row['count']}"
        else:
            size = 10 + (row['count'] / max_count * 40)
            popup_text = f"<b>{row['location_city']}</b><br>Crimes: {row['count']}"
        
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=size,
            popup=popup_text,
            color='red',
            fill=True,
            fillOpacity=0.6
        ).add_to(m)
    
    st_folium(m, width=800, height=500)


def tab_cluster_pins(filtered_df, lang='mk'):
    """3. Cluster Pins - individual markers grouped by zoom"""
    st.subheader(t('tab_clusters', lang))
    
    normalize = st.session_state.get('normalize', False)
    
    if normalize:
        st.caption(t('cluster_caption_normalized', lang))
    else:
        st.caption(t('cluster_caption', lang))
    
    df_with_coords = add_coords_to_df(filtered_df)
    df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
    
    if df_with_coords.empty:
        st.info(t('no_data_map', lang))
        return
    
    # Limit markers for performance (max 500)
    max_markers = 500
    if len(df_with_coords) > max_markers:
        df_with_coords = df_with_coords.sample(n=max_markers, random_state=42)
        st.caption(t('showing_sample', lang, shown=max_markers, total=len(filtered_df)))
    
    m = folium.Map(location=MACEDONIA_CENTER, zoom_start=8, tiles='cartodbpositron')
    
    # Get city crime rates if normalizing
    if normalize:
        city_counts = df_with_coords['location_city'].value_counts()
        city_rates = {city: (count / get_population(str(city))) * 100000 for city, count in city_counts.items()}
    else:
        city_rates = {}
    
    marker_cluster = MarkerCluster().add_to(m)
    
    for _, row in df_with_coords.iterrows():
        city = row.get('location_city', '')
        rate = city_rates.get(city, 0)
        
        # Color based on rate if normalizing
        if normalize and rate > 0:
            if rate > 100:
                color = 'darkred'
            elif rate > 50:
                color = 'red'
            elif rate > 20:
                color = 'orange'
            else:
                color = 'green'
        else:
            color = 'red'
        
        # Build location string
        location_str = row.get('location_city', t('na', lang))
        if row.get('location_address'):
            location_str = f"{row.get('location_address')} ({location_str})"
        
        popup_text = f"""
        <b>{row.get('crime_type', t('na', lang))[:40]}</b><br>
        <b>{t('location', lang)}:</b> {location_str}<br>
        <b>{t('date', lang)}:</b> {row.get('crime_date', t('na', lang))}<br>
        <b>{t('gender', lang)}:</b> {row.get('perpetrator_gender', t('na', lang))}
        """
        if normalize:
            popup_text = popup_text[:-6] + f"<b>Rate:</b> {rate:.1f} per 100k</div>"
        
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=folium.Popup(popup_text, max_width=300),
            icon=folium.Icon(color=color, icon='warning', prefix='fa')
        ).add_to(marker_cluster)
    
    st_folium(m, width=800, height=500)


def tab_map_with_filters(filtered_df, lang='mk'):
    """4. Map + Filter Bar"""
    st.subheader(t('tab_map_filters', lang))
    
    normalize = st.session_state.get('normalize', False)
    
    st.info(f"📊 {t('showing_incidents', lang).format(count=len(filtered_df))}")
    
    df_with_coords = add_coords_to_df(filtered_df)
    df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
    
    if df_with_coords.empty:
        st.info(t('no_data_map', lang))
        return
    
    # Limit markers for performance
    max_markers = 500
    if len(df_with_coords) > max_markers:
        df_with_coords = df_with_coords.sample(n=max_markers, random_state=42)
        st.caption(t('showing_sample', lang, shown=max_markers, total=len(filtered_df)))
    
    # Calculate city rates if normalizing
    if normalize:
        city_counts = df_with_coords['location_city'].value_counts()
        city_rates = {city: (count / get_population(str(city))) * 100000 for city, count in city_counts.items()}
        max_rate = max(city_rates.values()) if city_rates else 1
    else:
        city_rates = {}
        max_rate = 1
    
    m = folium.Map(location=MACEDONIA_CENTER, zoom_start=8, tiles='cartodbpositron')
    
    for _, row in df_with_coords.iterrows():
        location_str = row.get('location_city', '')
        if row.get('location_address'):
            location_str = f"{row.get('location_address')} ({location_str})"
        
        # Size based on rate if normalizing
        if normalize and max_rate > 0:
            rate = city_rates.get(row.get('location_city', ''), 0)
            radius = 6 + (rate / max_rate) * 10
            popup_text = f"<b>{location_str}</b><br>{row.get('crime_type', '')[:40]}<br>Rate: {rate:.1f} per 100k"
        else:
            radius = 6
            popup_text = f"<b>{location_str}</b><br>{row.get('crime_type', 'N/A')[:50]}"
        
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=radius,
            popup=popup_text,
            color='red',
            fill=True,
            fillOpacity=0.7
        ).add_to(m)
    
    st_folium(m, width=800, height=500)
    
    # Quick stats
    col1, col2, col3 = st.columns(3)
    col1.metric(t('cities_label', lang), filtered_df['location_city'].nunique())
    col2.metric(t('total_inc', lang), len(filtered_df))
    
    if normalize:
        total_pop = sum(get_population(str(c)) for c in filtered_df['location_city'].unique())
        rate = (len(filtered_df) / max(total_pop, 1)) * 100000
        col3.metric(t('rate_per_100k', lang), f"{rate:.1f}")
    else:
        days = (pd.to_datetime(filtered_df['crime_date']).max() - pd.to_datetime(filtered_df['crime_date']).min()).days or 1
        col3.metric(t('avg_day', lang), f"{len(filtered_df) / days:.1f}")


def tab_city_comparison(filtered_df, lang='mk'):
    """5. City Comparison Side-by-Side"""
    st.subheader(t('tab_city_compare', lang))
    
    normalize = st.session_state.get('normalize', False)
    
    if filtered_df.empty:
        st.info(t('no_data', lang))
        return
    
    # Get top 10 cities
    city_counts = filtered_df['location_city'].value_counts()
    cities = city_counts.nlargest(20).index.tolist()
    
    if len(cities) < 2:
        st.info("Need at least 2 cities for comparison")
        return
    
    col1, col2 = st.columns(2)
    with col1:
        city1 = st.selectbox(t('select_city1', lang), cities, key="city1")
    with col2:
        city2 = st.selectbox(t('select_city2', lang), cities, key="city2")
    
    df1 = filtered_df[filtered_df['location_city'] == city1]
    df2 = filtered_df[filtered_df['location_city'] == city2]
    
    # Calculate rates if normalizing
    pop1 = get_population(city1)
    pop2 = get_population(city2)
    rate1 = (len(df1) / pop1) * 100000 if pop1 else 0
    rate2 = (len(df2) / pop2) * 100000 if pop2 else 0
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"### {city1}")
        if normalize:
            st.write(f"**{rate1:.1f} {t('per_100k_residents', lang)}** ({len(df1)} {t('crimes', lang)})")
        else:
            st.write(f"**{len(df1)} {t('crimes', lang)}**")
        st.caption(f"{t('population', lang)}: {pop1:,}")
        coords1 = get_coords(city1)
        if coords1:
            m1 = folium.Map(location=coords1, zoom_start=12, tiles='cartodbpositron')
            df1_coords = add_coords_to_df(df1).dropna(subset=['lat', 'lon'])
            for _, row in df1_coords.head(100).iterrows():  # Limit markers
                folium.CircleMarker([row['lat'], row['lon']], radius=8, color='blue', fill=True).add_to(m1)
            st_folium(m1, width=400, height=300)
    
    with col2:
        st.write(f"### {city2}")
        if normalize:
            st.write(f"**{rate2:.1f} {t('per_100k_residents', lang)}** ({len(df2)} {t('crimes', lang)})")
        else:
            st.write(f"**{len(df2)} {t('crimes', lang)}**")
        st.caption(f"{t('population', lang)}: {pop2:,}")
        coords2 = get_coords(city2)
        if coords2:
            m2 = folium.Map(location=coords2, zoom_start=12, tiles='cartodbpositron')
            df2_coords = add_coords_to_df(df2).dropna(subset=['lat', 'lon'])
            for _, row in df2_coords.head(100).iterrows():  # Limit markers
                folium.CircleMarker([row['lat'], row['lon']], radius=8, color='red', fill=True).add_to(m2)
            st_folium(m2, width=400, height=300)


def tab_timeline_map(filtered_df, lang='mk'):
    """6. Timeline + Map"""
    st.subheader(t('tab_timeline', lang))
    
    normalize = st.session_state.get('normalize', False)
    
    if filtered_df.empty or 'crime_date' not in filtered_df.columns:
        st.info(t('no_date_data', lang))
        return
    
    # Show timeline chart
    filtered_df = filtered_df.copy()
    daily_counts = filtered_df.groupby(filtered_df['crime_date'].dt.date).size()
    
    st.line_chart(daily_counts, height=200)
    
    # Select time period
    selected_date = st.slider(
        t('select_date', lang),
        min_value=daily_counts.index.min(),
        max_value=daily_counts.index.max(),
        value=daily_counts.index.min()
    )
    
    # Map showing crimes around selected date
    df_with_coords = add_coords_to_df(filtered_df)
    df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
    
    # Filter to +/- 3 days from selected
    selected = pd.to_datetime(selected_date)
    recent_df = df_with_coords[
        (pd.to_datetime(df_with_coords['crime_date']) >= selected - timedelta(days=3)) &
        (pd.to_datetime(df_with_coords['crime_date']) <= selected + timedelta(days=3))
    ]
    
    # Limit markers
    if len(recent_df) > 200:
        recent_df = recent_df.sample(n=200, random_state=42)
    
    st.write(t('showing_period', lang).format(
        count=len(recent_df),
        start=selected_date - timedelta(days=3),
        end=selected_date + timedelta(days=3)
    ))
    
    m = folium.Map(location=MACEDONIA_CENTER, zoom_start=8, tiles='cartodbpositron')
    
    for _, row in recent_df.iterrows():
        location_str = row.get('location_city', t('na', lang))
        if row.get('location_address'):
            location_str = f"{row.get('location_address')} ({location_str})"
        
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=10,
            popup=f"{row['crime_date']}: {location_str}<br>{row['crime_type'][:40]}",
            color='red',
            fill=True,
            fillOpacity=0.8
        ).add_to(m)
    
    st_folium(m, width=800, height=400)





def tab_crime_types_map(filtered_df, lang='mk'):
    """8. Crime Type Icon Markers Map"""
    st.subheader(t('tab_crime_types', lang))
    
    normalize = st.session_state.get('normalize', False)
    
    df_with_coords = add_coords_to_df(filtered_df)
    df_with_coords = df_with_coords.dropna(subset=['lat', 'lon'])
    
    if df_with_coords.empty:
        st.info(t('no_crime_type_data', lang))
        return
    
    # Limit markers for performance
    max_markers = 500
    if len(df_with_coords) > max_markers:
        df_with_coords = df_with_coords.sample(n=max_markers, random_state=42)
        st.caption(t('showing_sample', lang, shown=max_markers, total=len(filtered_df)))
    
    # Get category counts
    category_counts = {}
    for crime_type in filtered_df['crime_type']:
        _, _, label = get_crime_config(crime_type, lang)
        category_counts[label] = category_counts.get(label, 0) + 1
    
    # Show legend
    st.write(f"**{t('legend', lang)}:**")
    legend_cols = st.columns(min(len(category_counts) + 1, 5))
    for i, (cat, count) in enumerate(sorted(category_counts.items(), key=lambda x: -x[1])):
        with legend_cols[i % 5]:
            st.metric(cat, count)
    
    # Create map with custom icons
    m = folium.Map(location=MACEDONIA_CENTER, zoom_start=8, tiles='cartodbpositron')
    
    # Add custom CSS for icons
    folium_css = """
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        .crime-icon {
            display: flex;
            align-items: center;
            justify-content: center;
            width: 32px;
            height: 32px;
            border-radius: 50%;
            font-size: 18px;
            color: white;
        }
        .drugs { background: purple; }
        .theft { background: orange; }
        .violence { background: red; }
        .traffic { background: blue; }
        .fraud { background: gray; }
        .weapons { background: black; }
        .arson { background: darkorange; }
        .other { background: lightblue; border: 1px solid #ccc; color: #333; }
    </style>
    """
    m.get_root().html.add_child(folium.Element(folium_css))
    
    # Icon map
    icon_map = {
        '🚬': ('fa fa-joint', 'drugs'),
        '💎': ('fa fa-gem', 'theft'),
        '👊': ('fa fa-hand-fist', 'violence'),
        '🚗': ('fa fa-car', 'traffic'),
        '🔫': ('fa fa-crosshairs', 'weapons'),
        '🔥': ('fa fa-fire', 'arson'),
        '🏭': ('fa fa-industry', 'other'),
        '📋': ('fa fa-question', 'other'),
        '❓': ('fa fa-question', 'other'),
    }
    
    for _, row in df_with_coords.iterrows():
        crime_type = row.get('crime_type', '')
        color, icon, category = get_crime_config(crime_type, lang)
        
        # Get icon class from category emoji
        emoji = category[:2]
        icon_class, css_class = icon_map.get(emoji, ('fa fa-circle', 'other'))
        
        # Build location string
        location_str = row.get('location_city', t('na', lang))
        if row.get('location_address'):
            location_str = f"{row.get('location_address')} ({location_str})"
        
        popup_text = f"""
        <div style="min-width: 200px;">
            <h4 style="margin: 0 0 10px 0;">{category}</h4>
            <p style="margin: 5px 0;"><b>{t('crime_type', lang)}:</b> {row.get('crime_type', t('na', lang))[:60]}</p>
            <p style="margin: 5px 0;"><b>{t('location', lang)}:</b> {location_str}</p>
            <p style="margin: 5px 0;"><b>{t('date', lang)}:</b> {row.get('crime_date', t('na', lang))}</p>
            <p style="margin: 5px 0;"><b>{t('outcome', lang)}:</b> {row.get('outcome', t('na', lang)) or t('na', lang)}</p>
        </div>
        """
        
        icon_html = f'<div class="crime-icon {css_class}"><i class="{icon_class}"></i></div>'
        
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=folium.Popup(popup_text, max_width=350),
            icon=folium.DivIcon(
                html=icon_html,
                icon_size=(32, 32),
                icon_anchor=(16, 16),
                class_name='crime-marker'
            )
        ).add_to(m)
    
    st_folium(m, width=800, height=550)


def tab_time_day_of_week(filtered_df, lang='mk'):
    """1. Day of Week Distribution"""
    st.subheader(t('tab_time_day_week', lang))
    
    if filtered_df.empty or 'crime_date' not in filtered_df.columns:
        st.info(t('no_date_data', lang))
        return
    
    # Extract day of week
    filtered_df = filtered_df.copy(deep=True).reset_index(drop=True)
    cols_to_drop = [c for c in filtered_df.columns if c in ['category', 'month', 'day_of_week']]
    if cols_to_drop:
        filtered_df = filtered_df.drop(columns=cols_to_drop)
    filtered_df['day_of_week'] = pd.to_datetime(filtered_df['crime_date']).dt.day_name()
    
    # Define day order
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    filtered_df['day_of_week'] = pd.Categorical(filtered_df['day_of_week'], categories=day_order, ordered=True)
    
    # Count by day
    day_counts = filtered_df['day_of_week'].value_counts().reindex(day_order)
    
    # Translate day labels for chart
    day_labels = {
        'Monday': 'Понеделник',
        'Tuesday': 'Вторник',
        'Wednesday': 'Среда',
        'Thursday': 'Четврток',
        'Friday': 'Петок',
        'Saturday': 'Сабота',
        'Sunday': 'Недела'
    }
    day_counts.index = [day_labels.get(d, d) for d in day_counts.index]
    
    st.bar_chart(day_counts, use_container_width=True, height=400)
    
    # Show statistics
    st.divider()
    col1, col2, col3 = st.columns(3)
    col1.metric(t('most_common_day', lang), day_counts.idxmax() if not day_counts.empty else "N/A")
    col1.metric(t('day_count', lang), day_counts.max() if not day_counts.empty else 0)
    col2.metric(t('least_common_day', lang), day_counts.idxmin() if not day_counts.empty else "N/A")
    col2.metric(t('day_count', lang), day_counts.min() if not day_counts.empty else 0)
    col3.metric(t('avg_per_day', lang), f"{day_counts.mean():.1f}")


def tab_time_monthly(filtered_df, lang='mk'):
    """2. Monthly Trend"""
    st.subheader(t('tab_monthly', lang))
    
    if filtered_df.empty or 'crime_date' not in filtered_df.columns:
        st.info(t('no_date_data', lang))
        return
    
    # Extract month and year
    filtered_df = filtered_df.copy(deep=True).reset_index(drop=True)
    cols_to_drop = [c for c in filtered_df.columns if c in ['category', 'month', 'day_of_week']]
    if cols_to_drop:
        filtered_df = filtered_df.drop(columns=cols_to_drop)
    filtered_df['crime_date'] = pd.to_datetime(filtered_df['crime_date'])
    filtered_df['month'] = filtered_df['crime_date'].dt.to_period('M')
    
    # Count by month
    monthly_counts = filtered_df.groupby('month').size()
    
    # Format labels
    monthly_labels = {m: m.strftime('%Y-%m') for m in monthly_counts.index}
    
    st.line_chart(monthly_counts, use_container_width=True, height=400)
    
    # Show statistics
    st.divider()
    st.write(f"**{t('trend_label', lang)}:** {monthly_counts.iloc[-3:].mean() - monthly_counts.iloc[0]}" + 
             (" 📈" if monthly_counts.iloc[-3:].mean() > monthly_counts.iloc[0] else " 📉" if monthly_counts.iloc[-3:].mean() < monthly_counts.iloc[0] else " ➡️"))


def tab_demographic_gender(filtered_df, lang='mk'):
    """7. Gender Distribution by Crime Type"""
    st.subheader(t('tab_gender_type', lang))
    
    if filtered_df.empty or 'perpetrator_gender' not in filtered_df.columns:
        st.info(t('no_data', lang))
        return
    
    # Work with fresh copy, ensure no residual category columns
    filtered_df = filtered_df.copy(deep=True).reset_index(drop=True)
    cols_to_drop = [c for c in filtered_df.columns if 'category' in c.lower()]
    if cols_to_drop:
        filtered_df = filtered_df.drop(columns=cols_to_drop)
    categories = [get_crime_config(crime_type, lang)[2] for crime_type in filtered_df['crime_type']]
    filtered_df['category'] = categories
    
    
    # Create pivot table: rows = categories, columns = gender
    gender_counts = filtered_df.pivot_table(
        index='category',
        columns='perpetrator_gender',
        aggfunc='count',
        values='id',
        fill_value=0
    )
    
    # Translate column labels for chart
    gender_cols = {'male': 'Машки', 'female': 'Женски', 'unknown': 'Непознат', 'mixed': 'Мешано'}
    gender_counts.columns = [gender_cols.get(c, c) for c in gender_counts.columns]
    
    # Show stacked bar chart
    st.bar_chart(gender_counts, use_container_width=True, height=500)
    
    # Show percentage breakdown
    st.divider()
    st.subheader(t('percentage_by_gender', lang))
    pct = gender_counts.div(gender_counts.sum(axis=1), axis=0) * 100
    pct = pct.fillna(0)
    st.dataframe(pct.style.format({c: '{:.1f}%' for c in pct.columns}).hide(axis="index"))


def tab_demographic_count(filtered_df, lang='mk'):
    """9. Single vs Multiple Perpetrators"""
    st.subheader(t('tab_perp_count', lang))
    
    if filtered_df.empty or 'perpetrator_count' not in filtered_df.columns:
        st.info(t('no_data', lang))
        return
    
    # Work with fresh copy
    filtered_df = filtered_df.copy(deep=True).reset_index(drop=True)
    cols_to_drop = [c for c in filtered_df.columns if 'category' in c.lower()]
    if cols_to_drop:
        filtered_df = filtered_df.drop(columns=cols_to_drop)
    filtered_df['category'] = [get_crime_config(str(crime_type), lang)[2] for crime_type in filtered_df['crime_type']]
    
    valid_counts = filtered_df[filtered_df['perpetrator_count'].isin(['single', 'multiple'])]
    
    if valid_counts.empty:
        st.info(t('no_data', lang))
        return
    
    # Create counts
    perp_counts = valid_counts['perpetrator_count'].value_counts()
    perp_counts.index = [c.upper() for c in perp_counts.index]
    
    # Show pie chart
    col1, col2 = st.columns([2, 1])
    with col1:
        # Stacked bar by category
        perp_category = valid_counts.pivot_table(
            index='perpetrator_count',
            columns='category',
            aggfunc='count',
            values='id',
            fill_value=0
        )
        perp_index = {'single': 'Еден сторител', 'multiple': 'Повеќе сторители'}
        perp_columns = {c: c for c in perp_category.columns}
        perp_category.index = [perp_index.get(c, c) for c in perp_category.index]
        st.bar_chart(perp_category, use_container_width=True, height=400)
    
    with col2:
        st.write(f"### {t('distribution', lang)}")
        total = len(valid_counts)
        for idx, count in perp_counts.items():
                pct = (count / total) * 100
                st.write(f"**{idx.upper()}**: {count} ({pct:.1f}%)")



def tab_comparison_rate(filtered_df, lang='mk'):
    """4. City vs Population (Rate per 100k)"""
    st.subheader(t('tab_rate', lang))
    
    if filtered_df.empty:
        st.info(t('no_data', lang))
        return
    
    # Calculate rates
    city_counts = filtered_df['location_city'].value_counts()
    city_pops = city_counts.index.map(lambda x: get_population(str(x) if pd.notna(x) else ''))
    
    # Filter to cities with population data
    valid = city_pops > 0
    city_rate_data = pd.DataFrame({
        'incidents': city_counts[valid],
        'population': city_pops[valid]
    })
    
    city_rate_data['rate_per_100k'] = (city_rate_data['incidents'] / city_rate_data['population']) * 100000
    
    # Sort by rate
    city_rate_data = city_rate_data.sort_values('rate_per_100k', ascending=True)
    
    # Show bar chart
    col1, col2 = st.columns(2)
    
    with col1:
        st.write(f"**{t('rate_per_100k_label', lang)}**")
        st.bar_chart(city_rate_data['rate_per_100k'], use_container_width=True, height=500)
    
    with col2:
        st.write(f"**{t('total_incidents_label', lang)}**")
        st.bar_chart(city_rate_data['incidents'], use_container_width=True, height=500)
    
    # Show table
    st.divider()
    st.write(f"### {t('detailed_stats', lang)}")
    display_cols = ['incidents', 'population', 'rate_per_100k']
    display_df = city_rate_data[display_cols].round(2).reset_index()
    display_df.columns = [t('city_col', lang), t('incidents_col', lang), t('population_col', lang), t('rate_col', lang)]
    st.dataframe(display_df, hide_index=True, height=400)


def tab_comparison_trends(filtered_df, lang='mk'):
    """5. Crime Rate Comparison Over Time"""
    st.subheader(t('tab_trends', lang))
    
    if filtered_df.empty:
        st.info(t('no_data', lang))
        return
    
    # Work with fresh copy
    filtered_df = filtered_df.copy(deep=True).reset_index(drop=True)
    cols_to_drop = [c for c in filtered_df.columns if c in ['category', 'month', 'day_of_week']]
    if cols_to_drop:
        filtered_df = filtered_df.drop(columns=cols_to_drop)
    filtered_df['crime_date'] = pd.to_datetime(filtered_df['crime_date'])
    filtered_df['category'] = [get_crime_config(crime_type, lang)[2] for crime_type in filtered_df['crime_type']]
    
    # Group by month and category
    filtered_df['month'] = filtered_df['crime_date'].dt.to_period('M')
    monthly_cat = filtered_df.groupby(['month', 'category']).size().reset_index(name='count')
    
    # Get top categories
    top_cats = monthly_cat['category'].value_counts().nlargest(5).index.tolist()
    monthly_cat = monthly_cat[monthly_cat['category'].isin(top_cats)]
    
    # Pivoted data for line chart
    trend_data = monthly_cat.pivot(index='month', columns='category', values='count')
    
    st.line_chart(trend_data, use_container_width=True, height=500)
    
    # Show legend
    st.divider()
    st.write(f"### {t('top_n_categories_label', lang, n=len(top_cats))}")
    cols = st.columns(3)
    for i, cat in enumerate(top_cats):
        with cols[i % 3]:
            total = trend_data[cat].sum() if cat in trend_data.columns else 0
            st.metric(cat, total)



def render_analytics():
    """Main analytics page renderer."""
    
    # Initialize session state defaults
    if 'lang' not in st.session_state:
        st.session_state['lang'] = 'mk'
    if 'normalize' not in st.session_state:
        st.session_state['normalize'] = False
    
    lang = st.session_state['lang']
    
    # Language selector - must be at top level to trigger re-render
    st.sidebar.subheader("🌐 Language / Јазик")
    langs = get_available_languages()
    current_lang_idx = langs.index(lang) if lang in langs else 0
    selected_lang = st.sidebar.selectbox(
        "Select language",
        langs,
        index=current_lang_idx,
        format_func=get_language_name,
        key="lang_selector"
    )
    st.session_state['lang'] = selected_lang
    lang = selected_lang
    
    # Main title
    st.title(t('app_title', lang))
    st.caption(t('app_subtitle', lang))
    
    # Get filtered data
    filtered_df = create_filters()
    
    if filtered_df.empty:
        st.warning(t('no_data', lang))
        return
    
    # Show statistics in sidebar
    st.sidebar.divider()
    st.sidebar.subheader(t('statistics', lang))
    st.sidebar.metric(t('total_incidents', lang), len(filtered_df))
    
    if 'crime_date' in filtered_df.columns:
        date_min = filtered_df['crime_date'].min()
        date_max = filtered_df['crime_date'].max()
        if pd.notna(date_min) and pd.notna(date_max):
            st.sidebar.caption(f"{t('date_range_data', lang)}: {date_min.strftime('%Y-%m-%d')} - {date_max.strftime('%Y-%m-%d')}")
    
    st.sidebar.metric(t('cities_covered', lang), filtered_df['location_city'].nunique())
    
    # Tabs for all visualizations
    tabs = st.tabs([
        t('tab_heatmap', lang),
        t('tab_bubble', lang),
        t('tab_clusters', lang),
        t('tab_map_filters', lang),
        t('tab_city_compare', lang),
        t('tab_timeline', lang),
        t('tab_time_day_week', lang),
        t('tab_monthly', lang),
        t('tab_trends', lang),
        t('tab_gender_type', lang),
        t('tab_perp_count', lang),
        t('tab_crime_types', lang),
    ])
    
    with tabs[0]:
        tab_heatmap(filtered_df, lang)
    with tabs[1]:
        tab_bubble_map(filtered_df, lang)
    with tabs[2]:
        tab_cluster_pins(filtered_df, lang)
    with tabs[3]:
        tab_map_with_filters(filtered_df, lang)
    with tabs[4]:
        tab_city_comparison(filtered_df, lang)
    with tabs[5]:
        tab_timeline_map(filtered_df, lang)
    with tabs[6]:
        tab_time_day_of_week(filtered_df, lang)
    with tabs[7]:
        tab_time_monthly(filtered_df, lang)
    with tabs[8]:
        tab_comparison_trends(filtered_df, lang)
    with tabs[9]:
        tab_demographic_gender(filtered_df, lang)
    with tabs[10]:
        tab_demographic_count(filtered_df, lang)
    with tabs[11]:
        tab_crime_types_map(filtered_df, lang)


if __name__ == "__main__":
    render_analytics()
