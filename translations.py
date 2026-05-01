"""
Multi-language support for MVR Analytics.
Currently supports: Macedonian (mk), English (en)
"""

# Translation dictionary
TRANSLATIONS = {
    'mk': {
        # Header
        'app_title': '📊 Аналитика на Криминални Билтени',
        'app_subtitle': 'MVR Република Северна Македонија',
        
        # Filters
        'filters': '🎛️ Филтри',
        'date_range': 'Период',
        'crime_types': 'Тип на кривично дело',
        'cities': 'Градови',
        'all_cities': 'Сите',
        'all': 'Сите',
        'gender': 'Пол',
        'all_genders': 'Сите',
        'male': 'Машки',
        'female': 'Женски',
        'normalize_pop': '📊 Нормализирај по глава на жители',
        'apply_filters': 'Примени филтри',
        'clear_filters': 'Исчисти филтри',
        
        # Statistics
        'statistics': '📈 Статистика',
        'total_incidents': 'Вкупни случаи',
        'date_range_data': 'Период на податоци',
        'cities_covered': 'Опфатени градови',
        
        # Tabs
        'tab_heatmap': '1. 🔥 Топлинска мапа',
        'tab_bubble': '2. ⭕ Меурчиња',
        'tab_clusters': '3. 📍 Кластери',
        'tab_map_filters': '4. 🗺️ Мапа+Филтри',
        'tab_city_compare': '5. 🏙️ Споредба на градови',
        'tab_timeline': '6. 🕰️ Временска линија',
        'tab_time_day_week': '9. 🕐 Неделен распоред',
        'tab_monthly': '10. 📊 Месечен тренд',
        'tab_rate': '12. 📈 Стапка по град',
        'tab_trends': '13. 📉 Трендови по категорија',
        'tab_gender_type': '14. 👥 Дистрибуција по пол',
        'tab_perp_count': '16. 👤 Број на сторители',
        'tab_crime_types': '17. 🏷️ По тип на дело',
        
        # Heatmap
        'heatmap_caption_normalized': 'Црвени области = повисока стапка на криминал (на 100к жители)',
        'heatmap_caption_raw': 'Црвени области = повисока концентрација на криминал',
        'no_geocoded': 'Нема геокодирани податоци за топлинска мапа',
        
        # Bubble map
        'bubble_caption_normalized': 'Големина на меур = криминал на 100,000 жители',
        'bubble_caption_raw': 'Големина на меур = број на кривични дела во тој град',
        
        # Clusters
        'cluster_caption': 'Ознаките се групираат кога сте зумирани',
        'cluster_caption_normalized': 'Ознаки обоени според стапка на криминал на 100к жители',
        
        # Map with filters
        'showing_incidents': 'Прикажани {count} случаи врз основа на вашите филтри',
        'showing_sample': '⚡ Прикажани {shown} од {total} случаи',
        'no_data_map': 'Нема геокодирани податоци',
        
        # Quick stats
        'cities_label': 'Градови',
        'total_inc': 'Вкупни',
        'avg_day': 'Просек/Ден',
        'rate_per_100k': 'Стапка (на 100к)',
        
        # City comparison
        'city_comparison': 'Споредба на два града',
        'select_city1': 'Избери прв ГРАД',
        'select_city2': 'Избери втор ГРАД',
        'population': 'Население',
        'per_100k_residents': 'на 100к жители',
        'crimes': 'кривични дела',
        'avg_age': 'Просечна возраст',
        'na': 'N/A',
        
        # Timeline
        'timeline_caption': 'Време со мапа',
        'no_date_data': 'Нема податоци за датум',
        'select_date': 'Избери датум за истакнување на мапа',
        'showing_period': 'Прикажани {count} случаи од {start} до {end}',
        'per_day': 'на ден',
        
        # Categories
        'category_breakdown': 'Категории на кривични дела',
        'no_category_data': 'Нема податоци',
        'no_age_data': 'Нема податоци за возраст',
        
        # New tab labels
        'most_common_day': 'Најчест ден',
        'least_common_day': 'Најредок ден',
        'avg_per_day': 'Просек по ден',
        'day_count': 'Број',
        'trend_label': 'Тренд',
        'percentage_by_gender': 'Процент по пол',
        'distribution': 'Дистрибуција',
        'age_distribution': 'Распределба на возраст',
        'mean_age': 'Просечна возраст',
        'median_age': 'Медијана',
        'min_age': 'Мин. возраст',
        'max_age': 'Макс. возраст',
        'rate_per_100k_label': 'Стапка на 100к жители',
        'total_incidents_label': 'Вкупни инциденти',
        'detailed_stats': 'Детални статистики',
        'city_col': 'Град',
        'incidents_col': 'Инциденти',
        'population_col': 'Население',
        'rate_col': 'Стапка на 100к',
        'top_n_categories_label': 'Топ {n} категории',
        'need_more_categories': 'Потребни се повеќе категории за анализа',
        'top_positive_corr': 'Највисоки позитивни корелации',
        'no_positive_corr': 'Нема најдени позитивни корелации',
        'not_enough_corr_data': 'Нема доволно податоци за корелациона анализа',
        

        # App navigation
        'app_page_title': '📋 Билтени за криминални булетини',
        'nav_label': 'Навигација',
        'nav_dashboard': '📊 Контролна табла',
        'nav_bulletins': '📄 Билтени',
        'nav_incidents': '🚨 Инциденти',
        'nav_analytics': '🗺️ Аналитика',
        'nav_errors': '❌ Грешки',
        'nav_search': '🔍 Пребарување',
        'all_bulletins': 'Сите билтени',
        'crime_incidents': 'Криминални инциденти',
        'processing_errors': 'Грешки при обработка',
        'search_label': 'Пребарување',
        'search_crimes': 'Пребарувај инциденти',
        'filter_status': 'Филтрирај по статус',
        'select_city': 'Град',
        'select_gender': 'Пол',
        'select_details': 'Детали',
        'type_label': 'Тип',
        'date_label': 'Датум',
        'city_label': 'Град',
        'gender_label': 'Пол',
        'perpetrators_label': 'Сторители',
        'original_text': 'Оригинален текст',
        'no_bulletins': 'Нема билтени. Стартувајте sync прво!',
        'no_data_yet': 'Нема податоци. Стартувајте sync прво!',
        'run_sync_first': 'Стартувајте sync за да се вчитаат податоците',
        'no_errors': 'Нема грешки!',
        'results': 'резултати',
        'incidents_label': 'инциденти',
        'last_updated': 'Последно ажурирање',
        'sync_label': '🔄 Синхронизација',
        'run_pipeline': '▶️ Синхронизирај',
        'quick_stats': 'Статистика',
        'bulletins_count': 'билтени',
        'total_inc_count': 'инциденти',
        'error_count': 'грешки',

        'top_categories': 'Топ категории',
        'crime_trends': 'Трендови на кривични дела по категорија',
        'top_cities': 'Топ {n} градови',
        
        # Crime types map
        'crime_types_map': 'Мапа по тип на кривично дело',
        'legend': 'Легенда',
        'no_crime_type_data': 'Нема геокодирани податоци за тип на дело',
        
        # Crime categories (for display)
        'cat_drugs': '🚬 Наркотици',
        'cat_theft': '💎 Кражба',
        'cat_violence': '👊 Насилство',
        'cat_traffic': '🚗 Сообраќајни несреќи',
        'cat_weapons': '🔫 Оружје',
        'cat_arson': '🔥 Пожари',
        'cat_other': '📋 Друго',
        
        # Common
        'loading': 'Вчитување...',
        'error': 'Грешка',
        'no_data': 'Нема податоци',
        'incidents': 'случаи',
        'date': 'Датум',
        'crime_type': 'Тип на дело',
        'location': 'Локација',
        'gender': 'Пол',
        'outcome': 'Исход',
    },
    
    'en': {
        # Header
        'app_title': '📊 Crime Bulletin Analytics',
        'app_subtitle': 'MVR Republic of North Macedonia',
        
        # Filters
        'filters': '🎛️ Filters',
        'date_range': 'Date Range',
        'crime_types': 'Crime Type',
        'cities': 'Cities',
        'all_cities': 'All',
        'all': 'All',
        'gender': 'Gender',
        'all_genders': 'All',
        'male': 'Male',
        'female': 'Female',
        'normalize_pop': '📊 Normalize by population',
        'apply_filters': 'Apply Filters',
        'clear_filters': 'Clear Filters',
        
        # Statistics
        'statistics': '📈 Statistics',
        'total_incidents': 'Total Incidents',
        'date_range_data': 'Data Period',
        'cities_covered': 'Cities Covered',
        
        # Tabs
        'tab_heatmap': '1. 🔥 Heatmap',
        'tab_bubble': '2. ⭕ Bubbles',
        'tab_clusters': '3. 📍 Clusters',
        'tab_map_filters': '4. 🗺️ Map+Filters',
        'tab_city_compare': '5. 🏙️ City Compare',
        'tab_timeline': '6. 🕰️ Timeline+Map',
        'tab_time_day_week': '9. 🕐 Weekly Schedule',
        'tab_monthly': '10. 📊 Monthly Trend',
        'tab_rate': '12. 📈 Rate by City',
        'tab_trends': '13. 📉 Category Trends',
        'tab_gender_type': '14. 👥 Gender by Type',
        'tab_perp_count': '16. 👤 Perpetrator Count',
        'tab_crime_types': '17. 🏷️ By Crime Type',
        
        # Heatmap
        'heatmap_caption_normalized': 'Red areas = higher crime rate (per 100k residents)',
        'heatmap_caption_raw': 'Red areas = higher crime concentration',
        'no_geocoded': 'No geocoded data available for heatmap',
        
        # Bubble map
        'bubble_caption_normalized': 'Circle size = crimes per 100,000 residents',
        'bubble_caption_raw': 'Circle size = number of crimes in that city',
        
        # Clusters
        'cluster_caption': 'Markers cluster together when zoomed out',
        'cluster_caption_normalized': 'Markers colored by crime rate per 100k residents',
        
        # Map with filters
        'showing_incidents': 'Showing {count} incidents based on your filters',
        'showing_sample': '⚡ Showing {shown} of {total} incidents',
        'no_data_map': 'No geocoded data available',
        
        # Quick stats
        'cities_label': 'Cities',
        'total_inc': 'Total',
        'avg_day': 'Avg/Day',
        'rate_per_100k': 'Rate (per 100k)',
        
        # City comparison
        'city_comparison': 'City Comparison',
        'select_city1': 'Select FIRST City',
        'select_city2': 'Select SECOND City',
        'population': 'Population',
        'per_100k_residents': 'per 100k residents',
        'crimes': 'crimes',
        'avg_age': 'Average age',
        'na': 'N/A',
        
        # Timeline
        'timeline_caption': 'Timeline with Map',
        'no_date_data': 'No date data available',
        'select_date': 'Select date to highlight on map',
        'showing_period': 'Showing {count} incidents from {start} to {end}',
        'per_day': 'per day',
        
        # Categories
        'category_breakdown': 'Crime Category Breakdown',
        'no_category_data': 'No data available',
        'top_categories': 'Top Categories',
        'crime_trends': 'Crime Trends by Category Over Time',
        'top_cities': 'Top {n} Cities',
        
        # Crime types map
        'crime_types_map': 'Crime Types Map',
        'legend': 'Legend',
        'no_crime_type_data': 'No geocoded data for crime types',
        
        # Crime categories
        'cat_drugs': '🚬 Drugs',
        'cat_theft': '💎 Theft',
        'cat_violence': '👊 Violence',
        'cat_traffic': '🚗 Traffic Accidents',
        'cat_weapons': '🔫 Weapons',
        'cat_arson': '🔥 Arson',
        'cat_other': '📋 Other',
        
        # Common
        'loading': 'Loading...',
        'error': 'Error',
        'no_data': 'No data',
        'no_age_data': 'No age data available',
        
        # New tab labels
        'most_common_day': 'Most Common Day',
        'least_common_day': 'Least Common Day',
        'avg_per_day': 'Avg/Day',
        'day_count': 'Count',
        'trend_label': 'Trend',
        'percentage_by_gender': 'Percentage by Gender',
        'distribution': 'Distribution',
        'age_distribution': 'Age Distribution',
        'mean_age': 'Mean Age',
        'median_age': 'Median Age',
        'min_age': 'Min Age',
        'max_age': 'Max Age',
        'rate_per_100k_label': 'Rate per 100k Residents',
        'total_incidents_label': 'Total Incidents',
        'detailed_stats': 'Detailed Stats',
        'city_col': 'City',
        'incidents_col': 'Incidents',
        'population_col': 'Population',
        'rate_col': 'Rate per 100k',
        'top_n_categories_label': 'Top {n} Categories',
        'need_more_categories': 'Need more crime categories for correlation analysis',
        'top_positive_corr': 'Top Positive Correlations',
        'no_positive_corr': 'No positive correlations found',
        'not_enough_corr_data': 'Not enough data for correlation analysis',
        

        # App navigation
        'app_page_title': '📋 Crime Bulletin Explorer',
        'nav_label': 'Navigation',
        'nav_dashboard': '📊 Dashboard',
        'nav_bulletins': '📄 Bulletins',
        'nav_incidents': '🚨 Incidents',
        'nav_analytics': '🗺️ Analytics',
        'nav_errors': '❌ Errors',
        'nav_search': '🔍 Search',
        'all_bulletins': 'All Bulletins',
        'crime_incidents': 'Crime Incidents',
        'processing_errors': 'Processing Errors',
        'search_label': 'Search',
        'search_crimes': 'Search crimes',
        'filter_status': 'Filter by status',
        'select_city': 'City',
        'select_gender': 'Gender',
        'select_details': 'Details',
        'type_label': 'Type',
        'date_label': 'Date',
        'city_label': 'City',
        'gender_label': 'Gender',
        'perpetrators_label': 'Perpetrators',
        'original_text': 'Original Text',
        'no_bulletins': 'No bulletins. Run sync first!',
        'no_data_yet': 'No data yet. Run sync first!',
        'run_sync_first': 'Run sync to load data',
        'no_errors': 'No errors!',
        'results': 'results',
        'incidents_label': 'incidents',
        'last_updated': 'Last updated',
        'sync_label': '🔄 Sync',
        'run_pipeline': '▶️ Run Pipeline Sync',
        'quick_stats': 'Quick Stats',
        'bulletins_count': 'bulletins',
        'total_inc_count': 'incidents',
        'error_count': 'errors',

        'incidents': 'incidents',
        'date': 'Date',
        'crime_type': 'Crime Type',
        'location': 'Location',
        'gender': 'Gender',
        'outcome': 'Outcome',
    }
}



def t(key, lang='mk', **kwargs):
    """
    Get translation for a key.
    
    Args:
        key: Translation key
        lang: Language code ('mk' or 'en')
        **kwargs: Format arguments for the translation string
    
    Returns:
        Translated string with optional formatting
    """
    translations = TRANSLATIONS.get(lang, TRANSLATIONS['mk'])
    text = translations.get(key, TRANSLATIONS['mk'].get(key, key))
    
    if kwargs:
        try:
            return text.format(**kwargs)
        except (KeyError, ValueError):
            return text
    
    return text


def get_available_languages():
    """Return list of available language codes."""
    return list(TRANSLATIONS.keys())


def get_language_name(code):
    """Get human-readable name for language code."""
    names = {
        'mk': '🇲🇰 Македонски',
        'en': '🇬🇧 English'
    }
    return names.get(code, code)
