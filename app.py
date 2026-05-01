"""
Streamlit UI for MVR Crime Bulletin Explorer
"""
import streamlit as st
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import os
import sys

# Add project dir to path for imports
sys.path.insert(0, os.path.dirname(__file__))

st.set_page_config(page_title="MVR Билтени", page_icon="📋", layout="wide")


@st.cache_resource
def get_engine():
    from config import get_settings
    settings = get_settings()
    return create_engine(
        settings.database_url,
        connect_args={"check_same_thread": False} if "sqlite" in settings.database_url else {},
    )


def get_bulletins():
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql("SELECT * FROM bulletins ORDER BY publication_date DESC", conn)


def get_incidents():
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql("SELECT * FROM crime_incidents ORDER BY crime_date DESC", conn)


def get_stats():
    engine = get_engine()
    with engine.connect() as conn:
        status_counts = pd.read_sql("SELECT status, COUNT(*) as c FROM bulletins GROUP BY status", conn)
        city_counts = pd.read_sql("SELECT location_city, COUNT(*) as c FROM crime_incidents GROUP BY location_city ORDER BY c DESC LIMIT 10", conn)
        total_bulletins = pd.read_sql("SELECT COUNT(*) as c FROM bulletins", conn).iloc[0,0]
        total_incidents = pd.read_sql("SELECT COUNT(*) as c FROM crime_incidents", conn).iloc[0,0]
        total_errors = pd.read_sql("SELECT COUNT(*) as c FROM processing_errors", conn).iloc[0,0]
        return status_counts, city_counts, total_bulletins, total_incidents, total_errors


def run_sync_streaming():
    """Run pipeline with real-time streaming output."""
    from translations import t
    lang = st.session_state.get('lang', 'mk')
    
    import subprocess
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    log_area = st.empty()
    log_lines = []
    total_bulletins = [0]
    
    def log(msg, style="info"):
        prefix_map = {"success": "✅ ", "error": "❌ ", "progress": "📊 ", "step": "  → "}
        prefix = prefix_map.get(style, "")
        log_lines.append(f"{prefix}{msg}")
        log_area.code("\n".join(log_lines[-40:]), language="bash")
    
    try:
        from config import get_settings
        from database import init_database
        
        settings = get_settings()
        log("🚀 Покренување синхронизација...")
        
        venv_python = os.path.join(os.path.dirname(__file__), '.venv', 'bin', 'python')
        
        process = subprocess.Popen(
            [venv_python, '-c', 'import sys; sys.path.insert(0, "."); import asyncio; from pipeline import run_pipeline_once; asyncio.run(run_pipeline_once())'],
            cwd=os.path.dirname(__file__),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        for line in process.stdout:
            line = line.strip()
            if line:
                if line.startswith("STEP|"):
                    parts = line.split("|")
                    if len(parts) >= 3:
                        action = parts[1]
                        detail = "|".join(parts[2:])
                        if action == "START":
                            log(f"Покренување...", "progress")
                        elif action == "INDEX":
                            log(f"Најдени {detail} билтени", "progress")
                        elif action == "PROGRESS":
                            log(f"Обработка {detail}", "step")
                        elif action == "SAVED":
                            log(f"✅ {detail}", "success")
                        elif action == "SKIP":
                            log(f"⏭️ Веќе обработен", "step")
                        elif action == "ERROR":
                            log(f"❌ {detail}", "error")
                        elif action == "FETCH":
                            log(f"Преземање податоци...", "step")
                        elif action == "EXTRACT":
                            log(f"Испраќање до LLM...", "step")
                        elif action == "PARSE":
                            log(f"LLM обработка...", "step")
                else:
                    log(line)
        
        process.wait()
        
        engine = get_engine()
        with engine.connect() as conn:
            processed = pd.read_sql("SELECT COUNT(*) as c FROM bulletins WHERE status='PROCESSED'", conn).iloc[0,0]
            total = pd.read_sql("SELECT COUNT(*) as c FROM bulletins", conn).iloc[0,0]
            incidents_count = pd.read_sql("SELECT COUNT(*) as c FROM crime_incidents", conn).iloc[0,0]
            errors_count = pd.read_sql("SELECT COUNT(*) as c FROM processing_errors", conn).iloc[0,0]
        
        progress_bar.progress(100)
        log("", "info")
        log("=" * 50, "success")
        log("✅ СИНХРОНИЗАЦИЈА Е ЗАВРШЕНА!", "success")
        log("=" * 50, "success")
        log(f"📊 Билтени: {processed}/{total}", "progress")
        log(f"📊 Инциденти: {incidents_count}", "progress")
        log(f"📊 Грешки: {errors_count}", "progress")
        
        st.cache_data.clear()
        status_text.text("✅ Завршено!")
        return True
        
    except Exception as e:
        import traceback
        log(f"\n❌ ГРЕШКА: {e}", "error")
        log(traceback.format_exc()[:500], "error")
        progress_bar.progress(0)
        return None


def main():
    from translations import t, get_language_name, get_available_languages
    
    # Language selector
    if 'lang' not in st.session_state:
        st.session_state['lang'] = 'mk'
    
    lang = st.session_state['lang']
    
    with st.sidebar:
        st.subheader("🌐 Јазик / Language")
        langs = get_available_languages()
        idx = langs.index(lang) if lang in langs else 0
        st.session_state['lang'] = st.selectbox(
            "Select",
            langs,
            index=idx,
            format_func=get_language_name,
            key="lang_selector_app"
        )
        lang = st.session_state['lang']
        
        st.divider()
        st.subheader(t('sync_label', lang))
        if st.button(t('run_pipeline', lang), type="primary", use_container_width=True):
            run_sync_streaming()
            st.rerun()
        
        st.divider()
        st.write(f"**{t('quick_stats', lang)}:**")
        try:
            _, _, total_b, total_i, total_e = get_stats()
            st.write(f"• {total_b} {t('bulletins_count', lang)}")
            st.write(f"• {total_i} {t('total_inc_count', lang)}")
            st.write(f"• {total_e} {t('error_count', lang)}")
        except:
            st.write(t('run_sync_first', lang))
    
    st.title(t('app_page_title', lang))
    
    # Navigation
    nav_pages = [
        t('nav_dashboard', lang),
        t('nav_bulletins', lang),
        t('nav_incidents', lang),
        t('nav_analytics', lang),
        t('nav_errors', lang),
        t('nav_search', lang),
    ]
    page = st.radio(t('nav_label', lang), nav_pages, horizontal=True)
    
    if page == t('nav_dashboard', lang):
        try:
            status_counts, city_counts, total_b, total_i, total_e = get_stats()
            col1, col2, col3, col4 = st.columns(4)
            col1.metric(t('nav_bulletins', lang).lstrip('📄 '), total_b)
            col2.metric('Обработени', status_counts[status_counts['status']=='PROCESSED']['c'].sum() if 'PROCESSED' in status_counts['status'].values else 0)
            col3.metric(t('nav_incidents', lang).lstrip('🚨 '), total_i)
            col4.metric(t('nav_errors', lang).lstrip('❌ '), total_e)
            
            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                st.subheader(t('nav_incidents', lang) + " по " + t('city_label', lang))
                if not city_counts.empty:
                    st.bar_chart(city_counts.set_index('location_city'))
            with col2:
                st.subheader(t('nav_bulletins', lang) + " — статус")
                if not status_counts.empty:
                    st.bar_chart(status_counts.set_index('status'))
        except Exception as e:
            st.info(t('no_data_yet', lang))
    
    elif page == t('nav_bulletins', lang):
        st.subheader(t('all_bulletins', lang))
        bulletins = get_bulletins()
        if not bulletins.empty:
            status = st.selectbox(t('filter_status', lang), ["Сите", "PROCESSED", "PENDING", "ERROR"])
            if status != "Сите":
                bulletins = bulletins[bulletins['status'] == status]
            st.dataframe(bulletins[['id', 'publication_date', 'status', 'processed_at']], hide_index=True, height=400)
        else:
            st.info(t('no_bulletins', lang))
    
    elif page == t('nav_incidents', lang):
        st.subheader(t('crime_incidents', lang))
        incidents = get_incidents()
        if not incidents.empty:
            cities = [t('all_cities', lang)] + sorted(incidents['location_city'].dropna().unique().tolist())
            col1, col2 = st.columns(2)
            with col1:
                city = st.selectbox(t('select_city', lang), cities)
            with col2:
                gender = st.selectbox(t('select_gender', lang), [t('all_cities', lang), t('male', lang), t('female', lang), "mixed", "unknown"])
            
            if city != t('all_cities', lang):
                incidents = incidents[incidents['location_city'] == city]
            if gender != t('all_cities', lang):
                incidents = incidents[incidents['perpetrator_gender'] == gender]
            
            st.write(f"**{len(incidents)}** {t('incidents_label', lang)}")
            display = incidents[['crime_type', 'crime_date', 'location_city', 'perpetrator_gender']].copy()
            display['crime_type'] = display['crime_type'].str[:50]
            
            # Translate perpetrator_gender labels in display
            gender_map = {'male': 'Машки', 'female': 'Женски', 'mixed': 'Мешано', 'unknown': 'Непознат'}
            display['perpetrator_gender'] = display['perpetrator_gender'].map(gender_map).fillna(display['perpetrator_gender'])
            
            st.dataframe(display, hide_index=True, height=400)
            
            if not incidents.empty:
                st.divider()
                sel = st.selectbox(t('select_details', lang), incidents['id'].tolist())
                inc = incidents[incidents['id'] == sel].iloc[0]
                col1, col2 = st.columns(2)
                with col1:
                    g = gender_map.get(inc['perpetrator_gender'], inc['perpetrator_gender'])
                    st.write(f"**{t('type_label', lang)}:** {inc['crime_type']}")
                    st.write(f"**{t('date_label', lang)}:** {inc['crime_date']}")
                    st.write(f"**{t('city_label', lang)}:** {inc['location_city']}")
                with col2:
                    st.write(f"**{t('gender_label', lang)}:** {g}")
                    st.write(f"**{t('perpetrators_label', lang)}:** {inc['perpetrator_count']}")
                st.text_area(t('original_text', lang), inc['raw_text'], height=150, disabled=True)
        else:
            st.info(t('no_data_yet', lang))
    
    elif page == t('nav_analytics', lang):
        try:
            from analytics import render_analytics
            render_analytics()
        except Exception as e:
            st.error(f"{t('error', lang)}: {e}")
    
    elif page == t('nav_errors', lang):
        st.subheader(t('processing_errors', lang))
        engine = get_engine()
        with engine.connect() as conn:
            errors = pd.read_sql("SELECT * FROM processing_errors ORDER BY created_at DESC", conn)
        if not errors.empty:
            st.dataframe(errors[['id', 'bulletin_id', 'error_type', 'created_at']], hide_index=True)
        else:
            st.success(t('no_errors', lang))
    
    elif page == t('nav_search', lang):
        st.subheader(t('search_label', lang))
        q = st.text_input(t('search_crimes', lang))
        if q:
            incidents = get_incidents()
            results = incidents[incidents['crime_type'].str.contains(q, case=False, na=False)]
            st.write(f"**{len(results)}** {t('results', lang)}")
            st.dataframe(results[['crime_type', 'crime_date', 'location_city']], hide_index=True)
    
    st.divider()
    st.caption(f"{t('last_updated', lang)}: {datetime.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    main()
