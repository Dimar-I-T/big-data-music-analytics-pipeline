import pandas as pd
import plotly.express as px
import requests
import streamlit as st

API_BASE = "http://localhost:8000"

st.set_page_config(
    page_title="Big Data Music Analytics Dashboard",
    page_icon="M11",
    layout="wide"
)

st.title("Cross Platform Music Analytics Dashboard")
st.caption("Arsitektur Big Data 5 Layer: Kafka • HDFS (Parquet) • Apache Spark • Serving API • Streamlit")

@st.cache_data(ttl=15)
def fetch_api(endpoint: str):
    resp = requests.get(f"{API_BASE}{endpoint}", timeout=5)
    resp.raise_for_status()
    return pd.DataFrame(resp.json())

# Sidebar Filter
st.sidebar.header("Filter Dashboard")
platform_filter = st.sidebar.selectbox(
    "Pilih Platform Musik",
    options=["Semua Platform", "spotify", "apple_music"],
    format_func=lambda x: x.replace("_", " ").title()
)

if st.sidebar.button("Refresh Data dari Serving Layer"):
    st.cache_data.clear()
    st.rerun()

try:
    df_top = fetch_api("/api/descriptive/top-songs")
    df_genre = fetch_api("/api/descriptive/genres")
    df_diag = fetch_api("/api/diagnostic/audio")
    df_corr = fetch_api("/api/diagnostic/correlations")
    df_metrics = fetch_api("/api/predictive/metrics")
    df_presc = fetch_api("/api/prescriptive/recommendations")
except Exception as e:
    st.error(f"Gagal terhubung ke Serving API ({API_BASE}). Pastikan 'serving_api.py' sudah berjalan. Detail: {e}")
    st.stop()

if platform_filter != "Semua Platform":
    df_top_view = df_top[df_top["platform"] == platform_filter]
    df_genre_view = df_genre[df_genre["platform"] == platform_filter]
    df_diag_view = df_diag[df_diag["platform"] == platform_filter]
    df_presc_view = df_presc[df_presc["platform"] == platform_filter]
else:
    df_top_view = df_top.groupby(["track_id", "track_name", "artist", "genre"], as_index=False).agg(
        total_plays=("total_plays", "sum"),
        total_minutes=("total_minutes", "sum"),
        avg_completion_pct=("avg_completion_pct", "mean")
    ).sort_values("total_plays", ascending=False)
    df_genre_view = df_genre
    df_diag_view = df_diag
    df_presc_view = df_presc

m_row = df_metrics.iloc[0]
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Play Events (HDFS Clean)", f"{int(m_row['total_events_analyzed']):,}")
c2.metric("Lagu Unik Dianalisis", f"{df_top_view['track_id'].nunique()}")
c3.metric("Akurasi Model Prediktif (RF)", f"{m_row['rf_accuracy']*100:.1f}%", f"vs Baseline {m_row['naive_baseline_accuracy']*100:.1f}%")
c4.metric("Rekomendasi Prioritas Tinggi", f"{len(df_presc_view[df_presc_view['prioritas'] == 'TINGGI'])} Lagu")

st.divider()

# BAGIAN 1: ANALITIK DESKRIPTIF
st.header("1. Analitik Deskriptif: Tren & Peringkat Pemutaran")
col_d1, col_d2 = st.columns([3, 2])

with col_d1:
    top10 = df_top_view.head(10).sort_values("total_plays", ascending=True)
    fig_bar = px.bar(
        top10,
        x="total_plays",
        y="track_name",
        orientation="h",
        color="genre",
        hover_data=["artist", "total_minutes", "avg_completion_pct"],
        title="Top 10 Lagu Terpopuler Berdasarkan Jumlah Pemutaran",
        labels={"total_plays": "Total Pemutaran", "track_name": "Judul Lagu"}
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col_d2:
    fig_genre = px.bar(
        df_genre_view,
        x="genre",
        y="total_plays",
        color="platform",
        barmode="stack",
        title="Komposisi Pemutaran Genre Lintas Platform",
        labels={"total_plays": "Jumlah Pemutaran", "genre": "Genre"}
    )
    st.plotly_chart(fig_genre, use_container_width=True)

st.divider()

# BAGIAN 2: ANALITIK DIAGNOSTIK
st.header("2. Analitik Diagnostik: Karakteristik Audio vs Retensi Pendengar")
col_diag1, col_diag2 = st.columns([3, 1])

with col_diag1:
    fig_scatter = px.scatter(
        df_diag_view,
        x="energy",
        y="avg_completion_rate",
        size="play_count",
        color="platform",
        hover_name="track_name",
        hover_data=["artist", "skip_rate", "danceability", "tempo"],
        title="Hubungan Skor Energy Lagu terhadap Tingkat Penyelesaian (Completion Rate)",
        labels={"energy": "Skor Energy (0 - 1)", "avg_completion_rate": "Rata-rata Completion Rate"}
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

with col_diag2:
    st.subheader("Korelasi Pearson")
    st.caption("Korelasi dengan tingkat penyelesaian lagu:")
    st.dataframe(df_corr, hide_index=True, use_container_width=True)

st.divider()

# BAGIAN 3 dan 4: ANALITIK PREDIKTIF & PRESKRIPTIF
st.header("3. Analitik Prediktif & Preskriptif: Potensi Top Chart & Rekomendasi Aksi")

col_p1, col_p2, col_p3 = st.columns(3)
col_p1.info(f"**Model:** {m_row['model_name']}")
col_p2.success(f"**F1-Score:** {m_row['rf_f1_score']} | **AUC:** {m_row['rf_auc']}")
col_p3.warning(f"**Naive Baseline Accuracy:** {m_row['naive_baseline_accuracy']*100:.1f}%")

prio_filter = st.multiselect(
    "Filter Tingkat Prioritas Aksi:",
    options=["TINGGI", "EKSPANSI", "EVALUASI", "RUTIN"],
    default=["TINGGI", "EKSPANSI", "EVALUASI", "RUTIN"]
)

df_action_filtered = df_presc_view[df_presc_view["prioritas"].isin(prio_filter)]

st.dataframe(
    df_action_filtered[[
        "prioritas", "track_name", "artist", "platform",
        "play_count", "skip_rate", "chart_probability", "rekomendasi_aksi"
    ]],
    hide_index=True,
    use_container_width=True
)