import streamlit as st
import pandas as pd
import psycopg2
import os
import io
from sqlalchemy import create_engine, text
import hashlib
import graphviz
import plotly.express as px
import plotly.graph_objects as go
from google import genai
from dotenv import load_dotenv

# ══════════════════════════════════════════════
# 1. CONFIGURATION
# ══════════════════════════════════════════════
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyDehcCQ6IYczTle07L01pPMdlxKkdGqd-w")
client = genai.Client(api_key=API_KEY)

# ══════════════════════════════════════════════
# 2. DATABASE (cached for speed)
# ══════════════════════════════════════════════
@st.cache_resource
def get_sqlalchemy_engine():
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "1234")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME", "testdb")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}", pool_size=5, pool_pre_ping=True)

def get_db_connection():
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            database=os.getenv("DB_NAME", "testdb"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "1234"),
            port=os.getenv("DB_PORT", "5432")
        )
    except Exception as e:
        st.error(f"❌ Database Connection Failed: {e}")
        return None

def hash_password(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

@st.cache_resource
def init_db():
    conn = get_db_connection()
    if conn:
        curr = conn.cursor()
        curr.execute("""
            CREATE TABLE IF NOT EXISTS app_users (
                id SERIAL PRIMARY KEY,
                name TEXT,
                email TEXT UNIQUE,
                password TEXT
            )
        """)
        conn.commit()
        conn.close()
    return True

@st.cache_data(ttl=120)
def get_detailed_schema():
    try:
        engine = get_sqlalchemy_engine()
        query = """
    SELECT 
        cols.table_name, cols.column_name, cols.data_type,
        CASE 
            WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'PK'
            WHEN tc.constraint_type = 'FOREIGN KEY' THEN 'FK'
            ELSE NULL 
        END AS key_type,
        ccu.table_name AS referenced_table,
        ccu.column_name AS referenced_column
    FROM information_schema.columns cols
    LEFT JOIN information_schema.key_column_usage kcu 
        ON cols.table_name = kcu.table_name AND cols.column_name = kcu.column_name
    LEFT JOIN information_schema.table_constraints tc 
        ON kcu.constraint_name = tc.constraint_name
    LEFT JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name AND tc.constraint_type = 'FOREIGN KEY'
    WHERE cols.table_schema = 'public'
    ORDER BY cols.table_name;
        """
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=120)
def get_table_data(table_name, limit=100):
    engine = get_sqlalchemy_engine()
    df = pd.read_sql(f'SELECT * FROM "{table_name}" LIMIT {limit}', engine)
    return df

@st.cache_data(ttl=120)
def get_all_users():
    try:
        engine = get_sqlalchemy_engine()
        return pd.read_sql("SELECT name, email FROM app_users ORDER BY id", engine)
    except Exception:
        return pd.DataFrame()

# ══════════════════════════════════════════════
# 3. PAGE CONFIG & PREMIUM CSS
# ══════════════════════════════════════════════
st.set_page_config(page_title="Relational AI Analyst", layout="wide", page_icon="⚡")

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
<style>
    /* ═══ RESET & GLOBAL ═══ */
    *, *::before, *::after { font-family: 'Inter', sans-serif !important; }
    
    .stApp {
        background: linear-gradient(135deg, #f8f9fc 0%, #eef1f8 50%, #f0f0fa 100%);
    }
    
    /* ═══ ANIMATED BACKGROUND ═══ */
    .stApp::before {
        content: '';
        position: fixed;
        top: 0; left: 0; right: 0; bottom: 0;
        background: 
            radial-gradient(ellipse 80% 50% at 20% 40%, rgba(99,102,241,0.05) 0%, transparent 60%),
            radial-gradient(ellipse 60% 40% at 80% 20%, rgba(139,92,246,0.04) 0%, transparent 50%),
            radial-gradient(ellipse 50% 60% at 60% 80%, rgba(236,72,153,0.03) 0%, transparent 50%);
        pointer-events: none;
        z-index: 0;
        animation: bgPulse 8s ease-in-out infinite alternate;
    }
    @keyframes bgPulse {
        0% { opacity: 0.6; }
        100% { opacity: 1; }
    }
    
    /* ═══ SIDEBAR ═══ */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #ffffff 0%, #f7f8fc 100%);
        border-right: 1px solid #e8e8f0;
    }
    section[data-testid="stSidebar"] .stMarkdown { color: #3d3d50; }
    
    /* ═══ TAB BAR ═══ */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
        background: #ffffff;
        border-radius: 14px;
        padding: 5px;
        border: 1px solid #e8e8f0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.04);
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        padding: 10px 18px;
        color: #8888a0;
        font-weight: 500;
        font-size: 0.85rem;
        transition: all 0.25s cubic-bezier(0.4,0,0.2,1);
        border: none;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: #5b5b80;
        background: rgba(99,102,241,0.05);
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 50%, #a855f7 100%) !important;
        color: white !important;
        font-weight: 600;
        box-shadow: 0 4px 15px rgba(99,102,241,0.3);
    }
    
    /* ═══ CARD ═══ */
    .neo-card {
        background: #ffffff;
        border: 1px solid #e8e8f0;
        border-radius: 20px;
        padding: 28px;
        margin: 16px 0;
        box-shadow: 0 4px 20px rgba(0,0,0,0.04);
        transition: all 0.3s cubic-bezier(0.4,0,0.2,1);
    }
    .neo-card:hover {
        border-color: rgba(99,102,241,0.2);
        box-shadow: 0 8px 30px rgba(99,102,241,0.08);
    }
    
    /* ═══ STAT PILLS ═══ */
    .stat-grid {
        display: grid;
        grid-template-columns: repeat(2, 1fr);
        gap: 10px;
        margin: 12px 0;
    }
    .stat-pill {
        background: linear-gradient(135deg, #f3f2ff, #ede8ff);
        border: 1px solid #e0d8f5;
        border-radius: 14px;
        padding: 16px 12px;
        text-align: center;
        transition: all 0.2s ease;
    }
    .stat-pill:hover {
        transform: translateY(-2px);
        border-color: rgba(99,102,241,0.3);
        box-shadow: 0 4px 15px rgba(99,102,241,0.1);
    }
    .stat-num {
        font-size: 1.8rem;
        font-weight: 800;
        background: linear-gradient(135deg, #6366f1, #8b5cf6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1.2;
    }
    .stat-lbl {
        font-size: 0.7rem;
        color: #8888a0;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 600;
        margin-top: 4px;
    }
    
    /* ═══ BIG METRIC CARDS ═══ */
    .metric-row {
        display: grid;
        grid-template-columns: repeat(3, 1fr);
        gap: 14px;
        margin: 16px 0;
    }
    .metric-box {
        background: #ffffff;
        border: 1px solid #e8e8f0;
        border-radius: 16px;
        padding: 24px;
        text-align: center;
        transition: all 0.25s ease;
        position: relative;
        overflow: hidden;
    }
    .metric-box::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        background: linear-gradient(90deg, #6366f1, #8b5cf6, #a855f7);
        opacity: 0;
        transition: opacity 0.3s ease;
    }
    .metric-box:hover::before { opacity: 1; }
    .metric-box:hover {
        transform: translateY(-3px);
        border-color: rgba(99,102,241,0.2);
        box-shadow: 0 8px 25px rgba(99,102,241,0.1);
    }
    .metric-box .val {
        font-size: 2rem;
        font-weight: 800;
        background: linear-gradient(135deg, #6366f1, #8b5cf6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-box .lbl {
        font-size: 0.75rem;
        color: #8888a0;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 600;
        margin-top: 6px;
    }
    
    /* ═══ BUTTONS ═══ */
    .stButton > button {
        background: linear-gradient(135deg, #6366f1, #8b5cf6) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 12px 28px !important;
        font-weight: 600 !important;
        font-size: 0.9rem !important;
        transition: all 0.25s cubic-bezier(0.4,0,0.2,1) !important;
        box-shadow: 0 4px 15px rgba(99,102,241,0.25) !important;
        letter-spacing: 0.3px !important;
    }
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 25px rgba(99,102,241,0.35) !important;
        background: linear-gradient(135deg, #5558e6, #7c4fec) !important;
    }
    .stButton > button:active {
        transform: translateY(0) !important;
    }
    
    /* ═══ INPUTS ═══ */
    .stTextInput input, .stTextArea textarea {
        background: #ffffff !important;
        border: 1px solid #e0e0ea !important;
        border-radius: 10px !important;
        color: #2d2d40 !important;
        font-size: 0.9rem !important;
        transition: all 0.2s ease !important;
    }
    .stTextInput input:focus, .stTextArea textarea:focus {
        border-color: #6366f1 !important;
        box-shadow: 0 0 0 3px rgba(99,102,241,0.1) !important;
    }
    .stSelectbox > div > div {
        background: #ffffff !important;
        border: 1px solid #e0e0ea !important;
        border-radius: 10px !important;
    }
    
    /* ═══ FILE UPLOADER ═══ */
    [data-testid="stFileUploader"] {
        background: #f8f7ff;
        border: 2px dashed #d0ccf0;
        border-radius: 16px;
        padding: 20px;
        transition: all 0.3s ease;
    }
    [data-testid="stFileUploader"]:hover {
        border-color: #8b5cf6;
        background: #f0eeff;
    }
    
    /* ═══ DATAFRAME ═══ */
    .stDataFrame {
        border-radius: 14px;
        overflow: hidden;
        border: 1px solid #e8e8f0;
    }
    
    /* ═══ HEADER ═══ */
    .hero {
        text-align: center;
        padding: 20px 0 30px 0;
        position: relative;
    }
    .hero h1 {
        font-size: 2.6rem;
        font-weight: 900;
        background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 40%, #a855f7 80%, #c084fc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 6px;
        letter-spacing: -0.5px;
    }
    .hero .subtitle {
        color: #8888a0;
        font-size: 0.95rem;
        font-weight: 400;
        letter-spacing: 0.3px;
    }
    
    /* ═══ SECTION HEADER ═══ */
    .sec-hdr {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 20px;
        padding-bottom: 12px;
        border-bottom: 1px solid #ebebf0;
    }
    .sec-hdr .ico { font-size: 1.4rem; }
    .sec-hdr .ttl {
        font-size: 1.2rem;
        font-weight: 700;
        color: #2d2d40;
        letter-spacing: -0.3px;
    }
    .sec-hdr .bdg {
        background: #ece8ff;
        color: #6366f1;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.5px;
        text-transform: uppercase;
    }
    
    /* ═══ USER CARD ═══ */
    .user-card {
        text-align: center;
        padding: 16px;
        background: linear-gradient(135deg, #f3f2ff, #ede8ff);
        border: 1px solid #e0d8f5;
        border-radius: 14px;
        margin-bottom: 12px;
    }
    .user-card .avatar {
        width: 48px; height: 48px;
        background: linear-gradient(135deg, #6366f1, #8b5cf6);
        border-radius: 50%;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 1.2rem;
        color: white;
        margin-bottom: 8px;
        box-shadow: 0 4px 15px rgba(99,102,241,0.25);
    }
    .user-card .name {
        color: #2d2d40;
        font-weight: 700;
        font-size: 0.95rem;
    }
    .user-card .role {
        color: #8888a0;
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 600;
        margin-top: 2px;
    }
    
    /* ═══ LOGIN CARD ═══ */
    .auth-card {
        background: #ffffff;
        border: 1px solid #e8e8f0;
        border-radius: 24px;
        padding: 40px 36px;
        box-shadow: 0 10px 40px rgba(0,0,0,0.06);
        position: relative;
        overflow: hidden;
    }
    .auth-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        background: linear-gradient(90deg, #6366f1, #8b5cf6, #a855f7);
    }
    .auth-title {
        text-align: center;
        margin-bottom: 28px;
    }
    .auth-title h2 {
        font-size: 1.8rem;
        font-weight: 800;
        background: linear-gradient(135deg, #4f46e5, #7c3aed);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 4px;
    }
    .auth-title p {
        color: #8888a0;
        font-size: 0.85rem;
    }
    
    /* ═══ EMPTY STATE ═══ */
    .empty-state {
        text-align: center;
        padding: 80px 20px;
    }
    .empty-state .icon { font-size: 4rem; margin-bottom: 16px; opacity: 0.6; }
    .empty-state h3 {
        color: #8888a0;
        font-weight: 600;
        font-size: 1.2rem;
        margin-bottom: 8px;
    }
    .empty-state p { color: #a0a0b0; font-size: 0.9rem; }
    
    /* ═══ CODE BLOCK ═══ */
    .stCodeBlock {
        border-radius: 14px !important;
        border: 1px solid #e8e8f0 !important;
    }
    
    /* ═══ DIVIDER ═══ */
    hr { border-color: #ebebf0 !important; }
    
    /* ═══ CHECKBOX ═══ */
    .stCheckbox label span { color: #5b5b70 !important; }
    
    /* ═══ SCROLLBAR ═══ */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: rgba(99,102,241,0.2); border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: rgba(99,102,241,0.4); }
    
    /* ═══ ANIMATION ═══ */
    @keyframes fadeInUp {
        from { opacity: 0; transform: translateY(15px); }
        to { opacity: 1; transform: translateY(0); }
    }
    .animate-in { animation: fadeInUp 0.4s ease-out forwards; }
</style>
""", unsafe_allow_html=True)

init_db()

if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

# ══════════════════════════════════════════════
# LOGIN / SIGNUP
# ══════════════════════════════════════════════
if not st.session_state['logged_in']:
    st.sidebar.markdown("""
    <div style="text-align:center; padding:20px 0;">
        <div style="font-size:2rem;">⚡</div>
        <div style="color:#6366f1; font-weight:700; font-size:0.9rem; margin-top:4px;">Relational AI</div>
        <div style="color:#8888a0; font-size:0.7rem; text-transform:uppercase; letter-spacing:2px;">Analyst</div>
    </div>
    """, unsafe_allow_html=True)
    auth_mode = st.sidebar.radio("Auth Mode", ["Login", "Sign Up"], label_visibility="collapsed")
    
    if auth_mode == "Sign Up":
        _, center, _ = st.columns([1, 1.5, 1])
        with center:
            st.markdown("""
            <div class="auth-card animate-in">
                <div class="auth-title">
                    <h2>Create Account</h2>
                    <p>Start exploring your databases with AI</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
            n_name = st.text_input("Full Name", placeholder="John Doe")
            n_email = st.text_input("Email", placeholder="john@example.com")
            n_pass = st.text_input("Password", type="password", placeholder="••••••••")
            st.markdown("")
            if st.button("Create Account →", width='stretch'):
                if not n_name or not n_email or not n_pass:
                    st.warning("Please fill all fields.")
                else:
                    try:
                        engine = get_sqlalchemy_engine()
                        with engine.connect() as conn:
                            conn.execute(text("INSERT INTO app_users (name, email, password) VALUES (:n, :e, :p)"),
                                        {"n": n_name, "e": n_email, "p": hash_password(n_pass)})
                            conn.commit()
                        st.success("✅ Account created! Switch to Login.")
                    except Exception:
                        st.error("❌ Email already exists!")
    else:
        _, center, _ = st.columns([1, 1.5, 1])
        with center:
            st.markdown("""
            <div class="auth-card animate-in">
                <div class="auth-title">
                    <h2>Welcome Back</h2>
                    <p>Login to your AI Database Analyst</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
            email = st.text_input("Email", placeholder="john@example.com")
            pw = st.text_input("Password", type="password", placeholder="••••••••")
            st.markdown("")
            if st.button("Login →", width='stretch'):
                try:
                    engine = get_sqlalchemy_engine()
                    with engine.connect() as conn:
                        result = conn.execute(text("SELECT name FROM app_users WHERE email=:e AND password=:p"),
                                            {"e": email, "p": hash_password(pw)})
                        user = result.fetchone()
                    if user:
                        st.session_state['logged_in'] = True
                        st.session_state['user_name'] = user[0]
                        st.rerun()
                    else:
                        st.error("❌ Invalid credentials")
                except Exception as e:
                    st.error(f"❌ Connection error: {e}")
    st.stop()

# ══════════════════════════════════════════════
# MAIN DASHBOARD
# ══════════════════════════════════════════════
user_initials = st.session_state['user_name'][0].upper() if st.session_state['user_name'] else "U"
st.sidebar.markdown(f"""
<div class="user-card">
    <div class="avatar">{user_initials}</div>
    <div class="name">{st.session_state['user_name']}</div>
    <div class="role">Database Analyst</div>
</div>
""", unsafe_allow_html=True)

if st.sidebar.button("Logout", width='stretch'):
    st.session_state['logged_in'] = False
    st.rerun()

st.markdown("""
<div class="hero animate-in">
    <h1>⚡ Relational AI Analyst</h1>
    <p class="subtitle">Explore schemas · Visualize data · AI-powered SQL</p>
</div>
""", unsafe_allow_html=True)

schema_df = get_detailed_schema()

# ═══ Sidebar Stats ═══
if not schema_df.empty:
    unique_tables = schema_df['table_name'].unique().tolist()
    total_cols = len(schema_df)
    pk_count = len(schema_df[schema_df['key_type'] == 'PK'])
    fk_count = len(schema_df[schema_df['key_type'] == 'FK'])
    
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"""
    <div class="stat-grid">
        <div class="stat-pill"><div class="stat-num">{len(unique_tables)}</div><div class="stat-lbl">Tables</div></div>
        <div class="stat-pill"><div class="stat-num">{total_cols}</div><div class="stat-lbl">Columns</div></div>
        <div class="stat-pill"><div class="stat-num">{pk_count}</div><div class="stat-lbl">Primary</div></div>
        <div class="stat-pill"><div class="stat-num">{fk_count}</div><div class="stat-lbl">Foreign</div></div>
    </div>
    """, unsafe_allow_html=True)
    
    st.sidebar.markdown("---")
    selected_table = st.sidebar.selectbox("🔎 Explore Table", unique_tables)
else:
    unique_tables = []
    selected_table = None

# ═══ Sidebar: Registered Users ═══
users_df = get_all_users()
if not users_df.empty:
    st.sidebar.markdown("---")
    with st.sidebar.expander(f"👥 Users ({len(users_df)})", expanded=False):
        for _, u in users_df.iterrows():
            is_me = u['name'] == st.session_state.get('user_name', '')
            badge = " 🟢" if is_me else ""
            st.markdown(f"**{u['name']}**{badge}  \n`{u['email']}`")

# ═══ TABS ═══
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
    ["📊 Data & Visuals", "🔑 Keys", "📐 Relations", "🤖 AI SQL", "📤 Upload", "🛠️ Create Table"]
)

# ═══════════════════════════════════════════════
# TAB 1-4: SCHEMA DEPENDENT
# ═══════════════════════════════════════════════
if not schema_df.empty and selected_table:
    with tab1:
        df = get_table_data(selected_table)
        
        st.markdown(f"""
        <div class="sec-hdr">
            <span class="ico">📋</span>
            <span class="ttl">{selected_table}</span>
            <span class="bdg">{len(df)} rows · {len(df.columns)} cols</span>
        </div>
        """, unsafe_allow_html=True)
        
        st.dataframe(df, width='stretch', height=380)
        st.divider()
        
        st.markdown("""<div class="sec-hdr"><span class="ico">📈</span><span class="ttl">Quick Insight</span></div>""", unsafe_allow_html=True)
        
        num_cols = df.select_dtypes(include=['number']).columns.tolist()
        all_cols = df.columns.tolist()
        
        if num_cols:
            c1, c2, c3 = st.columns(3)
            with c1: x_axis = st.selectbox("X Axis", all_cols)
            with c2: y_axis = st.selectbox("Y Axis", num_cols)
            with c3: chart_type = st.selectbox("Chart", ["Bar", "Line", "Scatter", "Area", "Pie"])
            
            tmpl = "plotly_white"
            clrs = ["#6366f1", "#8b5cf6", "#a855f7", "#c084fc", "#e879f9", "#f472b6"]
            
            if chart_type == "Bar":
                fig = px.bar(df, x=x_axis, y=y_axis, color=x_axis, color_discrete_sequence=clrs, template=tmpl)
            elif chart_type == "Line":
                fig = px.line(df, x=x_axis, y=y_axis, template=tmpl, markers=True)
                fig.update_traces(line=dict(color="#6366f1", width=3), marker=dict(size=6, color="#8b5cf6"))
            elif chart_type == "Area":
                fig = px.area(df, x=x_axis, y=y_axis, template=tmpl)
                fig.update_traces(fillcolor="rgba(99,102,241,0.12)", line=dict(color="#6366f1", width=2))
            elif chart_type == "Pie":
                fig = px.pie(df, names=x_axis, values=y_axis, color_discrete_sequence=clrs, template=tmpl, hole=0.4)
            else:
                fig = px.scatter(df, x=x_axis, y=y_axis, template=tmpl, color_discrete_sequence=["#6366f1"], size_max=12)
            
            fig.update_layout(
                plot_bgcolor="rgba(255,255,255,0)", paper_bgcolor="rgba(255,255,255,0)",
                font=dict(color="#5b5b70", size=12),
                margin=dict(l=16, r=16, t=40, b=16),
                legend=dict(bgcolor="rgba(255,255,255,0)")
            )
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No numeric columns available for charts.")

    with tab2:
        st.markdown(f"""
        <div class="sec-hdr">
            <span class="ico">🔑</span>
            <span class="ttl">Key Analysis</span>
            <span class="bdg">{selected_table}</span>
        </div>
        """, unsafe_allow_html=True)
        
        table_meta = schema_df[schema_df['table_name'] == selected_table]
        
        def highlight_keys(val):
            if val == 'PK': return 'background-color: #ece8ff; color: #6366f1; font-weight: 700'
            if val == 'FK': return 'background-color: #fef3c7; color: #d97706; font-weight: 700'
            return ''
        
        st.dataframe(
            table_meta.style.map(highlight_keys, subset=['key_type']),
            width='stretch', height=400
        )

    with tab3:
        st.markdown("""<div class="sec-hdr"><span class="ico">📐</span><span class="ttl">Entity Relationships</span></div>""", unsafe_allow_html=True)
        
        dot = graphviz.Digraph(engine='dot')
        dot.attr(bgcolor='transparent', rankdir='LR', splines='ortho')
        dot.attr('node', style='filled,rounded', fillcolor='#f3f2ff', fontcolor='#2d2d40', 
                 color='#6366f1', shape='box', fontname='Inter', fontsize='11', penwidth='1.5')
        dot.attr('edge', color='#8b5cf6', fontcolor='#5b5b70', fontname='Inter', fontsize='9', penwidth='1.5', arrowsize='0.8')
        
        rels = schema_df[schema_df['key_type'] == 'FK']
        for _, r in rels.iterrows():
            dot.edge(r['table_name'], r['referenced_table'], label=f" {r['column_name']}→{r['referenced_column']} ")
        
        if not rels.empty:
            st.graphviz_chart(dot)
        else:
            st.info("No foreign key relationships found.")

    with tab4:
        st.markdown("""
        <div class="sec-hdr">
            <span class="ico">🤖</span>
            <span class="ttl">AI SQL Assistant</span>
            <span class="bdg">Gemini 2.0</span>
        </div>
        """, unsafe_allow_html=True)
        
        user_q = st.text_area("Ask about your database:", placeholder="e.g. Write a query to find the top 5 records by sales...", height=120)
        
        if st.button("✨ Generate", width='stretch'):
            if not user_q.strip():
                st.warning("Type a question first.")
            else:
                prompt = f"""You are an expert Business Intelligence Analyst and Database Consultant.

Schema Context: {schema_df.to_dict()}

User Question: {user_q}

Provide your response in the following business-oriented format:

📊 **Business Context**: Explain what this data means from a business perspective.

🔍 **SQL Query**: Provide an optimized SQL query.

📈 **Key Business Insights**: What KPIs, trends, or actionable insights can be derived?

💡 **Recommendations**: Suggest business actions based on the analysis.

Always frame answers in terms of business value, revenue impact, operational efficiency, and strategic decision-making."""
                with st.spinner("Analyzing..."):
                    try:
                        response = client.models.generate_content(model='gemini-flash-latest', contents=prompt)
                        st.markdown(response.text)
                    except Exception as e:
                        err = str(e)
                        if "429" in err or "RESOURCE_EXHAUSTED" in err:
                            st.error("⚠️ API quota exceeded. Wait ~24h or upgrade at https://ai.google.dev/")
                        else:
                            st.error(f"❌ {e}")
else:
    for t in [tab1, tab2, tab3, tab4]:
        with t:
            st.markdown("""
            <div class="empty-state">
                <div class="icon">📭</div>
                <h3>No Tables Yet</h3>
                <p>Upload a dataset or create a table to get started</p>
            </div>
            """, unsafe_allow_html=True)

# ═══════════════════════════════════════════════
# TAB 5: UPLOAD DATASET
# ═══════════════════════════════════════════════
with tab5:
    st.markdown("""
    <div class="sec-hdr">
        <span class="ico">📤</span>
        <span class="ttl">Upload Dataset</span>
        <span class="bdg">CSV · Excel</span>
    </div>
    """, unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("Drop your file here", type=["csv", "xlsx", "xls"], key="dataset_upload", label_visibility="collapsed")
    
    if uploaded_file is not None:
        try:
            if uploaded_file.name.endswith(".csv"):
                upload_df = pd.read_csv(uploaded_file)
            else:
                upload_df = pd.read_excel(uploaded_file)
            
            st.markdown(f"""
            <div class="metric-row">
                <div class="metric-box"><div class="val">{upload_df.shape[0]}</div><div class="lbl">Rows</div></div>
                <div class="metric-box"><div class="val">{upload_df.shape[1]}</div><div class="lbl">Columns</div></div>
                <div class="metric-box"><div class="val">{uploaded_file.name.rsplit('.',1)[-1].upper()}</div><div class="lbl">Format</div></div>
            </div>
            """, unsafe_allow_html=True)
            
            st.dataframe(upload_df.head(20), width='stretch', height=280)
            
            st.divider()
            col_a, col_b = st.columns(2)
            with col_a:
                table_name = st.text_input("Table name", value=uploaded_file.name.rsplit('.', 1)[0].lower().replace(' ', '_').replace('-','_'), key="upload_tbl")
            with col_b:
                if_exists = st.selectbox("If exists", ["append", "replace", "fail"], key="upload_mode")
            
            st.markdown("")
            if st.button("⬆️ Upload to Database", key="upload_btn", width='stretch'):
                if not table_name.strip():
                    st.error("Enter a table name.")
                else:
                    with st.spinner("Uploading..."):
                        try:
                            engine = get_sqlalchemy_engine()
                            upload_df.to_sql(table_name.strip().lower(), engine, if_exists=if_exists, index=False)
                            st.success(f"🎉 **{table_name.strip().lower()}** uploaded!")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ {e}")
        except Exception as e:
            st.error(f"❌ Cannot read file: {e}")

# ═══════════════════════════════════════════════
# TAB 6: CREATE TABLE
# ═══════════════════════════════════════════════
with tab6:
    st.markdown("""
    <div class="sec-hdr">
        <span class="ico">🛠️</span>
        <span class="ttl">Create Table</span>
        <span class="bdg">No Code</span>
    </div>
    """, unsafe_allow_html=True)
    
    new_table_name = st.text_input("Table name", key="create_tbl", placeholder="e.g. students, products")
    
    TYPES = ["TEXT", "INTEGER", "BIGINT", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP", "SERIAL", "VARCHAR(255)", "NUMERIC"]
    
    if 'col_count' not in st.session_state:
        st.session_state['col_count'] = 3
    
    st.markdown("**Columns:**")
    cols_def = []
    for i in range(st.session_state['col_count']):
        c1, c2, c3, c4 = st.columns([3, 2, 0.7, 0.7])
        with c1:
            cn = st.text_input(f"Col {i+1}", key=f"cn_{i}", placeholder="column name", label_visibility="collapsed" if i > 0 else "visible")
        with c2:
            ct = st.selectbox("Type", TYPES, key=f"ct_{i}", label_visibility="collapsed" if i > 0 else "visible")
        with c3:
            pk = st.checkbox("PK", key=f"pk_{i}")
        with c4:
            nn = st.checkbox("NN", key=f"nn_{i}", help="NOT NULL")
        if cn.strip():
            cols_def.append((cn.strip(), ct, pk, nn))
    
    bc1, bc2, _ = st.columns([1, 1, 3])
    with bc1:
        if st.button("➕ Add", key="add_col", width='stretch'):
            st.session_state['col_count'] += 1
            st.rerun()
    with bc2:
        if st.session_state['col_count'] > 1:
            if st.button("➖ Remove", key="rm_col", width='stretch'):
                st.session_state['col_count'] -= 1
                st.rerun()
    
    st.divider()
    
    if new_table_name.strip() and cols_def:
        lines = []
        for cn, ct, pk, nn in cols_def:
            l = f'    "{cn}" {ct}'
            if pk: l += " PRIMARY KEY"
            if nn and not pk: l += " NOT NULL"
            lines.append(l)
        sql = f'CREATE TABLE "{new_table_name.strip().lower()}" (\n' + ",\n".join(lines) + "\n);"
        
        st.code(sql, language="sql")
        st.markdown("")
        
        if st.button("🚀 Create Table", key="create_btn", width='stretch'):
            conn = get_db_connection()
            if conn:
                try:
                    curr = conn.cursor()
                    curr.execute(sql)
                    conn.commit()
                    st.success(f"🎉 **{new_table_name.strip().lower()}** created!")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    conn.rollback()
                    st.error(f"❌ {e}")
                finally:
                    conn.close()
    elif new_table_name.strip() and not cols_def:
        st.warning("Define at least one column.")
    else:
        st.markdown("""
        <div class="empty-state">
            <div class="icon">✏️</div>
            <h3>Design Your Table</h3>
            <p>Enter a name and add columns above</p>
        </div>
        """, unsafe_allow_html=True)