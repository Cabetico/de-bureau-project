import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import plotly.graph_objects as go
import os
import folium
from streamlit_folium import st_folium
import requests

# Page configuration
st.set_page_config(
    page_title="Bureau BI Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

GCP_PROJECT = os.getenv("GCP_PROJECT")

# ==================== AUTHENTICATION ====================
def check_password():
    """Simple password authentication"""
    def password_entered():
        if st.session_state["password"] == os.getenv("STREAMLIT_PASSWORD", "admin123"):
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.title("🔐 Bureau BI Dashboard Login")
        st.text_input(
            "Password", 
            type="password", 
            on_change=password_entered, 
            key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        st.title("🔐 Bureau BI Dashboard Login")
        st.text_input(
            "Password", 
            type="password", 
            on_change=password_entered, 
            key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        return True

# ==================== BIGQUERY CONNECTION ====================
@st.cache_resource
def get_bigquery_client():
    """Initialize BigQuery client with service account credentials"""
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    gcp_project = os.getenv("GCP_PROJECT")
    
    if credentials_path and os.path.exists(credentials_path):
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        return bigquery.Client(credentials=credentials, project=gcp_project)
    else:
        # Fallback to default credentials
        return bigquery.Client(project=gcp_project)

# ==================== DATA LOADING ====================
@st.cache_data(ttl=3600)  # Cache for 1 hour, adjust based on your batch refresh interval
def load_applications_by_state():
    """Load applications count by Mexican state"""
    client = get_bigquery_client()
    query = f"""
    SELECT 
        state as state_name,
        N as num_applications
    FROM `{GCP_PROJECT}.marts.fct_applications_per_state`
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_metrics():
    """Load general metrics derived from state data"""
    # We'll derive this from the state data since we only have that table
    df_states = load_applications_by_state()
    
    # Calculate total applications
    total_applications = df_states['num_applications'].sum()
    avg_applications_per_state = df_states['num_applications'].mean()
    total_states = len(df_states)
    
    # Create a metrics dataframe
    metrics = pd.DataFrame({
        'total_applications': [total_applications],
        'avg_applications_per_state': [avg_applications_per_state],
        'total_states': [total_states]
    })
    
    return metrics

# Time series data not available in current schema - commented out
# @st.cache_data(ttl=3600)
# def load_time_series():
#     """Load applications over time"""
#     client = get_bigquery_client()
#     query = f"""
#     SELECT 
#         FORMAT_DATE('%Y-%m', fecha_solicitud) as month,
#         COUNT(*) as num_applications
#     FROM `{GCP_PROJECT}.buro_marts.applications_offices`
#     WHERE fecha_solicitud IS NOT NULL
#     GROUP BY month
#     ORDER BY month
#     """
#     return client.query(query).to_dataframe()

# ==================== MEXICO MAP DATA ====================
# Mapping from state abbreviations to full names and coordinates
MEXICO_STATES_MAPPING = {
    'AGS': {'name': 'Aguascalientes', 'lat': 21.88, 'lon': -102.30},
    'BC': {'name': 'Baja California', 'lat': 30.84, 'lon': -115.28},
    'BCS': {'name': 'Baja California Sur', 'lat': 26.04, 'lon': -111.67},
    'CAMP': {'name': 'Campeche', 'lat': 19.83, 'lon': -90.53},
    'CHIS': {'name': 'Chiapas', 'lat': 16.75, 'lon': -93.12},
    'CHIH': {'name': 'Chihuahua', 'lat': 28.63, 'lon': -106.08},
    'COAH': {'name': 'Coahuila', 'lat': 27.06, 'lon': -101.71},
    'COL': {'name': 'Colima', 'lat': 19.24, 'lon': -103.72},
    'DGO': {'name': 'Durango', 'lat': 24.02, 'lon': -104.67},
    'GTO': {'name': 'Guanajuato', 'lat': 21.02, 'lon': -101.26},
    'GRO': {'name': 'Guerrero', 'lat': 17.44, 'lon': -99.55},
    'HGO': {'name': 'Hidalgo', 'lat': 20.09, 'lon': -98.76},
    'JAL': {'name': 'Jalisco', 'lat': 20.66, 'lon': -103.35},
    'MEX': {'name': 'México', 'lat': 19.50, 'lon': -99.74},
    'MICH': {'name': 'Michoacán', 'lat': 19.57, 'lon': -101.71},
    'MOR': {'name': 'Morelos', 'lat': 18.68, 'lon': -99.10},
    'NAY': {'name': 'Nayarit', 'lat': 21.75, 'lon': -104.85},
    'NL': {'name': 'Nuevo León', 'lat': 25.59, 'lon': -99.99},
    'OAX': {'name': 'Oaxaca', 'lat': 17.07, 'lon': -96.72},
    'PUE': {'name': 'Puebla', 'lat': 19.04, 'lon': -98.20},
    'QRO': {'name': 'Querétaro', 'lat': 20.59, 'lon': -100.39},
    'QROO': {'name': 'Quintana Roo', 'lat': 19.18, 'lon': -88.48},
    'SLP': {'name': 'San Luis Potosí', 'lat': 22.16, 'lon': -100.98},
    'SIN': {'name': 'Sinaloa', 'lat': 25.00, 'lon': -107.50},
    'SON': {'name': 'Sonora', 'lat': 29.30, 'lon': -110.33},
    'TAB': {'name': 'Tabasco', 'lat': 17.84, 'lon': -92.62},
    'TAMPS': {'name': 'Tamaulipas', 'lat': 24.27, 'lon': -98.84},
    'TLAX': {'name': 'Tlaxcala', 'lat': 19.32, 'lon': -98.24},
    'VER': {'name': 'Veracruz', 'lat': 19.53, 'lon': -96.91},
    'YUC': {'name': 'Yucatán', 'lat': 20.71, 'lon': -89.09},
    'ZAC': {'name': 'Zacatecas', 'lat': 22.77, 'lon': -102.58},
    'CDMX': {'name': 'Ciudad de México', 'lat': 19.43, 'lon': -99.13}
}

# ==================== VISUALIZATIONS ====================
@st.cache_data(ttl=86400)
def load_mexico_geojson():
    """Load Mexico states GeoJSON from GitHub"""
    url = "https://raw.githubusercontent.com/angelnmara/geojson/master/mexicoHigh.json"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Error loading GeoJSON: {str(e)}")
        return None

def get_color_for_value(value, min_val, max_val):
    """Generate color based on value using a gradient from light to dark green"""
    if max_val == min_val:
        return "#006847"
    
    # Normalize value between 0 and 1
    normalized = (value - min_val) / (max_val - min_val)
    
    # Color scale from light yellow-green to dark green
    if normalized < 0.2:
        return "#d4edda"
    elif normalized < 0.4:
        return "#95d5b2"
    elif normalized < 0.6:
        return "#52b788"
    elif normalized < 0.8:
        return "#2d6a4f"
    else:
        return "#1b4332"

def create_mexico_map(df_states):
    """Create interactive Folium map with state boundaries colored by applications"""
    # Prepare data
    df_map = df_states.copy()
    df_map['state_name'] = df_map['state_name'].str.strip()
    df_map['full_name'] = df_map['state_name'].map(lambda x: MEXICO_STATES_MAPPING.get(x, {}).get('name', x))
    
    # Load GeoJSON
    geojson_data = load_mexico_geojson()
    if not geojson_data:
        st.error("Unable to load map data")
        return None
    
    # Create mapping of state names to data
    state_data_map = {}
    for _, row in df_map.iterrows():
        state_data_map[row['full_name']] = {
            'abbreviation': row['state_name'],
            'applications': row['num_applications']
        }
    
    # Get min and max for color scaling
    min_apps = df_map['num_applications'].min()
    max_apps = df_map['num_applications'].max()
    
    # Create the map
    m = folium.Map(
        location=[23.6, -102.5],
        zoom_start=5,
        tiles="CartoDB positron",
        zoom_control=True,
    )
    
    def style_function(feature):
        """Style each state based on number of applications"""
        state_name = feature['properties'].get('name', '')
        
        # Try to find matching state data
        data = state_data_map.get(state_name)
        
        if data:
            color = get_color_for_value(data['applications'], min_apps, max_apps)
            return {
                'fillColor': color,
                'color': 'white',
                'weight': 1.5,
                'fillOpacity': 0.7
            }
        else:
            # State with no data - gray
            return {
                'fillColor': '#cccccc',
                'color': 'white',
                'weight': 1.5,
                'fillOpacity': 0.3
            }
    
    def highlight_function(feature):
        """Highlight state on hover"""
        return {
            'fillColor': '#FFD700',
            'color': '#333',
            'weight': 2.5,
            'fillOpacity': 0.8
        }
    
    # Enrich GeoJSON features with our data
    for feature in geojson_data['features']:
        state_name = feature['properties'].get('name', '')
        data = state_data_map.get(state_name)
        
        if data:
            feature['properties']['tooltip_text'] = f"{data['abbreviation']}: {data['applications']:,} applications"
            feature['properties']['applications'] = data['applications']
            feature['properties']['abbreviation'] = data['abbreviation']
        else:
            feature['properties']['tooltip_text'] = f"{state_name}: No data"
            feature['properties']['applications'] = 0
            feature['properties']['abbreviation'] = 'N/A'
    
    # Add GeoJSON layer with tooltips showing abbreviation and applications
    folium.GeoJson(
        geojson_data,
        name="States",
        style_function=style_function,
        highlight_function=highlight_function,
        tooltip=folium.GeoJsonTooltip(
            fields=['tooltip_text'],
            aliases=[''],
            style=(
                "background-color: white;"
                "border: 2px solid #006847;"
                "border-radius: 8px;"
                "padding: 8px 12px;"
                "font-size: 14px;"
                "font-weight: 600;"
                "color: #1a1a2e;"
                "box-shadow: 0 2px 4px rgba(0,0,0,0.1);"
            ),
            sticky=True,
        ),
    ).add_to(m)
    
    # Add custom legend
    legend_html = f'''
    <div style="
        position: fixed; 
        bottom: 50px; 
        right: 50px; 
        width: 200px; 
        background-color: white; 
        border: 2px solid #006847; 
        border-radius: 8px; 
        padding: 15px;
        font-size: 14px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        z-index: 9999;
    ">
        <h4 style="margin: 0 0 10px 0; color: #1a1a2e;">Applications</h4>
        <div style="margin-bottom: 5px;">
            <span style="background: #1b4332; width: 20px; height: 15px; display: inline-block; margin-right: 5px;"></span>
            {max_apps:,}
        </div>
        <div style="margin-bottom: 5px;">
            <span style="background: #2d6a4f; width: 20px; height: 15px; display: inline-block; margin-right: 5px;"></span>
        </div>
        <div style="margin-bottom: 5px;">
            <span style="background: #52b788; width: 20px; height: 15px; display: inline-block; margin-right: 5px;"></span>
        </div>
        <div style="margin-bottom: 5px;">
            <span style="background: #95d5b2; width: 20px; height: 15px; display: inline-block; margin-right: 5px;"></span>
        </div>
        <div>
            <span style="background: #d4edda; width: 20px; height: 15px; display: inline-block; margin-right: 5px;"></span>
            {min_apps:,}
        </div>
    </div>
    '''
    m.get_root()#.html.add_child(folium.Element(legend_html))
    
    return m

def create_time_series_chart(df_time):
    """Create time series chart"""
    fig = px.line(
        df_time,
        x='month',
        y='num_applications',
        title='Applications Over Time',
        labels={'month': 'Month', 'num_applications': 'Number of Applications'}
    )
    fig.update_traces(mode='lines+markers')
    fig.update_layout(height=400)
    return fig

def create_top_states_bar(df_states):
    """Create bar chart of top 10 states"""
    df_top = df_states.copy()
    
    # Strip whitespace and add full names
    df_top['state_name'] = df_top['state_name'].str.strip()
    df_top['full_name'] = df_top['state_name'].map(lambda x: MEXICO_STATES_MAPPING.get(x, {}).get('name', x))
    
    # Sort and get top 10
    df_top = df_top.nlargest(10, 'num_applications')
    
    fig = px.bar(
        df_top,
        x='num_applications',
        y='full_name',
        orientation='h',
        title='Top 10 States by Applications',
        labels={'num_applications': 'Number of Applications', 'full_name': 'State'}
    )
    fig.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
    return fig

# ==================== MAIN APP ====================
def main():
    if not check_password():
        return
    
    # Sidebar
    st.sidebar.title("📊 Bureau BI Dashboard")
    st.sidebar.markdown("---")
    
    page = st.sidebar.radio(
        "Navigation",
        ["Overview", "State Analysis", "Raw Data"]
    )
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.info("Data refreshes automatically every hour")
    
    # Main content
    if page == "Overview":
        st.title("📈 Overview Dashboard")
        
        # Load metrics
        try:
            df_metrics = load_metrics()
            
            # Display KPIs
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "Total Applications",
                    f"{df_metrics['total_applications'].values[0]:,.0f}"
                )
            with col2:
                st.metric(
                    "Total States",
                    f"{df_metrics['total_states'].values[0]:,.0f}"
                )
            with col3:
                st.metric(
                    "Avg Applications/State",
                    f"{df_metrics['avg_applications_per_state'].values[0]:,.1f}"
                )
            
            st.markdown("---")
            
            # Load state data
            df_states = load_applications_by_state()
            
            # Top states visualization
            st.subheader("Top States by Applications")
            st.plotly_chart(create_top_states_bar(df_states), use_container_width=True)
        
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            st.info("Please check your BigQuery table names and column names match the queries.")
    
    elif page == "State Analysis":
        st.title("🗺️ State Analysis")
        
        try:
            df_states = load_applications_by_state()
            
            # Interactive Mexico Map with Folium
            st.subheader("Interactive Map - Applications by State")
            st.info("💡 Hover over any state to see its abbreviation and number of applications")
            
            mexico_map = create_mexico_map(df_states)
            if mexico_map:
                st_folium(mexico_map, width="100%", height=600)
            
            st.markdown("---")
            
            # State comparison
            st.subheader("State Comparison")
            
            # Add full names for selection
            df_states_clean = df_states.copy()
            df_states_clean['state_name'] = df_states_clean['state_name'].str.strip()
            df_states_clean['full_name'] = df_states_clean['state_name'].map(lambda x: MEXICO_STATES_MAPPING.get(x, {}).get('name', x))
            
            # Create mapping for selection (show full names, use abbreviations)
            state_options = {row['full_name']: row['state_name'] for _, row in df_states_clean.iterrows()}
            
            selected_full_names = st.multiselect(
                "Select states to compare",
                options=list(state_options.keys()),
                default=list(state_options.keys())[:3]
            )
            
            if selected_full_names:
                # Convert back to abbreviations for filtering
                selected_abbrevs = [state_options[name] for name in selected_full_names]
                df_selected = df_states_clean[df_states_clean['state_name'].isin(selected_abbrevs)]
                
                # Bar chart with applications
                fig = px.bar(
                    df_selected,
                    x='full_name',
                    y='num_applications',
                    title='Number of Applications by State',
                    labels={'num_applications': 'Number of Applications', 'full_name': 'State'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Show data table
                st.dataframe(
                    df_selected[['full_name', 'num_applications']].sort_values('num_applications', ascending=False),
                    use_container_width=True
                )
        
        except Exception as e:
            st.error(f"Error loading state data: {str(e)}")
    
    elif page == "Raw Data":
        st.title("📋 Raw Data")
        
        data_choice = st.selectbox(
            "Select dataset",
            ["Applications by State", "Metrics"]
        )
        
        try:
            if data_choice == "Applications by State":
                df = load_applications_by_state()
                # Add full names for display
                df_display = df.copy()
                df_display['state_name'] = df_display['state_name'].str.strip()
                df_display['full_name'] = df_display['state_name'].map(lambda x: MEXICO_STATES_MAPPING.get(x, {}).get('name', x))
                df = df_display[['state_name', 'full_name', 'num_applications']].sort_values('num_applications', ascending=False)
            else:
                df = load_metrics()
            
            st.dataframe(df, use_container_width=True)
            
            # Download button
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"{data_choice.lower().replace(' ', '_')}.csv",
                mime="text/csv"
            )
        
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")

if __name__ == "__main__":
    main()