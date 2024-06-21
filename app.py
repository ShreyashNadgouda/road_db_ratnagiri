import streamlit as st
import geopandas as gpd
import folium
from streamlit_folium import st_folium
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import pandas as pd
from datetime import datetime
from urllib.parse import quote_plus
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load secrets
secrets = st.secrets["database"]

# SQLAlchemy engine creation with connection pooling
@st.cache_resource
def get_postgis_engine():
    url = (
        f'postgresql+psycopg2://'
        f'{quote_plus(secrets["user"])}:{quote_plus(secrets["password"])}'
        f'@{secrets["host"]}:{secrets["port"]}'
        f'/{quote_plus(secrets["db"])}'
    )
    st.write(f"Database URL: {url}")  # For debugging
    logger.info(f"Attempting to connect to the database at {url}")
    try:
        engine = create_engine(url, poolclass=QueuePool, pool_size=5, max_overflow=10, connect_args={'connect_timeout': 10})
        logger.info("Database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        st.error(f"Failed to create database engine: {e}")
        return None

# Fetch data from database with caching
@st.cache_data(ttl=600)
def fetch_data(query):
    engine = get_postgis_engine()
    if engine is None:
        st.error("Engine is not available. Unable to fetch data.")
        return gpd.GeoDataFrame()  # Return an empty GeoDataFrame on error

    try:
        with engine.connect() as connection:
            gdf = gpd.read_postgis(text(query), con=connection, geom_col='geom')

        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)  # Assuming the fetched data uses WGS84 CRS

        return gdf
    except Exception as e:
        logger.error(f"An error occurred while fetching data: {e}")
        st.error(f"An error occurred while fetching data: {e}")
        return gpd.GeoDataFrame()  # Return an empty GeoDataFrame on error

# Convert date from `dd.mm.yyyy` to `yyyy-mm-dd`
def convert_date(date_str):
    return datetime.strptime(date_str, '%d.%m.%Y').strftime('%Y-%m-%d')

# Paths to shapefiles
district_shapefile_path = 'data/Ratnagiri_Taluka_Boundries'
road_network_shapefile_path = 'data/RN_DIV'

# Load district shapefile
@st.cache_resource
def load_district_gdf(path):
    district_gdf = gpd.read_file(path)
    if district_gdf.crs is None:
        district_gdf.set_crs(epsg=4326, inplace=True)
    else:
        district_gdf = district_gdf.to_crs(epsg=4326)
    return district_gdf

district_gdf = load_district_gdf(district_shapefile_path)

# Load road network shapefile
@st.cache_resource
def load_road_network_gdf(path):
    road_network_gdf = gpd.read_file(path)
    if road_network_gdf.crs is None:
        road_network_gdf.set_crs(epsg=4326, inplace=True)
    return road_network_gdf.to_crs(epsg=4326)

road_network_gdf = load_road_network_gdf(road_network_shapefile_path)

# Streamlit app
st.title("Ratnagiri District Road Network Mapping")

# Dropdown for selecting a category
category = st.selectbox("Select a Category", ["Road Length", "Date", "Road Type", "Block Name", "Scheme Name", "Category of Work", "Total Expenditure", "Approved Amount", "Compare Expenditure and Approved Amount", "PCI After Completion of Work", "Current Status"])

query = ""

# Additional filter option for blocks
st.subheader("Optional: Filter by Block")
selected_block = st.selectbox("Select a Block (or leave as 'All' to see the entire district)", ["All", "RATNAGIRI", "LANJA", "SANGAMESHWAR", "RAJAPUR"])


# Based on category, show specific options
# Based on category, show specific options
if category == "Road Length":
    query_type = st.selectbox("Select Query Type", ["Greater than", "Less than", "Equal to"])
    length_value = st.slider("Select Length (km)", min_value=0.0, max_value=50.0, value=5.0, step=0.1)

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if query_type == "Greater than":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_total_length" > {length_value}{block_filter}'
    elif query_type == "Less than":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_total_length" < {length_value}{block_filter}'
    elif query_type == "Equal to":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_total_length" = {length_value}{block_filter}'

elif category == "Date":
    query_type = st.selectbox("Select Query Type", ["Completed Before", "Completed After", "Approved On", "Approved Before", "Approved After"])
    date_value = st.date_input("Select Date")
    date_str = date_value.strftime('%Y-%m-%d')

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if query_type == "Completed Before":
        query = f'''
            SELECT * FROM "RN_DIV" 
            WHERE 
                "ratnagiri_final_completion_certificate_date" ~ '^\d{{2}}\.\d{{2}}\.\d{{4}}$'
                AND TO_DATE("ratnagiri_final_completion_certificate_date", 'DD.MM.YYYY') < '{date_str}'
                {block_filter}
        '''
    elif query_type == "Completed After":
        query = f'''
            SELECT * FROM "RN_DIV" 
            WHERE 
                "ratnagiri_final_completion_certificate_date" ~ '^\d{{2}}\.\d{{2}}\.\d{{4}}$'
                AND TO_DATE("ratnagiri_final_completion_certificate_date", 'DD.MM.YYYY') > '{date_str}'
                {block_filter}
        '''
    elif query_type == "Approved On":
        query = f'''
            SELECT * FROM "RN_DIV" 
            WHERE 
                "ratnagiri_final_date_of_approval" ~ '^\d{{2}}\.\d{{2}}\.\d{{4}}$'
                AND TO_DATE("ratnagiri_final_date_of_approval", 'DD.MM.YYYY') = '{date_str}'
                {block_filter}
        '''
    elif query_type == "Approved Before":
        query = f'''
            SELECT * FROM "RN_DIV" 
            WHERE 
                "ratnagiri_final_date_of_approval" ~ '^\d{{2}}\.\d{{2}}\.\d{{4}}$'
                AND TO_DATE("ratnagiri_final_date_of_approval", 'DD.MM.YYYY') < '{date_str}'
                {block_filter}
        '''
    elif query_type == "Approved After":
        query = f'''
            SELECT * FROM "RN_DIV" 
            WHERE 
                "ratnagiri_final_date_of_approval" ~ '^\d{{2}}\.\d{{2}}\.\d{{4}}$'
                AND TO_DATE("ratnagiri_final_date_of_approval", 'DD.MM.YYYY') > '{date_str}'
                {block_filter}
        '''

elif category == "Road Type":
    road_types = ["MDR", "NH", "ODR", "RR(ODR)", "RR(VR)", "RR(VR)VR", "SH", "VR"]
    selected_types = st.multiselect("Select Road Types", road_types)

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if selected_types:
        conditions = [f'"drrp_road_" LIKE \'{road_type}%\'' for road_type in selected_types]
        where_clause = " OR ".join(conditions)
        query = f'SELECT * FROM "RN_DIV" WHERE ({where_clause}){block_filter}'

elif category == "Block Name":
    block_names = ["LANJA", "RAJAPUR", "RATNAGIRI", "SANGAMESHWAR"]
    selected_blocks = st.multiselect("Select Block Names", block_names)

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if selected_blocks:
        block_list = "', '".join(selected_blocks)
        query = f'SELECT * FROM "RN_DIV" WHERE "Block_Name" IN (\'{block_list}\'){block_filter}'

elif category == "Scheme Name":
    scheme_names = [
        "50540106 (04)", "50540349 (03)", "5055 Capital Expenditure on Roads and Bridges (04) District and Other Road Bridges (50545369) Bridges",
        "Account Head - 50545289", "District Rural Roads Development and Strengthening 3054", "Hill Development Programme",
        "Local Development Program of MPs", "MMGSY", "Road Special Repair Program (Group-A)", "Road Special Repair Program (Group-B)",
        "Road Special Repair Program (Group-C)", "Road Special Repair Program (Group-D)",
        "Roads and Bridges / Work-wise Information under the Budget Plan- Information about the Roads and Bridges works to be constructed through Annuity"
    ]

    selected_schemes = st.multiselect("Select Scheme Names", scheme_names)

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if selected_schemes:
        scheme_list = "', '".join(selected_schemes)
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_scheme_name" IN (\'{scheme_list}\'){block_filter}'

elif category == "Category of Work":
    category_of_work_groups = {
        'Asphalt Resurfacing': ['Asphalt Resurfacing', 'Asphalt Resurfacing.'],
        'Building a protective wall': ['Building a protective wall', 'Building a protective wall.', 'Building a protective wall.  Drainage - Construction of a protective wall'],
        'Complete Road': ['Complete Road', 'Complete Road.', 'Complete Road. Under Surface Improvement Program'],
        'Drainage': ['Drainage', 'Drainage - Construction of a protective wall.', 'Drainage - Construction of a protective wall.'],
        'Rehabilitation': ['Rehabilitation', 'Rehabilitation.', 'Rehabilitation.  Asphalt Resurfacing'],
        'Surface': ['Surface', 'Surface Improvement']
    }

    selected_categories = st.multiselect("Select Category of Work", list(category_of_work_groups.keys()))

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if selected_categories:
        conditions = []
        for category in selected_categories:
            subcategories = category_of_work_groups[category]
            sub_conditions = [f'"ratnagiri_final_category_of_work" = \'{subcategory}\'' for subcategory in subcategories]
            conditions.append("(" + " OR ".join(sub_conditions) + ")")

        where_clause = " OR ".join(conditions)
        query = f'SELECT * FROM "RN_DIV" WHERE ({where_clause}){block_filter}'

elif category == "Total Expenditure":
    query_type = st.selectbox("Select Query Type", ["Greater than", "Less than", "Equal to"])
    expenditure_value = st.slider("Select Expenditure (INR)", min_value=0, max_value=10000, value=1000, step=1)

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if query_type == "Greater than":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_total_expenditure" > {expenditure_value}{block_filter}'
    elif query_type == "Less than":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_total_expenditure" < {expenditure_value}{block_filter}'
    elif query_type == "Equal to":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_total_expenditure" = {expenditure_value}{block_filter}'

elif category == "Approved Amount":
    query_type = st.selectbox("Select Query Type", ["Greater than", "Less than", "Equal to"])
    approved_amount_value = st.slider("Select Approved Amount (INR)", min_value=0, max_value=10000, value=1000, step=1)

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if query_type == "Greater than":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_approved_amount" > {approved_amount_value}{block_filter}'
    elif query_type == "Less than":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_approved_amount" < {approved_amount_value}{block_filter}'
    elif query_type == "Equal to":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_approved_amount" = {approved_amount_value}{block_filter}'

elif category == "Compare Expenditure and Approved Amount":
    query_type = st.selectbox("Select Query Type", ["Expenditure exceeds Approved Amount", "Approved Amount exceeds Expenditure"])

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if query_type == "Expenditure exceeds Approved Amount":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_total_expenditure" > "ratnagiri_final_approved_amount"{block_filter}'
    elif query_type == "Approved Amount exceeds Expenditure":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_approved_amount" > "ratnagiri_final_total_expenditure"{block_filter}'

elif category == "PCI After Completion of Work":
    query_type = st.selectbox("Select Query Type", ["Greater than", "Less than", "Equal to"])
    pci_value = st.slider("Select PCI Value", min_value=0, max_value=100, value=50)

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if query_type == "Greater than":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_pci_after_completion_of_work" > {pci_value}{block_filter}'
    elif query_type == "Less than":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_pci_after_completion_of_work" < {pci_value}{block_filter}'
    elif query_type == "Equal to":
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_pci_after_completion_of_work" = {pci_value}{block_filter}'

elif category == "Current Status":
    current_status_groups = {
        'Work is Complete': ['Work is complete', 'Work done', 'Work done final', 'Work complete and final', 'Work complete final', 'The work is complete and final'],
        'In Progress': ['in progress'],
        '50% Work in Progress': ['50 percent work in progress'],
        'Budget Level': ['budget level'],
        'Final Bill Done': ['Final bill done'],
        'Final Payment Due': ['Final payment due in full'],
        'Work Physically Complete': ['Work physically complete final payable outstanding'],
        'Others': []  
    }

    engine = get_postgis_engine()
    with engine.connect() as connection:
        result = connection.execute(text('SELECT DISTINCT "ratnagiri_final_current_status" FROM "RN_DIV"'))
        all_statuses = [row[0] for row in result if row[0] not in sum(current_status_groups.values(), [])]

    current_status_groups['Others'] = all_statuses
    all_current_statuses = sum(current_status_groups.values(), [])
    selected_current_statuses = st.multiselect("Select Current Status", all_current_statuses)

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if selected_current_statuses:
        filtered_statuses = filter(None, selected_current_statuses)
        values_list = "', '".join(filtered_statuses)
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_current_status" IN (\'{values_list}\'){block_filter}'

    
    # Fetch all unique status values from the database
    engine = get_postgis_engine()
    with engine.connect() as connection:
        result = connection.execute(text('SELECT DISTINCT "ratnagiri_final_current_status" FROM "RN_DIV"'))
        all_statuses = [row[0] for row in result if row[0] not in sum(current_status_groups.values(), [])]
    
    current_status_groups['Others'] = all_statuses
    
    # Combine all statuses into a single list for selection
    all_current_statuses = sum(current_status_groups.values(), [])
    
    # Allow the user to select one or multiple current statuses
    selected_current_statuses = st.multiselect("Select Current Status", all_current_statuses)
    
    if selected_current_statuses:
        # Filter out None values
        filtered_statuses = filter(None, selected_current_statuses)
        # Convert the selected values to a string for SQL IN clause
        values_list = "', '".join(filtered_statuses)
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_current_status" IN (\'{values_list}\')'


# Initialize gdf to an empty GeoDataFrame
gdf = gpd.GeoDataFrame()

# Execute and display the query results
if query:
    st.subheader("Query Results")
    st.write(f"Query: {query}")
    
    try:
        gdf = fetch_data(query)
        st.write(gdf)
    except Exception as e:
        st.error(f"An error occurred while fetching data: {e}")

# Create the folium map
m = folium.Map(location=[17.0, 73.3], zoom_start=10)

# Optional layers
show_district_boundaries = st.checkbox("Show District Boundaries", value=True)
show_road_network = st.checkbox("Show Road Network", value=False)

# Create the folium map
m = folium.Map(location=[17.0, 73.3], zoom_start=10)

if show_district_boundaries:
    folium.GeoJson(
        district_gdf,
        name="District Boundaries",
        style_function=lambda x: {
            'color': 'blue',
            'weight': 2,
            'fillOpacity': 0.1,
        },
        highlight_function=lambda x: {'weight': 3, 'color': 'darkblue'},
        tooltip=folium.GeoJsonTooltip(fields=["NAME_3"], aliases=["District:"]),
    ).add_to(m)

if show_road_network:
    folium.GeoJson(
        road_network_gdf,
        name="Road Network",
        style_function=lambda x: {
            'color': 'green',
            'weight': 2,
            'fillOpacity': 0.1,
        },
        highlight_function=lambda x: {'weight': 3, 'color': 'darkgreen'},
        tooltip=folium.GeoJsonTooltip(fields=["DRRP_ROAD_"], aliases=["Road Type:"]),
    ).add_to(m)

# Add queried data layer to the map
if not gdf.empty:
    folium.GeoJson(
        gdf,
        name="Query Results",
        style_function=lambda x: {
            'color': 'red',
            'weight': 2,
            'fillOpacity': 0.1,
        },
        highlight_function=lambda x: {'weight': 3, 'color': 'darkred'},
        tooltip=folium.GeoJsonTooltip(
            fields=[
                'drrp_road_', 'ratnagiri_final_total_length', 'ratnagiri_final_scheme_name',
                'ratnagiri_final_total_expenditure', 'ratnagiri_final_current_status'
            ],
            aliases=[
                'Road Type:', 'Total Length:', 'Scheme Name:',
                'Total Expenditure:', 'Current Status:'
            ]
        ),
    ).add_to(m)

folium.LayerControl().add_to(m)

st_folium(m, width=900, height=800)

