import streamlit as st
import geopandas as gpd
import folium
from streamlit_folium import st_folium
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import pandas as pd
from datetime import datetime
from urllib.parse import quote_plus

# Database configuration
database = {
    'host': "127.0.0.1",
    'port': 5432,  # Postgres default port
    'db': "testing1",
    'user': "shreeshnadgouda",
    'password': "yash1234",
}

# SQLAlchemy engine creation with connection pooling
def get_postgis_engine():
    url = (
        f'postgresql+psycopg2://'
        f'{database["user"]}:{quote_plus(database["password"])}'
        f'@{database["host"]}:{database["port"]}'
        f'/{database["db"]}'
    )
    engine = create_engine(url, poolclass=QueuePool, pool_size=5, max_overflow=10)
    return engine

# Fetch data from database
@st.cache_data(ttl=600)
def fetch_data(query):
    engine = get_postgis_engine()
    with engine.connect() as connection:
        gdf = gpd.read_postgis(text(query), con=connection, geom_col='geom')
    
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)  # Assuming the fetched data uses WGS84 CRS
    
    return gdf

# Fetch non-geometry data from database
@st.cache_data(ttl=600)
def fetch_non_geom_data(query):
    engine = get_postgis_engine()
    with engine.connect() as connection:
        df = pd.read_sql(query, con=connection)
    return df


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
category = st.selectbox("Select a Category", ["Road Length", "Date", "Road Type", "Block Name", "Scheme Name", "Category of Work", "Contractor Name", "Total Expenditure", "Approved Amount", "Compare Expenditure and Approved Amount", "PCI After Completion of Work", "Current Status","Analysis and Reporting"])

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
        'Asphalt Resurfacing': ['Asphalt Resurfacing', 'Asphalt Resurfacing.','Asphalt Resurfacing.','asphalting','asphalting','Asphaltiing','Improvement and asphalting.','Improvement, asphalting','Rocking, asphalting'],
        'Building a protective wall': ['Building a protective wall', 'Building a protective wall.','Building protective walls', 'Construction of a protective wall'],
        'Complete Road': ['Complete Road', 'Complete Road.', 'Complete Road. Under Surface Improvement Program'],
        'Drainage': ['Drainage', 'Drainage - Construction of a protective wall.', 'Drainage - Construction of a protective wall.'],
        'Rehabilitation': ['Rehabilitation', 'Rehabilitation.', 'Rehabilitation.  Asphalt Resurfacing'],
        'Surface': ['Surface', 'Surface Improvement'],
        'Road widening':['Road widening.','to widen.'],
        'Construction of bridge and gutters':['Construction of a bridge.','Construction of concrete gutters.'],
        'Filling potholes with laterite stones':['Filling potholes with laterite stones'],
        'Filling the pits with asphalt':['Filling the pits with asphalt'],
        'make way':['make way'],
        'Making the road':['Making the road'],
        'Paving and asphalting':['Paving and asphalting'],
        'Paving the road':['Paving the road'],
        'Paving, reinforcement and asphalting':['Paving, reinforcement and asphalting'],
        'Rebuilding the bridge.':['Rebuilding the bridge.','Rebuilding the Peacocks.','Repair of Peacocks','Repairing the hole.','To repair the hole','to repair the sacv'],
        'Reinforcement and asphalting':['Reinforcement and asphalting','Reinforcement, asphalting','Erection, reinforcement and asphalting','reinforcement and asphalting','Reinforcement and asphalting.'],
        'Renovation of asphalt, construction of retaining wall':['Renovation of asphalt, construction of retaining wall','Repair and construction of protective wall'],
        'Resurfacing':['Resurfacing'],
        'Road Improvement and Asphalting.':['Road Improvement and Asphalting.','Road strengthening and asphalting'],
        'Road improvement.':['Road improvement.','to improve'],
        'None':['None']
        
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



elif category == "Contractor Name":
    # Define the unique contractor names for the 'ratnagiri_final_contractor_name' category
    contractor_names_groups = [
    'Abhishek Ms. ask',
    'Adil Mahmmad Ajim Thakur',
    'Akhyaksha Shramjeevi, S.M.Labor.Institute Marya.Rajapur',
    'Akshay Ashok Pawar M.Po.Phansop Ta.Ratnagiri',
    'AM swamy',
    'amol balkrushna khandare',
    'Ashfaq Hajju',
    'Ask Mr. Abhishek Sudhir',
    'Avish Qutbuhaddin goalkeeper, Rs. Saundal',
    'Babu Habu Chavan M.Po.Pawas',
    'bettigiri',
    'Bhavesh Sonu Narkar M.Po.Saundal Dist.Rajapur',
    'Chairman Adarsh Mazdoor Co-Operative Society Marya Mu.Po. Ta.Lanja',
    'Chairman Dapoli Taluka Ma San Dapoli',
    'Chairman Prerna Mazur Sahkari Sansya. Marya M.Po. Anjani T. Village Ratnagiri District',
    'Chairman Rajapur Taluka M.S.S.Marya Mu.Po.Rajapur',
    'Chairman Shramjeevi Mazur .S. Sanstha. Marya. Raipatan Dist.Rajapur',
    'Chairman Sukai Devi M S San Pedhambe Chiplun',
    'Chairman Vireshwar Labor Co-operative Society Ltd.,Mu.Po. Ta.Chiplun Dist.Ratnagiri',
    'Chairman Vireshwar Mazur Sahkari Sanstha Marya..M.P.O. h Chiplun Dist.Ratnagiri',
    'Chairman, Bhagyodaya M.S. M.Po.Kumble Ta.Mandangaon',
    'Chairman, Shivtej Mazur with. Sanstha Phansop',
    'Dhaneshwar cont',
    'Dinesh Chandrakant Jathar',
    'Dipashree Construction',
    'Diptesh Prabhakar Kadam M.Po.Barsu Ta.Rajapur',
    'Five-star labourer',
    'Fuzel Ibzi',
    'Gaurav Patole',
    'Gaurav V. Patole',
    'Gaurav Vijay Solgaonkar M.Po.Lanja',
    'Hon. Sarpanch, Gram Panchayat Punas Tehsil Lanja',
    'Hon\'ble Sarpanch Gram Panchayat Vervali Khu.Padvan',
    'Hon\'ble Sarpanch, Gram Panchayat Panval, Ratnagiri',
    'Institution Marya with President, Siddharth Mazur. gods',
    'Jahid Khan',
    'Jan Seva Mazur Co., Ltd',
    'Kedardev Kisan M.S.No.Marya.Agve Dist.Chiplun',
    'Kedarling Knstr. Ta. Sangameshwar.',
    'Kedarlinga Construction',
    'Kedarlinga Construction Prop. Shri Mangesh Ya Shinde Bhadkamba',
    'Kondwadi Project M.S.No.Marya.Vashi T.Sangamashwar',
    'Ku.Yashraj Construction Prof.P.Kiraan Yashwant Tulsawdekar',
    'Labor Labor Asst. Institute Raipatan, Ta.Rajapur, Dist.Ratnagiri',
    'Laboring labourers',
    'Lalit Charudatta Kadam',
    'M.Yash Construction',
    'Mahalsa',
    'Mahalsa Constr.Propra Shri. Shankar Gu.Sevlani Ulhasnagar Thane',
    'Mahalsa Construction',
    'Mahalsa Construction Prof. Shankar Th. Sewlani Ulhasnagar Thane',
    'Mahalsa Construction Proprietary. Shankar Gurudas Sewalani Ta.Ratnagiri',
    'Manish Vasant Lingalat Mu.Po.Oni Ta .Rajapur',
    'Mauli Enterprises Kanjeevara',
    'Mhalsa Construction',
    'Milid Vijay More',
    'Mister. Suresh Bhagoji Jhor',
    'Motilal Nuru Rathod',
    'Mr. Afam et al. Thakur',
    'Mr. Akshay Ashok Pawar, M.P. Karbude, T. Dist. Ratnagiri',
    'Mr. Akshay Ramu Chavan',
    'Mr. Akshay Suryakant Desai',
    'Mr. Akshay Suryakant Desai.',
    'Mr. Amit Harishchandra Bandbe',
    'Mr. Anant Babasaheb Chaugule',
    'Mr. Aniket Rajendra Khatu',
    'Mr. Anil Babu Rathore',
    'Mr. Anil Maruti Salunke',
    'Mr. Anil Subhash asked',
    'Mr. Anup Narendra Gunijan M.Po. Kondhethar Sub-district Rajapur',
    'Mr. Arnun Narayan Kesrekar',
    'Mr. Ashfaq Ismail Haju M.Po. Talgaon District Rajapur',
    'Mr. Ashok Kalu Pawar',
    'Mr. Ashraf H. Mukri',
    'Mr. Babu Habu Chavan, M.Po.Pawas,T. Dist. Ratnagiri',
    'Mr. Bhavesh Sonu Narkar',
    'Mr. D. S. Padyar',
    'Mr. Devendra Dattaram fights',
    'Mr. Diptesh Prabhakar Kadam',
    'Mr. Diptesh Prabhakar Kadam M.P. Barsu Sub-district Rajapur, District Ratnagiri',
    'Mr. Gauri Charudatta Kadam',
    'Mr. Gurdas Subhash Desai.',
    'Mr. Karishma Chandrashekhar Bendkhale',
    'Mr. Kashinath Mohan Sakpal',
    'Mr. Ketan Nandkishore More',
    'Mr. Ketan Nandkishore More.',
    'Mr. Kumar Desu Naik',
    'Mr. Mahesh Prabhakar Desai',
    'Mr. Mayur Milind Bhingard.',
    'Mr. Mayur Milind Bhingarde',
    'Mr. Mohan Desu Rathod',
    'Mr. Motilal Naru Rathod M.Po.Pali',
    'Mr. Motilal Nuru Rathod',
    'Mr. Naresh Vishnu Sawant',
    'Mr. Nasir Hassan Mujawar M.Po. h Rajapur Dist. Ratnagiri',
    'Mr. Nikhil S. Khanwilkar',
    'Mr. Nikhil Santaji Khanwilkar',
    'Mr. Nitish Gopinath Kudali',
    'Mr. Nitish Kudali',
    'Mr. PA Salvi',
    'Mr. Patiram Khiru Rathod M.P.O. Khedshi District Ratnagiri',
    'Mr. Pradeep Mr. Neverrekar.',
    'Mr. Pradip Kumar Pandurang Kale',
    'Mr. Prashant Manohar Sawant',
    'Mr. Prathamesh Pradip Nikam M.Po.Agve,T. Chiplun, District Ratnagiri',
    'Mr. Rajesh Ramchandra Pawar',
    'Mr. S.G. Padyar',
    'Mr. Sachin Mahadev Pawar',
    'Mr. Sagar Manohar asked',
    'Mr. Sagar Shivaji Rathod',
    'Mr. Sagar Shivaji Rathod Mu.Po.Patgaon Ta.Sangmeshwar',
    'Mr. Sandesh Vijay Jagde',
    'Mr. Sanju Revnu Rathod',
    'Mr. Santosh Suresh Rathod',
    'Mr. Sarpanch, Gram Panchayat Phanswale, Ratnagiri',
    'Mr. Shailesh Suresh Jadyar',
    'Mr. Shivlal Khiru Rathod',
    'Mr. Siddesh Sadanand Vibhute',
    'Mr. Somsingh Jivlu Rathod',
    'Mr. Somsingh Jivlu Rathod M.P.O. Bhate Ta.Ratnagiri',
    'Mr. Sumedh Mahendra Mahadeek',
    'Mr. Vaibhav Kadam.',
    'Mr. Vaishnavi Vitthal Girkar',
    'Mr. Vikas Kisan Rathod',
    'Mr. Vishal Jaywant Shinde M.P. Shirgaon Distt. Ratnagiri',
    'Mr.Ashish Sandip Mohite',
    'Mr. Vishal Jaywant Shinde, M.P. Shirgaon District. Ratnagiri',
    'Mr.Ashish Sandip Mohite',
    'Mr.Ganesh Krishna Adoor',
    'Mr.Rajan Motiram Rathod M.P.O. Majgaon Ratnagiri',
    'Mr.Seddhesh Upendra Pendhari, M.P.O. Devrukh Dist.Ratnagiri',
    'Mr.Shital Navnath Todkar',
    'MRD Samant Construction',
    'Mrs. Sneha Madan Shinde',
    'Ms. Avinash Anant Topre',
    'Ms. Kiran Keshav Mestry',
    'Ms. Nirali Sunil Deshmukh',
    'Ms. Omkar Ramesh Divekar',
    'Ms. Satyavan Damodar Bhadane, M.P.O. Dabhade',
    'N. D. Construction',
    'N.D. Constrction',
    'Nilesh Balkrishna Doiphode',
    'Nilesh Balkrishna Doiphode, M.Po. Karvande',
    'Nitesh Rajaram Rathod',
    'Nitin Mahendra Nair',
    'Nitin Mahendra Nair, M.Po. Jaigad',
    'Nitin Mahendra Nayyar',
    'Nitin Sudhir Kadam',
    'Om Sai Ram Construction',
    'Omkareshwar Construction, M.P.O. Deoghar',
    'P. K. Infraprojects',
    'P.K Infra Project',
    'Padmashree Construction',
    'Padmashree Construction, Ms.Kiran Devgan Powar',
    'Pradip Construction, M.Po. Tolewadi, Ta.Sangamashwar',
    'Prafull Kumar Balu Surve',
    'Premanand Mangesh Khamkar',
    'Prithviraj Construction',
    'Pundlik Balu Surve',
    'Ramdas Mahadev Aaptekar',
    'Ravikumar R. Khan.',
    'Rayabandi Const.Pro.Llp M.Po. Mumbai',
    'Rohit Pundlik Rathod M.Po. Anjanari Ta.Rajapur Distt. Ratnagiri',
    'Rohit Ramakant Rathod',
    'Rohit Ramakant Rathod M.Po. Mu.Po. Sangmeshwar',
    'Sachin Sandip Patil',
    'Sachin Sandip Patil, M.Po. Pali',
    'Sadashiv Trimbak Parab',
    'Sadguru Construction',
    'Sadguru Construction M.Po. Dapoli',
    'Sadguru Construction Prop.Sadashiv Trim Parab',
    'Saikrupa Construction',
    'Samadhan Dnyaneshwar Surve',
    'Sanjay Shivaji Rathod M.Po.Murkutwadi',
    'Santosh Vasudev Parbundi',
    'Saptashrungi Construction',
    'Saptshrungi Construction Prop. Ganesh Sagar Udhavne Mu.Po. Devgarh',
    'Seemaji Construction',
    'Shailesh Keshav Rathod',
    'Shailesh Keshav Rathod M.Po. Masanwadi',
    'Shailesh Madhavrao Rathod M.Po. Akharoli',
    'Shantaram Ramchandra Rathod',
    'Shashi Kamal Construction, Mu.Po. Mumbai',
    'Shivaji Chintaman',
    'Shivtej M.S.M.Po. Sade',
    'Shraddha Construction',
    'Shraddha Construction, M.Po. Mumbai',
    'Shraddha Construction Prof. Shraddha Shahu',
    'Shree Contraction, M.Po. Anvayi Ta. Chiplun',
    'Shree Construction',
    'Shree Sai Samartha Construction',
    'Shree Sai Samarth Construction',
    'Shree Siddhivinayak Construction',
    'Shree Siddhivinayak Construction, Mu.Po. Devrukh',
    'Shree Swami Samarth Construction',
    'Shri Ganesh Construction',
    'Shri Ganesh Construction, M.Po. Phansop',
    'Shri Ganesh Construction, M.Po.Phasop T.Lanja',
    'Shri Maruti Construction',
    'Shri Maruti Construction, Prof. Ashok Purushottam Chavan, Mu.Po. Karvande',
    'Shri Mhatreshwar Construction Prop.Shashikant Thakre,M.Po.Devghar',
    'Shri Ramkrishna M.S.M.Po.Jaigad',
    'Shri Sai Construction',
    'Shri Samarth Construction, M.Po. Devghar',
    'Shri Siddhivinayak Construction',
    'Shri Siddhivinayak Construction Prof. Bhagvat Namdeorao Mauli, M.Po. Talgaon',
    'Shri Siddhivinayak Construction,M.Po.Talgaon, Distt. Rajapur',
    'Shri Swami Samarth Construction',
    'Shri Swami Samarth Construction, Prop. Mahendra Navnath Mahadik, Mu.Po. Mumbai',
    'Shriraj Const. Prof. T.R. Rathod',
    'Shriram Construction',
    'Siddhivinayak Construction',
    'Siddhivinayak Construction M.Po. Mumbai',
    'Siddhivinayak Construction, Prop. Kailas Yashwantrao Kadam, M.Po. Panval',
    'Siddhivinayak Construction, Prop. Sarita Yashwant Kadam',
    'Somnath Chintaman Shinde',
    'Sudhir Construction, M.Po. Phansop',
    'Sudhir Sitaram Nimbalkar',
    'Suman Singh',
    'Suman Singh Prop. Suman Singh, M.Po. Rajapur',
    'Suresh Construction',
    'Surya Construction Prop. Mr. Suryakant Yashwant Kadam',
    'Tularam Ramakant Rathod M.Po.Rajapur',
    'Vaishnavi Construction Prop. Shri. Suryakant,Mu.Po. Devghar',
    'Vasanti Construction',
    'Vishal Enterprises, M.Po. Mumbai',
    'Yash Construction',
    'Yash contraction',
    'Yash Cont'
]        
    selected_contractors = st.multiselect("Select Contractor Names", contractor_names_groups)

    block_filter = f' AND "block_name" = \'{selected_block}\'' if selected_block != "All" else ""

    if selected_contractors:
        contractor_list = "', '".join(selected_contractors)
        query = f'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_contractor_name" IN (\'{contractor_list}\'){block_filter}'


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

if category == "Analysis and Reporting":
    report_query = st.selectbox("Select Report Query", [
        "Total Length of Roads by Taluka",
        "Total Expenditure on Roads by Contractor",
        "Total Expenditure by Road Category",
        "Roads with Highest Approved Amount (Top 10)",
        "Roads with Lowest Approved Amount (Bottom 10)",
        "Roads with Delayed Completion",
        "Roads by Contractor and Current Status",
        "Count of Roads by Description of Work",
        "Total Length of Roads by Scheme Name",
        "Total Approved Amount by Contractor",
        "Average Expenditure per Road by Contractor",
        "Top 5 Contractors by Average PCI After Completion",
        "Roads with High Expenditure to Length Ratio (Top 10)",
        "Top 5 Schemes by Average Length of Roads",
        "Contractor Workload: Number of Projects and Total Length",
        "Top 10 Contractors with Most Roads Completed",
        "Bottom 10 Contractors with Fewest Roads Completed",
        "Count of Roads by Scheme Name",
        "Average Expenditure by Road Category",
        "Median Approved Amount by Contractor",
        "Total Length of Roads by Contractor",
        "Count of Roads by Status",
        "Average Length of Roads by Taluka",
        "Top 10 Roads by Expenditure per Kilometer",
        "Count of Roads by Department",
        "Average PCI After Completion by Taluka",
        "Total Expenditure by Block",
        "Contractor with Maximum Number of Roads",
        "Top 10 Roads by Total Expenditure",
        "Bottom 10 Roads by Total Expenditure",
        "Count of Roads by Category of Work",
        "Average Approved Amount by Scheme Name",
        "Total Expenditure by Road Owner"
    ])

    # Queries that don't require geometry
    non_geom_queries = [
        "Total Length of Roads by Taluka",
        "Total Expenditure on Roads by Contractor",
        "Total Expenditure by Road Category",
        "Roads with Highest Approved Amount (Top 10)",
        "Roads with Lowest Approved Amount (Bottom 10)",
        "Roads with Delayed Completion",
        "Roads by Contractor and Current Status",
        "Count of Roads by Description of Work",
        "Total Length of Roads by Scheme Name",
        "Total Approved Amount by Contractor",
        "Average Expenditure per Road by Contractor",
        "Top 5 Contractors by Average PCI After Completion",
        "Roads with High Expenditure to Length Ratio (Top 10)",
        "Top 5 Schemes by Average Length of Roads",
        "Contractor Workload: Number of Projects and Total Length",
        "Top 10 Contractors with Most Roads Completed",
        "Bottom 10 Contractors with Fewest Roads Completed",
        "Count of Roads by Scheme Name",
        "Average Expenditure by Road Category",
        "Median Approved Amount by Contractor",
        "Total Length of Roads by Contractor",
        "Count of Roads by Status",
        "Average Length of Roads by Taluka",
        "Top 10 Roads by Expenditure per Kilometer",
        "Count of Roads by Department",
        "Average PCI After Completion by Taluka",
        "Total Expenditure by Block",
        "Contractor with Maximum Number of Roads",
        "Top 10 Roads by Total Expenditure",
        "Bottom 10 Roads by Total Expenditure",
        "Count of Roads by Category of Work",
        "Average Approved Amount by Scheme Name",
        "Total Expenditure by Road Owner"
    ]

    # Define the queries
    queries = {
        "Total Length of Roads by Taluka": 'SELECT "ratnagiri_final_taluka" AS Taluka, SUM("ratnagiri_final_total_length") AS TotalLength FROM "RN_DIV" GROUP BY "ratnagiri_final_taluka"',
        "Total Expenditure on Roads by Contractor": 'SELECT "ratnagiri_final_contractor_name", SUM("ratnagiri_final_total_expenditure") AS TotalExpenditure FROM "RN_DIV" GROUP BY "ratnagiri_final_contractor_name"',
        "Total Expenditure by Road Category": 'SELECT "roadcatego", SUM("ratnagiri_final_total_expenditure") AS TotalExpenditure FROM "RN_DIV" GROUP BY "roadcatego"',
        "Roads with Highest Approved Amount (Top 10)": 'SELECT * FROM "RN_DIV" ORDER BY "ratnagiri_final_approved_amount" DESC LIMIT 10',
        "Roads with Lowest Approved Amount (Bottom 10)": 'SELECT * FROM "RN_DIV" ORDER BY "ratnagiri_final_approved_amount" ASC LIMIT 10',
        "Roads with Delayed Completion": 'SELECT * FROM "RN_DIV" WHERE "ratnagiri_final_current_status" = \'Delayed\'',
        "Roads by Contractor and Current Status": 'SELECT "ratnagiri_final_contractor_name", "ratnagiri_final_current_status", COUNT(*) AS RoadCount FROM "RN_DIV" GROUP BY "ratnagiri_final_contractor_name", "ratnagiri_final_current_status"',
        "Count of Roads by Description of Work": 'SELECT "ratnagiri_final_description_of_work", COUNT(*) AS RoadCount FROM "RN_DIV" GROUP BY "ratnagiri_final_description_of_work"',
        "Total Length of Roads by Scheme Name": 'SELECT "ratnagiri_final_scheme_name", SUM("ratnagiri_final_total_length") AS TotalLength FROM "RN_DIV" GROUP BY "ratnagiri_final_scheme_name"',
        "Total Approved Amount by Contractor": 'SELECT "ratnagiri_final_contractor_name", SUM("ratnagiri_final_approved_amount") AS TotalApprovedAmount FROM "RN_DIV" GROUP BY "ratnagiri_final_contractor_name"',
        "Average Expenditure per Road by Contractor": 'SELECT "ratnagiri_final_contractor_name", AVG("ratnagiri_final_total_expenditure") AS AverageExpenditure FROM "RN_DIV" GROUP BY "ratnagiri_final_contractor_name"',
        "Top 5 Contractors by Average PCI After Completion": 'SELECT "ratnagiri_final_contractor_name", AVG("ratnagiri_final_pci_after_completion_of_work") AS AveragePCI FROM "RN_DIV" GROUP BY "ratnagiri_final_contractor_name" ORDER BY AveragePCI DESC LIMIT 5',
        "Roads with High Expenditure to Length Ratio (Top 10)": 'SELECT *, ("ratnagiri_final_total_expenditure" / "ratnagiri_final_total_length") AS ExpenditureToLengthRatio FROM "RN_DIV" ORDER BY ExpenditureToLengthRatio DESC LIMIT 10',
        "Top 5 Schemes by Average Length of Roads": 'SELECT "ratnagiri_final_scheme_name", AVG("ratnagiri_final_total_length") AS AverageLength FROM "RN_DIV" GROUP BY "ratnagiri_final_scheme_name" ORDER BY AverageLength DESC LIMIT 5',
        "Contractor Workload: Number of Projects and Total Length": 'SELECT "ratnagiri_final_contractor_name", COUNT(*) AS NumberOfProjects, SUM("ratnagiri_final_total_length") AS TotalLength FROM "RN_DIV" GROUP BY "ratnagiri_final_contractor_name"',
        "Top 10 Contractors with Most Roads Completed": 'SELECT "ratnagiri_final_contractor_name", COUNT(*) AS CompletedRoads FROM "RN_DIV" WHERE "ratnagiri_final_current_status" = \'Completed\' GROUP BY "ratnagiri_final_contractor_name" ORDER BY CompletedRoads DESC LIMIT 10',
        "Bottom 10 Contractors with Fewest Roads Completed": 'SELECT "ratnagiri_final_contractor_name", COUNT(*) AS CompletedRoads FROM "RN_DIV" WHERE "ratnagiri_final_current_status" = \'Completed\' GROUP BY "ratnagiri_final_contractor_name" ORDER BY CompletedRoads ASC LIMIT 10',
        "Count of Roads by Scheme Name": 'SELECT "ratnagiri_final_scheme_name", COUNT(*) AS RoadCount FROM "RN_DIV" GROUP BY "ratnagiri_final_scheme_name"',
        "Average Expenditure by Road Category": 'SELECT "roadcatego", AVG("ratnagiri_final_total_expenditure") AS AverageExpenditure FROM "RN_DIV" GROUP BY "roadcatego"',
        "Median Approved Amount by Contractor": 'SELECT "ratnagiri_final_contractor_name", PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "ratnagiri_final_approved_amount") AS MedianApprovedAmount FROM "RN_DIV" GROUP BY "ratnagiri_final_contractor_name"',
        "Total Length of Roads by Contractor": 'SELECT "ratnagiri_final_contractor_name", SUM("ratnagiri_final_total_length") AS TotalLength FROM "RN_DIV" GROUP BY "ratnagiri_final_contractor_name"',
        "Count of Roads by Status": 'SELECT "ratnagiri_final_current_status", COUNT(*) AS RoadCount FROM "RN_DIV" GROUP BY "ratnagiri_final_current_status"',
        "Average Length of Roads by Taluka": 'SELECT "ratnagiri_final_taluka", AVG("ratnagiri_final_total_length") AS AverageLength FROM "RN_DIV" GROUP BY "ratnagiri_final_taluka"',
        "Top 10 Roads by Expenditure per Kilometer": 'SELECT *, ("ratnagiri_final_total_expenditure" / "ratnagiri_final_total_length") AS ExpenditurePerKm FROM "RN_DIV" ORDER BY ExpenditurePerKm DESC LIMIT 10',
        "Count of Roads by Department": 'SELECT "ratnagiri_final_department", COUNT(*) AS RoadCount FROM "RN_DIV" GROUP BY "ratnagiri_final_department"',
        "Average PCI After Completion by Taluka": 'SELECT "ratnagiri_final_taluka", AVG("ratnagiri_final_pci_after_completion_of_work") AS AveragePCI FROM "RN_DIV" GROUP BY "ratnagiri_final_taluka"',
        "Total Expenditure by Block": 'SELECT "block_name", SUM("ratnagiri_final_total_expenditure") AS TotalExpenditure FROM "RN_DIV" GROUP BY "block_name"',
        "Contractor with Maximum Number of Roads": 'SELECT "ratnagiri_final_contractor_name", COUNT(*) AS NumberOfRoads FROM "RN_DIV" GROUP BY "ratnagiri_final_contractor_name" ORDER BY NumberOfRoads DESC LIMIT 1',
        "Top 10 Roads by Total Expenditure": 'SELECT * FROM "RN_DIV" ORDER BY "ratnagiri_final_total_expenditure" DESC LIMIT 10',
        "Bottom 10 Roads by Total Expenditure": 'SELECT * FROM "RN_DIV" ORDER BY "ratnagiri_final_total_expenditure" ASC LIMIT 10',
        "Count of Roads by Category of Work": 'SELECT "ratnagiri_final_category_of_work", COUNT(*) AS RoadCount FROM "RN_DIV" GROUP BY "ratnagiri_final_category_of_work"',
        "Average Approved Amount by Scheme Name": 'SELECT "ratnagiri_final_scheme_name", AVG("ratnagiri_final_approved_amount") AS AverageApprovedAmount FROM "RN_DIV" GROUP BY "ratnagiri_final_scheme_name"',
        "Total Expenditure by Road Owner": 'SELECT "roadowner", SUM("ratnagiri_final_total_expenditure") AS TotalExpenditure FROM "RN_DIV" GROUP BY "roadowner"'
    }

    # Get the corresponding query for the selected report
    query = queries.get(report_query)

    # Execute the query based on whether it requires geometry or not
    if report_query in non_geom_queries:
        df = fetch_non_geom_data(query)
    else:
        df = fetch_data(query)

    # Display the results
    if report_query in non_geom_queries:
        st.write(df)
    else:
        st.map(df)

    
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