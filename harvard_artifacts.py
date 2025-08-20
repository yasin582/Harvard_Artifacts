import streamlit as st
import mysql.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import json
import pandas as pd

# Database credentials (replace with your own)
DB_HOST = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com"
DB_PORT = 4000
DB_USER = "4JX5zZKcrMbbGKn.root"
DB_PASSWORD = "ali9zRfvEMFC0mIS"
DB_DATABASE = "GUVI_PROJECTS"

# Harvard Art Museums API Key
Api_key = "58fc1902-7fb7-47b5-96d1-fee5755705c4"

# API Base URL
BASE_URL = "https://api.harvardartmuseums.org/object"

# List of classifications to collect data from
classification = ["Drawings", "Paintings", "Vessels", "Sculpture", "Coins"]

# Set up the Streamlit App layout
st.title("Harvard's Artifacts Collection")
st.write("An end-to-end ETL and data exploration platform.")

# --- SECTION 1: DATA COLLECTION ---
st.header("1. Data Collection")
st.write("Select a classification to fetch data from the Harvard Art Museums API.")

# Initialize session state variables if they don't exist
if 'artifact_metadata' not in st.session_state:
    st.session_state.artifact_metadata = []
if 'artifact_media' not in st.session_state:
    st.session_state.artifact_media = []
if 'artifacts_colors' not in st.session_state:
    st.session_state.artifacts_colors = []

selected_classification = st.selectbox("Select a classification:", classification)

if st.button("Collect Data"):
    st.info(f"Collecting data for: {selected_classification}...")
    
    # Clear existing data before collecting new data
    st.session_state.artifact_metadata = []
    st.session_state.artifact_media = []
    st.session_state.artifacts_colors = []
    
    unique_objectids = set()
    
    def fetch_page(classification, page):
        """Fetches data for a single page."""
        params = {
            "apikey": Api_key,
            "size": 100,
            "page": page,
            "classification": classification
        }
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            return data.get("records", [])
        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching data for [{classification}] on page {page}: {e}")
            return []

    with st.spinner("Fetching data..."):
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Create tasks to fetch 25 pages concurrently to get up to 2500 unique artifacts
            futures = [executor.submit(fetch_page, selected_classification, page) for page in range(1, 26)]
            
            for future in as_completed(futures):
                if len(unique_objectids) >= 2500:
                    st.warning("Reached the artifact limit of 2500. Stopping data collection.")
                    break
                    
                records = future.result()
                
                if not records:
                    continue
                
                for record in records:
                    objectid = record.get("objectid")
                    
                    if objectid and objectid not in unique_objectids:
                        unique_objectids.add(objectid)
                        
                        # Data for artifact_metadata table
                        st.session_state.artifact_metadata.append({
                            "id": record.get("id"),
                            "title": record.get("title"),
                            "culture": record.get("culture"),
                            "period": record.get("period"),
                            "century": record.get("century"),
                            "medium": record.get("medium"),
                            "dimensions": record.get("dimensions"),
                            "description": record.get("description"),
                            "department": record.get("department"),
                            "classification": record.get("classification"),
                            "accessionyear": record.get("accessionyear"),
                            "accessionmethod": record.get("accessionmethod")
                        })
                        
                        # Data for artifact_media table
                        st.session_state.artifact_media.append({
                            "objectid": objectid,
                            "imagecount": record.get("imagecount"),
                            "mediacount": record.get("mediacount"),
                            "colorcount": record.get("colorcount"),
                            "media_rank": record.get("media_rank"),
                            "datebegin": record.get("datebegin"),
                            "dateend": record.get("dateend")
                        })
                        
                        # Data for artifact_colors table
                        colors = record.get("colors", [])
                        if isinstance(colors, list):
                            for color_data in colors:
                                st.session_state.artifacts_colors.append({
                                    "objectid": objectid,
                                    "color": color_data.get("color"),
                                    "spectrum": color_data.get("spectrum"),
                                    "hue": color_data.get("hue"),
                                    "percent": color_data.get("percent"),
                                    "css3": color_data.get("css3")
                                })
    
    st.success(f"Data collection complete! Found {len(st.session_state.artifact_metadata)} unique artifacts.")

# --- SECTION 2: DISPLAY COLLECTED DATA IN SCROLLABLE BOXES ---
st.header("2. Collected Data")
st.write("This is the data collected from the API, displayed in a scrollable JSON format.")

# Create three columns for the layout
col1, col2, col3 = st.columns(3)

# Display Metadata in a scrollable container
with col1:
    st.subheader("Metadata")
    with st.container(height=400):
        for item in st.session_state.artifact_metadata[:100]:
            st.json(item)

# Display Media in a scrollable container
with col2:
    st.subheader("Media")
    with st.container(height=400):
        for item in st.session_state.artifact_media[:100]:
            st.json(item)

# Display Colors in a scrollable container
with col3:
    st.subheader("Colors")
    with st.container(height=400):
        # Only display a small, manageable portion of the color data
        for item in st.session_state.artifacts_colors[:100]:
            st.json(item)
    
# --- SECTION 3: INSERT INTO SQL ---
if "artifact_metadata" in st.session_state and st.session_state.artifact_metadata:
    st.header("3. Insert into SQL")
    
    if st.button("Insert into SQL"):
        try:
            connection = mysql.connector.connect(host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",port = 4000,user = "4JX5zZKcrMbbGKn.root",password = "ali9zRfvEMFC0mIS",database = "GUVI_PROJECTS")
            mycursor = connection.cursor(buffered=True)

            # Check for existing IDs to avoid "Duplicate entry" error
            existing_ids_query = "SELECT id FROM artifact_metadata"
            mycursor.execute(existing_ids_query)
            existing_ids = set([row[0] for row in mycursor.fetchall()])

            new_metadata_records = [r for r in st.session_state.artifact_metadata if r.get('id') not in existing_ids]
            new_object_ids = {rec.get('id') for rec in new_metadata_records}
            
            if new_metadata_records:
                # Insert into artifact_metadata table
                sql_metadata = "INSERT INTO artifact_metadata (id, title, culture, period, century, medium, dimensions, description, department, classification, accessionyear, accessionmethod) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                metadata_values = [(r.get('id'), r.get('title'), r.get('culture'), r.get('period'), r.get('century'), r.get('medium'), r.get('dimensions'), r.get('description'), r.get('department'), r.get('classification'), r.get('accessionyear'), r.get('accessionmethod')) for r in new_metadata_records]
                mycursor.executemany(sql_metadata, metadata_values)

                # Insert into artifact_media table
                sql_media = "INSERT INTO artifact_media (objectid, imagecount, mediacount, colorcount, media_rank, datebegin, dateend) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                media_values = [(r.get('objectid'), r.get('imagecount'), r.get('mediacount'), r.get('colorcount'), r.get('media_rank'), r.get('datebegin'), r.get('dateend')) for r in st.session_state.artifact_media if r.get('objectid') in new_object_ids]
                mycursor.executemany(sql_media, media_values)

                # Insert into artifacts_colors table, the list is already limited
                sql_colors = "INSERT INTO artifacts_colors (objectid, color, spectrum, hue, percent, css3) VALUES (%s, %s, %s, %s, %s, %s)"
                colors_values = [(r.get('objectid'), r.get('color'), r.get('spectrum'), r.get('hue'), r.get('percent'), r.get('css3')) for r in st.session_state.artifacts_colors if r.get('objectid') in new_object_ids]
                mycursor.executemany(sql_colors, colors_values)
                
                connection.commit()
                st.success(f"Data inserted successfully! Added {len(new_metadata_records)} new records.")

                # This section shows the tables after insertion
                st.subheader("Inserted Data:")
                
                st.write("Artifacts Metadata")
                st.dataframe(pd.DataFrame(new_metadata_records), use_container_width=True)

                st.write("Artifacts Media")
                st.dataframe(pd.DataFrame(media_values), use_container_width=True)

                st.write("Artifacts Colors")
                st.dataframe(pd.DataFrame(colors_values), use_container_width=True)
                
            else:
                st.warning("No new records to insert. All artifacts collected already exist in the database.")
            
        except mysql.connector.Error as err:
            st.error(f"Error inserting data into MySQL: {err}")
        finally:
            if 'connection' in locals() and connection.is_connected():
                mycursor.close()
                connection.close()

# --- SECTION 4: SQL QUERIES ---
st.header("4. SQL Queries")
st.write("Select a predefined query to run on your database.")

query_options = {
    "1. List all artifacts from the 11th century belonging to Byzantine culture.": "SELECT * FROM artifact_metadata WHERE century = '11th century' AND culture = 'Byzantine'",
    "2. What are the unique cultures represented in the artifacts?": "SELECT DISTINCT culture FROM artifact_metadata",
    "3. List all artifacts from the Archaic Period.": "SELECT * FROM artifact_metadata WHERE period = 'Archaic Period'",
    "4. List artifact titles ordered by accession year in descending order.": "SELECT title, accessionyear FROM artifact_metadata ORDER BY accessionyear DESC",
    "5. How many artifacts are there per department?": "SELECT department, COUNT(*) as number_of_artifacts FROM artifact_metadata GROUP BY department",
    "6. Which artifacts have more than 3 images?": "SELECT objectid, title FROM artifact_metadata JOIN artifact_media ON artifact_metadata.id = artifact_media.objectid WHERE imagecount > 3",
    "7. What is the average rank of all artifacts?": "SELECT AVG(media_rank) FROM artifact_media",
    "8. Which artifacts have a higher mediacount than colorcount?": "SELECT objectid, title FROM artifact_metadata JOIN artifact_media ON artifact_metadata.id = artifact_media.objectid WHERE mediacount > colorcount",
    "9. List all artifacts created between 1500 and 1600.": "SELECT title FROM artifact_metadata JOIN artifact_media ON artifact_metadata.id = artifact_media.objectid WHERE datebegin >= 1500 AND dateend <= 1600",
    "10. How many artifacts have no media files?": "SELECT COUNT(*) FROM artifact_media WHERE mediacount = 0",
    "11. What are all the distinct hues used in the dataset?": "SELECT DISTINCT hue FROM artifacts_colors",
    "12. What are the top 5 most used colors by frequency?": "SELECT hue, COUNT(*) AS hue_count FROM artifacts_colors GROUP BY hue ORDER BY hue_count DESC LIMIT 5",
    "13. What is the average coverage percentage for each hue?": "SELECT hue, AVG(percent) AS average_coverage FROM artifacts_colors GROUP BY hue",
    "14. List all colors used for a given artifact ID.": "SELECT hue, color FROM artifacts_colors WHERE objectid = 1412",
    "15. What is the total number of color entries in the dataset?": "SELECT COUNT(*) FROM artifacts_colors",
    "16. List artifact titles and hues for all artifacts belonging to the Byzantine culture.": "SELECT m.title, c.hue FROM artifact_metadata m JOIN artifacts_colors c ON m.id = c.objectid WHERE m.culture = 'Byzantine'",
    "17. List each artifact title with its associated hues.": "SELECT m.title, c.hue FROM artifact_metadata m JOIN artifacts_colors c ON m.id = c.objectid",
    "18. Get artifact titles, cultures, and media ranks where the period is not null.": "SELECT m.title, m.culture, a.media_rank FROM artifact_metadata m JOIN artifact_media a ON m.id = a.objectid WHERE m.period IS NOT NULL",
    "19. Find artifact titles ranked in the top 10 that include the color hue 'Grey'.": "SELECT m.title, a.media_rank, c.hue FROM artifact_metadata m JOIN artifact_media a ON m.id = a.objectid JOIN artifacts_colors c ON m.id = c.objectid WHERE a.rank <= 10 AND c.hue = 'Grey'",
    "20. How many artifacts exist per classification, and what is the average media count for each?": "SELECT m.classification, COUNT(*) AS artifact_count, AVG(a.mediacount) AS avg_media FROM artifact_metadata m JOIN artifact_media a ON m.id = a.objectid GROUP BY m.classification",
    "Extra Query 1: Most common artifact titles": "SELECT title, COUNT(*) AS title_count FROM artifact_metadata GROUP BY title ORDER BY title_count DESC",
    "Extra Query 2: Earliest and latest artifacts": "SELECT title, accessionyear FROM artifact_metadata WHERE accessionyear = (SELECT MIN(accessionyear) FROM artifact_metadata) OR accessionyear = (SELECT MAX(accessionyear) FROM artifact_metadata)",
    "Extra Query 3: List all artifacts with a specific color from a specific culture": "SELECT m.title, m.culture, c.hue FROM artifact_metadata m JOIN artifact_media a ON m.id = a.objectid JOIN artifacts_colors c ON m.id = c.objectid WHERE m.culture = 'American' AND c.hue = 'Red';",
    "Extra Query 4: Find artifacts with no description": "SELECT id, title, classification FROM artifact_metadata WHERE description IS NULL;",
    "Extra Query 5:  Count artifacts with a specific medium" : "SELECT medium, COUNT(id) AS number_of_artifacts FROM artifact_metadata WHERE medium IS NOT NULL GROUP BY medium ORDER BY number_of_artifacts DESC;"
}

selected_query_key = st.selectbox("Select a query to run:", list(query_options.keys()))
selected_query = query_options[selected_query_key]
st.code(selected_query, language="sql")

if st.button("Run Query"):
    try:
        connection = mysql.connector.connect(host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",port = 4000,user = "4JX5zZKcrMbbGKn.root",password = "ali9zRfvEMFC0mIS",database = "GUVI_PROJECTS")
        mycursor = connection.cursor(buffered=True)
        
        mycursor.execute(selected_query)
        query_result = mycursor.fetchall()

        if query_result:
            headers = [i[0] for i in mycursor.description]
            # Convert the list of tuples into a DataFrame with headers for proper display
            df_result = pd.DataFrame(query_result, columns=headers)
            st.dataframe(df_result, use_container_width=True)
        else:
            st.warning("Query returned an empty result. There may be no data in your database that matches the query criteria.")

    except mysql.connector.Error as err:
        st.error(f"Error executing query: {err}")
    finally:
            if 'connection' in locals() and connection.is_connected():
                mycursor.close()

                connection.close()
