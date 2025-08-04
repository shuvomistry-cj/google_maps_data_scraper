import streamlit as st
import pandas as pd
import os
import zipfile
import tempfile
import time
import subprocess
import sys
from datetime import datetime
from main import Business, BusinessList, extract_coordinates_from_url
import threading
import queue
import json
import io

# Configure Streamlit page
st.set_page_config(
    page_title="Google Maps Data Scraper",
    page_icon="🗺️",
    layout="wide"
)

# Initialize session state
if 'scraping_progress' not in st.session_state:
    st.session_state.scraping_progress = 0
if 'scraping_status' not in st.session_state:
    st.session_state.scraping_status = "idle"
if 'scraped_data' not in st.session_state:
    st.session_state.scraped_data = None
if 'search_history' not in st.session_state:
    st.session_state.search_history = []

def load_search_history():
    """Load search history from file"""
    history_file = "search_history.json"
    if os.path.exists(history_file):
        try:
            with open(history_file, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_search_history(history):
    """Save search history to file"""
    history_file = "search_history.json"
    with open(history_file, 'w') as f:
        json.dump(history, f, indent=2)

def scrape_single_search(search_query, limit, progress_callback=None):
    """Scrape data for a single search query using subprocess"""
    try:
        # Run the main.py script with subprocess
        cmd = [sys.executable, "main.py", f"-s={search_query}", f"-t={limit}"]

        if progress_callback:
            progress_callback(0)

        # Launch subprocess and stream stdout line-by-line for live progress
        scraped_count = 0
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        start_time = time.time()
        with subprocess.Popen(cmd, cwd=os.getcwd(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env) as proc:
            for line in proc.stdout:
                # Detect each business scraped (main.py prints "Scraped Business:")
                if "Scraped Business:" in line:
                    scraped_count += 1
                    if progress_callback:
                        pct = int(min(99, (scraped_count / max(1, limit)) * 100))
                        elapsed = time.time() - start_time
                        rate = scraped_count / elapsed if scraped_count else 0
                        eta = (limit - scraped_count) / rate if rate else 0
                        progress_callback(pct)
                        if progress_callback:
                            progress_callback(pct)  # already sent
                        status = f"Scraped {scraped_count}/{limit} • ETA {int(eta)}s"
                        # pass via st.session_state for UI thread
                        st.session_state["live_status"] = status
            proc.wait()
            if proc.returncode != 0:
                raise Exception(f"Scraping failed (exit {proc.returncode})")
        if progress_callback:
            progress_callback(100)
        

        
        # Find the generated Excel file
        clean_search = search_query.strip().replace(' ', '_')
        expected_filename = f"google_maps_data_{clean_search}.xlsx"
        expected_path = os.path.join("output", expected_filename)
        
        if not os.path.exists(expected_path):
            raise Exception(f"Expected output file not found: {expected_path}")
        
        # Load the data from Excel to create BusinessList object
        df = pd.read_excel(expected_path)
        business_list = BusinessList()
        
        for _, row in df.iterrows():
            business = Business(
                name=row.get('name', ''),
                address=row.get('address', ''),
                website=row.get('website', ''),
                phone_number=row.get('phone_number', ''),
                reviews_count=row.get('reviews_count', ''),
                reviews_average=row.get('reviews_average', ''),
                latitude=row.get('latitude', ''),
                longitude=row.get('longitude', '')
            )
            business_list.business_list.append(business)
        
        if progress_callback:
            progress_callback(100)
        
        return business_list
        
    except Exception as e:
        raise Exception(f"Scraping error: {str(e)}")

def scrape_multiple_searches(search_queries, limit, progress_callback=None):
    """Scrape data for multiple search queries using subprocess"""
    try:
        # Create a temporary input file
        temp_input_file = "temp_input.txt"
        with open(temp_input_file, 'w') as f:
            for query in search_queries:
                if query.strip():
                    f.write(query.strip() + '\n')
        
        if progress_callback:
            progress_callback(0)
        expected_total = limit * len(search_queries)
        cmd = [sys.executable, "main.py", f"-t={limit}"]
        
        # Temporarily rename input.txt if it exists
        input_backup = None
        if os.path.exists("input.txt"):
            input_backup = "input_backup.txt"
            os.rename("input.txt", input_backup)
        
        # Copy temp file to input.txt
        os.rename(temp_input_file, "input.txt")
        
        try:
            env = os.environ.copy(); env["PYTHONUNBUFFERED"] = "1"
            scraped_total = 0
            start_time = time.time()
            with subprocess.Popen(cmd, cwd=os.getcwd(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env) as proc:
                for line in proc.stdout:
                    if "Scraped Business:" in line:
                        scraped_total += 1
                        if progress_callback:
                            pct = int(min(99, (scraped_total / max(1, expected_total)) * 100))
                            elapsed = time.time() - start_time
                            rate = scraped_total / elapsed if scraped_total else 0
                            eta = (expected_total - scraped_total) / rate if rate else 0
                            progress_callback(pct)
                            st.session_state["live_status_multi"] = f"Scraped {scraped_total}/{expected_total} • ETA {int(eta)}s"
                proc.wait()
                if proc.returncode != 0:
                    raise Exception(f"Multi-search scraping failed (exit {proc.returncode})")
            

            
            # Collect all generated Excel files
            all_results = {}
            output_dir = "output"
            
            if os.path.exists(output_dir):
                for query in search_queries:
                    query = query.strip()
                    if query:
                        clean_search = query.replace(' ', '_')
                        expected_filename = f"google_maps_data_{clean_search}.xlsx"
                        expected_path = os.path.join(output_dir, expected_filename)
                        
                        if os.path.exists(expected_path):
                            # Load the data from Excel
                            df = pd.read_excel(expected_path)
                            business_list = BusinessList()
                            
                            for _, row in df.iterrows():
                                business = Business(
                                    name=row.get('name', ''),
                                    address=row.get('address', ''),
                                    website=row.get('website', ''),
                                    phone_number=row.get('phone_number', ''),
                                    reviews_count=row.get('reviews_count', ''),
                                    reviews_average=row.get('reviews_average', ''),
                                    latitude=row.get('latitude', ''),
                                    longitude=row.get('longitude', '')
                                )
                                business_list.business_list.append(business)
                            
                            all_results[query] = business_list
            
            if progress_callback:
                progress_callback(100)
            
            return all_results
            
        finally:
            # Restore original input.txt if it existed
            if os.path.exists("input.txt"):
                os.remove("input.txt")
            if input_backup:
                os.rename(input_backup, "input.txt")
                
    except Exception as e:
        raise Exception(f"Multi-search error: {str(e)}")

def create_zip_file(results_dict):
    """Create a zip file containing all Excel files"""
    temp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(temp_dir, "google_maps_data.zip")
    
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for search_query, business_list in results_dict.items():
            # Create Excel file
            clean_search = search_query.strip().replace(' ', '_').replace('/', '_')
            filename = f"google_maps_data_{clean_search}"
            excel_path = business_list.save_to_excel(filename)
            
            # Add to zip
            zipf.write(excel_path, f"{filename}.xlsx")
    
    return zip_path

# Main App
st.title("🗺️ Google Maps Data Scraper")
st.markdown("---")

# Create tabs
tab1, tab2, tab3 = st.tabs(["🔍 Single Search", "📋 Multi Search", "📊 History"])

# Tab 1: Single Search
with tab1:
    st.header("Single Search")
    st.markdown("Enter a search query to scrape Google Maps data")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_query = st.text_input(
            "What and Where",
            placeholder="e.g., Hotels in New York",
            help="Enter your search query (what you're looking for and where)"
        )
    
    with col2:
        limit = st.number_input(
            "Limit",
            min_value=1,
            max_value=100,
            value=20,
            help="Maximum number of results to scrape"
        )
    
    if st.button("🚀 Start Scraping", type="primary", key="single_search"):
        if search_query:
            st.session_state.scraping_status = "running"
            st.session_state.scraping_progress = 0
            
            # Progress bar
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def update_progress(progress):
                st.session_state.scraping_progress = progress
                progress_bar.progress(progress)
                status_text.text(f"Scraping in progress... {progress}%")
            
            try:
                status_text.text("Starting scraper...")
                business_list = scrape_single_search(search_query, limit, update_progress)
                
                st.session_state.scraping_status = "completed"
                st.session_state.scraped_data = business_list
                
                # Add to history
                history_entry = {
                    "timestamp": datetime.now().isoformat(),
                    "type": "single",
                    "query": search_query,
                    "limit": limit,
                    "results_count": len(business_list.business_list)
                }
                st.session_state.search_history.append(history_entry)
                save_search_history(st.session_state.search_history)
                
                progress_bar.progress(100)
                status_text.text("✅ Scraping completed!")
                
                st.success(f"Successfully scraped {len(business_list.business_list)} businesses!")
                
                # Show preview
                if business_list.business_list:
                    st.subheader("Preview of scraped data:")
                    df = business_list.dataframe()
                    st.dataframe(df.head())
                    
                    # Download button
                    clean_search = search_query.strip().replace(' ', '_')
                    filename = f"google_maps_data_{clean_search}"
                    excel_path = business_list.save_to_excel(filename)
                    
                    with open(excel_path, "rb") as file:
                        st.download_button(
                            label="📥 Download Excel File",
                            data=file.read(),
                            file_name=f"{filename}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                
            except Exception as e:
                st.session_state.scraping_status = "error"
                st.error(f"An error occurred: {str(e)}")
                status_text.text("❌ Scraping failed!")
        else:
            st.warning("Please enter a search query!")

# Tab 2: Multi Search
with tab2:
    st.header("Multi Search")
    st.markdown("Upload a text file with multiple search queries (one per line)")
    
    uploaded_file = st.file_uploader(
        "Choose input.txt file",
        type=['txt'],
        help="Upload a text file with one search query per line"
    )
    
    if uploaded_file is not None:
        # Read and display file content
        content = uploaded_file.read().decode('utf-8')
        search_queries = [line.strip() for line in content.split('\n') if line.strip()]
        
        st.info(f"Found {len(search_queries)} search queries:")
        for i, query in enumerate(search_queries, 1):
            st.write(f"{i}. {query}")

        limit_multi = st.number_input(
            "Limit per search",
            min_value=1,
            max_value=100,
            value=20,
            help="Maximum number of results to scrape for each search query"
        )
        
        if st.button("🚀 Start Multi Scraping", type="primary", key="multi_search"):
            st.session_state.scraping_status = "running"
            st.session_state.scraping_progress = 0
            
            # Progress bar
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def update_progress(progress):
                st.session_state.scraping_progress = progress
                progress_bar.progress(progress)
                status_text.text(f"Scraping in progress... {progress}%")
            
            try:
                status_text.text("Starting multi-search scraper...")
                results_dict = scrape_multiple_searches(search_queries, limit_multi, update_progress)
                
                st.session_state.scraping_status = "completed"
                
                # Add to history
                total_results = sum(len(bl.business_list) for bl in results_dict.values())
                history_entry = {
                    "timestamp": datetime.now().isoformat(),
                    "type": "multi",
                    "queries": search_queries,
                    "limit": limit_multi,
                    "results_count": total_results
                }
                st.session_state.search_history.append(history_entry)
                save_search_history(st.session_state.search_history)
                
                progress_bar.progress(100)
                status_text.text("✅ Multi-search scraping completed!")
                
                st.success(f"Successfully scraped data for {len(results_dict)} search queries!")
                
                # Show results summary
                st.subheader("Results Summary:")
                for query, business_list in results_dict.items():
                    st.write(f"• **{query}**: {len(business_list.business_list)} businesses")
                
                # Create and offer zip download
                zip_path = create_zip_file(results_dict)
                
                with open(zip_path, "rb") as file:
                    st.download_button(
                        label="📦 Download All Excel Files (ZIP)",
                        data=file.read(),
                        file_name="google_maps_data_multi.zip",
                        mime="application/zip"
                    )
                
            except Exception as e:
                st.session_state.scraping_status = "error"
                st.error(f"An error occurred: {str(e)}")
                status_text.text("❌ Multi-search scraping failed!")

# Tab 3: History
with tab3:
    st.header("Search History")
    
    # Load history
    if not st.session_state.search_history:
        st.session_state.search_history = load_search_history()
    
    if st.session_state.search_history:
        st.markdown(f"**Total searches performed:** {len(st.session_state.search_history)}")
        
        # Display history
        for i, entry in enumerate(reversed(st.session_state.search_history), 1):
            with st.expander(f"Search #{len(st.session_state.search_history) - i + 1} - {entry['type'].title()} Search"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Timestamp:** {entry['timestamp']}")
                    st.write(f"**Type:** {entry['type'].title()}")
                    st.write(f"**Results:** {entry['results_count']} businesses")
                
                with col2:
                    if entry['type'] == 'single':
                        st.write(f"**Query:** {entry['query']}")
                        st.write(f"**Limit:** {entry['limit']}")

                        # Download button for single search result
                        clean_search = entry['query'].strip().replace(' ', '_')
                        excel_path = os.path.join('output', f'google_maps_data_{clean_search}.xlsx')
                        if os.path.exists(excel_path):
                            with open(excel_path, 'rb') as _f:
                                st.download_button(
                                    label="📥 Download Excel",
                                    data=_f.read(),
                                    file_name=f"google_maps_data_{clean_search}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key=f"history_single_{i}"
                                )
                        else:
                            st.warning("File not found in output folder.")
                    else:
                        st.write(f"**Queries ({len(entry['queries'])})**:")
                        for j, query in enumerate(entry['queries'], 1):
                            st.write(f"{j}. {query}")

                        # Download button for multi search ZIP
                        import zipfile
                        from io import BytesIO
                        zip_buffer = BytesIO()
                        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            for q in entry['queries']:
                                clean_q = q.strip().replace(' ', '_')
                                file_path = os.path.join('output', f'google_maps_data_{clean_q}.xlsx')
                                if os.path.exists(file_path):
                                    zipf.write(file_path, arcname=os.path.basename(file_path))
                        if zip_buffer.getbuffer().nbytes > 0:
                            st.download_button(
                                label="📦 Download ZIP",
                                data=zip_buffer.getvalue(),
                                file_name="google_maps_data_multi.zip",
                                mime="application/zip",
                                key=f"history_multi_{i}"
                            )
                        else:
                            st.warning("No result files found in output folder.")
        
        # Clear history button
        if st.button("🗑️ Clear History", type="secondary"):
            st.session_state.search_history = []
            save_search_history([])
            st.success("History cleared!")
            st.rerun()
    else:
        st.info("No search history found. Start scraping to see your search history here!")

# Footer
st.markdown("---")
st.markdown("Built with ❤️ using Streamlit and Playwright")
