import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from streamlit_option_menu import option_menu

st.set_page_config(layout="wide", page_title="ARGO RAG Chatbot")

# Apply global dark theme for Plotly
pio.templates.default = "plotly_dark"

# import and initialize your RAG pipeline.
# ...from your_backend import RAGPipeline
# rag_model = RAGPipeline()
st.session_state['rag_model'] = 'RAG Model Backend'

st.title("ARGO Float Data Chatbot")
st.markdown("Ask questions about oceanographic data using natural language.")

# Initialize session state for the selected page
if 'selected_page' not in st.session_state:
    st.session_state['selected_page'] = "Chat Interface"

# Find the index of the currently selected page
selected_index = ["Chat Interface", "Data Visualization"].index(st.session_state['selected_page'])

selected = option_menu(
    menu_title=None,
    options=["Chat Interface", "Data Visualization"],
    icons=["chat-dots", "graph-up"],
    menu_icon="cast",
    default_index=selected_index,
    orientation="horizontal",
    styles={
        "container": {"padding": "0", "border-radius": "10px"},
        "icon": {"color": "#4d908e", "font-size": "20px"},
        "nav-link": {"font-size": "16px", "text-align": "center", "margin": "0px"},
        "nav-link-selected": {"background-color": "#d1e2e8", "color": "#2c3e50"}
    }
)

# Update the session state with the new selection
if selected != st.session_state["selected_page"]:
    st.session_state["selected_page"] = selected
    st.rerun()

# Chat Interface Page (RAG Integration) ---
if st.session_state['selected_page'] == "Chat Interface":
    st.header("Chat with the ARGO Data Assistant")
    st.markdown("e.g., *Show me salinity profiles near the equator in March 2023*")
    
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat messages from history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Accept user input
    if prompt := st.chat_input("What do you want to know about ARGO floats?"):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Display user message in chat message container
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Simulate AI response from the RAG pipeline
        with st.chat_message("assistant"):
            with st.spinner("Retrieving and generating response..."):
                # --- The Core RAG Integration Point ---
                # This is where your Streamlit frontend calls your backend logic.
                # The backend would perform:
                # 1. Retrieval: Query ChromaDB for relevant data chunks
                # 2. Augmentation: Combine the query with retrieved context
                # 3. Generation: Send the augmented prompt to the LLM (e.g., Gemini)
                # 4. Response: Get the final, human-readable answer
                
                # a placeholder response to show the flow.
                response_text = "Thank you for your question. My RAG pipeline is working to retrieve the most relevant ARGO data to answer your query. The full system will provide a detailed response here, based on information from the ChromaDB vector store."
                
                st.markdown(response_text)
                
                # display a table or graph from the retrieved data if your backend returns it.
                # Example:
                # if retrieved_data is not None:
                #     st.dataframe(retrieved_data)

elif st.session_state['selected_page'] == "Data Visualization":
    st.header("Interactive ARGO Data Visualizations")
    st.markdown("Explore various visualizations of ARGO float data.")

    # Dummy data for demo
    dummy_data = {
        'latitude': [-48.136],
        'longitude': [128.389],
        'temperature': [9.198],
        'salinity': [34.479946],
        'pressure': [4.3999996],
    }
    df_argo = pd.DataFrame(dummy_data)
    
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Temperature vs Depth Profile")
        fig_depth = px.scatter(
            df_argo,
            x='temperature',
            y='pressure',
            color='salinity',
            title="Temperature vs Depth Profile",
            color_continuous_scale=px.colors.sequential.Teal   # Use teal palette instead of rainbow
        )
        fig_depth.update_yaxes(autorange="reversed", title="Pressure (dbar)")
        fig_depth.update_layout(
            xaxis=dict(showgrid=True, gridcolor="#aaaaaa", zerolinecolor="#aaaaaa"),
            yaxis=dict(showgrid=True, gridcolor="#aaaaaa", zerolinecolor="#aaaaaa"),
            plot_bgcolor="#1e2a30",    # Dark plot background
            paper_bgcolor="#1e2a30"    # Dark figure background
        )
        st.plotly_chart(fig_depth, use_container_width=True)
    with col2:
        st.subheader("ARGO Float Locations")
        fig_map = px.scatter_geo(
            df_argo,
            lat='latitude',
            lon='longitude',
            projection="natural earth",
            title="ARGO Float Locations",
        )
        # Make map background brighter
        fig_map.update_geos(
            showland=True, landcolor="lightgray",
            showocean=True, oceancolor="lightblue",
            showlakes=True, lakecolor="lightblue",
            showcountries=True, countrycolor="white"
        )
        fig_map.update_layout(
            plot_bgcolor="#1e2a30",    # Dark plot background
            paper_bgcolor="#1e2a30"    # Dark figure background
        )
        st.plotly_chart(fig_map, use_container_width=True)
    
    st.subheader("Profile Data")
    st.dataframe(df_argo)
