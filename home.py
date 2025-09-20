import streamlit as st
import plotly.io as pio

st.set_page_config(layout="wide", page_title="ARGO RAG Chatbot")

# Add the following code to inject the custom CSS
with open("style.css") as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Apply global dark theme for Plotly
pio.templates.default = "plotly_dark"

# Placeholder for your RAG model initialization
# This can be moved to a shared module if needed by multiple pages
# st.session_state['rag_model'] = 'RAG Model Backend'

st.title("ARGO Float Data Chatbot")
st.markdown("Ask questions about oceanographic data using natural language.")
st.markdown("Use the navigation sidebar on the left to select a page.")