import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

def data_visualization():
    st.header("Interactive ARGO Data Visualizations")
    st.markdown("Explore various visualizations of ARGO float data.")

    # Dummy data for demo
    dummy_data = {
        'latitude': [-48.136, 15.0, 5.0],
        'longitude': [128.389, 75.0, 90.0],
        'temperature': [9.198, 28.5, 29.1],
        'salinity': [34.479946, 35.2, 34.8],
        'pressure': [4.3999996, 50.0, 10.0],
    }
    df_argo = pd.DataFrame(dummy_data)
    
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("Temperature vs Depth Profile")
        fig_depth = px.scatter(
            df_argo,
            x='temperature',
            y='pressure',
            color='salinity',
            title="Temperature vs Depth Profile",
            color_continuous_scale=px.colors.sequential.Teal,
            width=600,
            height=600
        )
        fig_depth.update_yaxes(autorange="reversed", title="Pressure (dbar)")
        fig_depth.update_layout(
            xaxis=dict(showgrid=True, gridcolor="#aaaaaa", zerolinecolor="#aaaaaa"),
            yaxis=dict(showgrid=True, gridcolor="#aaaaaa", zerolinecolor="#aaaaaa"),
            plot_bgcolor="rgba(0,0,0,0)",  # Set plot background to transparent
            paper_bgcolor="rgba(0,0,0,0)" # Set paper background to transparent
        )
        st.plotly_chart(fig_depth, use_container_width=False)

    with col2:
        st.subheader("ARGO Float Locations")
        fig_map = px.scatter_geo(
            df_argo,
            lat='latitude',
            lon='longitude',
            projection="natural earth",
            title="ARGO Float Locations",
            width=600,
            height=600
        )
        fig_map.update_geos(
            scope="asia",
            center=dict(lat=20, lon=80),
            projection_scale=1.5,
            showland=True, 
            landcolor="#7f8c8d",
            showocean=True, 
            oceancolor="#2c3e50",
            showlakes=True, 
            lakecolor="#2c3e50",
            showcountries=True, 
            countrycolor="#212121",
            countrywidth=1.5
        )
        fig_map.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",  # Set plot background to transparent
            paper_bgcolor="rgba(0,0,0,0)" # Set paper background to transparent
        )
        st.plotly_chart(fig_map, use_container_width=False)
    
    st.subheader("Profile Data")
    st.dataframe(df_argo)

data_visualization()