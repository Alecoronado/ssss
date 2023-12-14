import streamlit as st
import pandas as pd
from streamlit.logger import get_logger
import altair as alt
import re
from datetime import datetime
import threading
import io
import numpy as np
from dateutil.relativedelta import relativedelta

LOGGER = st.logger.get_logger(__name__)
_lock = threading.Lock()

# URLs de las hojas de Google Sheets
sheet_url_proyectos = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSHedheaRLyqnjwtsRvlBFFOnzhfarkFMoJ04chQbKZCBRZXh_2REE3cmsRC69GwsUK0PoOVv95xptX/pub?gid=2084477941&single=true&output=csv"
sheet_url_operaciones = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSHedheaRLyqnjwtsRvlBFFOnzhfarkFMoJ04chQbKZCBRZXh_2REE3cmsRC69GwsUK0PoOVv95xptX/pub?gid=1468153763&single=true&output=csv"
sheet_url_desembolsos = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSHedheaRLyqnjwtsRvlBFFOnzhfarkFMoJ04chQbKZCBRZXh_2REE3cmsRC69GwsUK0PoOVv95xptX/pub?gid=1657640798&single=true&output=csv"

# Inicializar la aplicación de Streamlit
st.title("Aplicación de Preprocesamiento de Datos")

# Función para cargar los datos desde las hojas de Google Sheets
def load_data(url):
    with _lock:
        return pd.read_csv(url)
    
# Cargar los datos
df_proyectos = load_data(sheet_url_proyectos)
df_operaciones = load_data(sheet_url_operaciones)
df_operaciones_desembolsos = load_data(sheet_url_desembolsos)
    
    
# Función para convertir el monto a un número flotante
def convert_to_float(monto_str):
    try:
        # Asumimos que el separador de miles es el punto y el separador decimal es la coma
        # Primero eliminamos el punto de los miles
        monto_str = monto_str.replace('.', '')
        # Luego reemplazamos la coma decimal por un punto
        monto_str = monto_str.replace(',', '.')
        return float(monto_str)
    except ValueError:
        return np.nan

# Función para procesar los datos
def process_data(df_proyectos, df_operaciones, df_operaciones_desembolsos):
    # Preparar los DataFrames seleccionando las columnas requeridas
    df_proyectos = df_proyectos[['NoProyecto', 'IDAreaPrioritaria', 'IDAreaIntervencion']]
    df_operaciones = df_operaciones[['NoProyecto', 'NoOperacion', 'IDEtapa', 'Alias', 'Pais', 'FechaVigencia', 'Estado', 'AporteFONPLATAVigente']]
    df_operaciones_desembolsos = df_operaciones_desembolsos[['IDDesembolso', 'NoOperacion', 'Monto', 'FechaEfectiva']]

    # Convertir la columna 'Monto' a numérico
    df_operaciones_desembolsos['Monto'] = df_operaciones_desembolsos['Monto'].apply(convert_to_float)

    merged_df = pd.merge(df_operaciones_desembolsos, df_operaciones, on='NoOperacion', how='left')
    merged_df = pd.merge(merged_df, df_proyectos, on='NoProyecto', how='left')
    

    # Convierte las columnas 'FechaEfectiva' y 'FechaVigencia' al formato correcto
    merged_df['FechaEfectiva'] = pd.to_datetime(merged_df['FechaEfectiva'], dayfirst=True, errors='coerce')
    merged_df['FechaVigencia'] = pd.to_datetime(merged_df['FechaVigencia'], dayfirst=True, errors='coerce')

    # Calculate the difference in years as a float first
    merged_df['Ano'] = ((merged_df['FechaEfectiva'] - merged_df['FechaVigencia']).dt.days / 365).fillna(-1)

    # Filter to exclude rows where 'Ano' is negative
    filtered_df = merged_df[merged_df['Ano'] >= 0]

    # Convert 'Ano' to integer
    filtered_df['Ano'] = filtered_df['Ano'].astype(int)

    # Write the filtered dataframe to the Streamlit app
    st.write(filtered_df)

    
    # Realizar cálculos utilizando 'AporteFONPLATAVigente' y 'IDAreaPrioritaria'
    result_df = filtered_df.groupby(['IDEtapa', 'Ano'])['Monto'].sum().reset_index()
    result_df['Monto Acumulado'] = result_df.groupby(['IDEtapa'])['Monto'].cumsum().reset_index(drop=True)
    result_df['Porcentaje del Monto'] = result_df.groupby(['IDEtapa'])['Monto'].apply(lambda x: x / x.sum() * 100).reset_index(drop=True)
    result_df['Porcentaje del Monto Acumulado'] = result_df.groupby(['IDEtapa'])['Monto Acumulado'].apply(lambda x: x / x.max() * 100).reset_index(drop=True)

    st.write(result_df)

# Procesar los datos
resultado_df = process_data(df_proyectos, df_operaciones, df_operaciones_desembolsos)

# Cargar los datos
with st.spinner('Cargando datos...'):
    df_proyectos = load_data(sheet_url_proyectos)
    df_operaciones = load_data(sheet_url_operaciones)
    df_operaciones_desembolsos = load_data(sheet_url_desembolsos)

def dataframe_to_excel_bytes(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Resultados', index=False)
    output.seek(0)
    return output

def run():
    st.set_page_config(
        page_title="Desembolsos",
        page_icon="👋",
    )

    st.title("Desembolsos de Proyectos📊")
    st.write("Explora las métricas relacionadas con los desembolsos cargando los datos desde Google Sheets.")

    # Procesar datos desde Google Sheets
    result_df = process_data()
    if not result_df.empty:
        st.write(result_df)
        
        # Convertir el DataFrame a bytes y agregar botón de descarga
        excel_bytes = dataframe_to_excel_bytes(result_df)
        st.download_button(
            label="Descargar DataFrame en Excel",
            data=excel_bytes,
            file_name="resultados_desembolsos.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )


