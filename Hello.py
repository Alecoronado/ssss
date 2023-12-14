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


    result_df = merged_df.groupby(['IDEtapa', 'Ano', 'IDDesembolso', 'AporteFONPLATAVigente'])['Monto'].sum().reset_index()
    result_df['Monto Acumulado'] = result_df.groupby(['IDEtapa'])['Monto'].cumsum().reset_index(drop=True)
    result_df['Porcentaje del Monto'] = result_df['Monto'] / result_df['AporteFONPLATAVigente'] * 100
    result_df['Porcentaje del Monto Acumulado'] = result_df['Monto Acumulado'] / result_df['AporteFONPLATAVigente'] * 100
    st.write(df_final)

    # Filtrar para excluir años negativos
    df_final = df_final[df_final['Ano'] >= 0]

    # Agrupar por 'Ano' y 'IDEtapa' y sumar los montos
    df_grouped = df_final.groupby(['Ano', 'IDEtapa'], as_index=False)['Monto'].sum()

    # Unir los resultados de la suma con las columnas adicionales de 'df_final'
    # Primero, se eliminan duplicados para evitar múltiples filas con la misma 'IDEtapa' y 'Ano'
    df_unique = df_final.drop_duplicates(subset=['Ano', 'IDEtapa'])
    # Luego, se realiza un merge para añadir las columnas adicionales
    result_df = pd.merge(df_grouped, df_unique, on=['Ano', 'IDEtapa'], how='left')

    # Seleccionar las columnas que quieres mantener en result_df
    result_df = result_df[['Ano', 'IDEtapa', 'Monto', 'NoOperacion', 'Alias', 'Pais', 'IDAreaPrioritaria', 'IDAreaIntervencion']]

    return result_df

# Cargar los datos
df_proyectos = load_data(sheet_url_proyectos)
df_operaciones = load_data(sheet_url_operaciones)
df_operaciones_desembolsos = load_data(sheet_url_desembolsos)

# Procesar los datos
result_df = process_data(df_proyectos, df_operaciones, df_operaciones_desembolsos)

# Mostrar el DataFrame resultante en la aplicación
st.write("DataFrame Resumido con Sumas de Monto por Año y Etapa:")
st.dataframe(result_df)
    

# Cargar los datos
df_proyectos = load_data(sheet_url_proyectos)
df_operaciones = load_data(sheet_url_operaciones)
df_operaciones_desembolsos = load_data(sheet_url_desembolsos)

# Procesar los datos
df_final, df_final_summarized = process_data(df_proyectos, df_operaciones, df_operaciones_desembolsos)

# Mostrar los DataFrames en la aplicación
st.write("DataFrame Final:")
st.dataframe(df_final)

st.write("DataFrame Final Resumido con Sumas de Monto:")
st.dataframe(result_df)

# Cargar los datos
with st.spinner('Cargando datos...'):
    df_proyectos = load_data(sheet_url_proyectos)
    df_operaciones = load_data(sheet_url_operaciones)
    df_operaciones_desembolsos = load_data(sheet_url_desembolsos)



# Función para convertir DataFrame a bytes para descargar en Excel
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

        # Definir colores para los gráficos
        color_monto = 'steelblue'
        color_porcentaje = 'firebrick'
        color_acumulado = 'goldenrod'
        
        # Asegurar que las columnas necesarias estén presentes
        if 'IDEtapa' in result_df.columns:
            # Crear una serie combinada con 'IDEtapa' y 'APODO' si está disponible
            if 'Alias' in result_df.columns:
                combined_series = result_df['IDEtapa'] + " (" + result_df['Alias'] + ")"
            else:
                combined_series = result_df['IDEtapa'].astype(str)
            
            # Ordenar las opciones alfabéticamente
            sorted_combined_series = combined_series.sort_values()

            # Usar la serie ordenada en el selectbox
            selected_combined = st.selectbox('Selecciona el Proyecto:', sorted_combined_series.unique())
            selected_etapa = selected_combined.split(" (")[0]
            filtered_df = result_df[result_df['IDEtapa'] == selected_etapa]

            if 'AporteFONPLATAVigente' in filtered_df.columns:
                # Perform calculations only if 'AporteFONPLATA' is available
                df_monto_anual = filtered_df.groupby('Ano')['Monto'].sum().reset_index()
                df_monto_acumulado_anual = df_monto_anual['Monto'].cumsum()

                aporte_total = filtered_df['AporteFONPLATAVigente'].iloc[0]
                df_porcentaje_monto_anual = (df_monto_anual['Monto'] / aporte_total * 100).round(2)
                df_porcentaje_monto_acumulado_anual = (df_monto_acumulado_anual / aporte_total * 100).round(2)

                combined_df = pd.DataFrame({
                    'Ano': df_monto_anual['Ano'],
                    'Monto': df_monto_anual['Monto'],
                    'Monto Acumulado': df_monto_acumulado_anual,
                    'Porcentaje del Monto': df_porcentaje_monto_anual,
                    'Porcentaje del Monto Acumulado': df_porcentaje_monto_acumulado_anual
                })

                st.write("Resumen de Datos:")
                st.write(combined_df)
                
                # Ensure 'Monto' is in the combined_df before attempting to modify it
                if 'Monto' in combined_df.columns:
                    combined_df['Monto'] = (combined_df['Monto'] / 1_000_000).round(3)
                # Additional code for chart generation goes here...
            else:
                st.error("La columna 'AporteFONPLATA' no está presente en los datos cargados.")
        else:
            st.error("La columna 'IDEtapa' no está presente en los datos cargados.")


        # Función para crear gráficos de líneas con puntos y etiquetas
        def line_chart_with_labels(data, x_col, y_col, title, color):
            # Gráfico de línea con puntos
            chart = alt.Chart(data).mark_line(point=alt.OverlayMarkDef(color=color, fill='black', strokeWidth=2), strokeWidth=3).encode(
                x=alt.X(f'{x_col}:N', axis=alt.Axis(title='Año', labelAngle=0)),
                y=alt.Y(f'{y_col}:Q', axis=alt.Axis(title=y_col)),
                color=alt.value(color),
                tooltip=[x_col, y_col]
            ).properties(
                title=title,
                width=600,
                height=400
            )

            # Etiquetas de texto para cada punto
            text = chart.mark_text(
                align='left',
                baseline='middle',
                dx=20,  # Desplazamiento en el eje X para evitar solapamiento con los puntos
                dy=-20  # Desplazamiento en el eje Y para alejar el texto de la línea
            ).encode(
                text=alt.Text(f'{y_col}:Q'),
                color=alt.value('black')  # Establece el color del texto a negro
            )
            return chart + text  # Combinar gráfico de línea con etiquetas

        # Crear los tres gráficos con etiquetas
        chart_monto = line_chart_with_labels(combined_df, 'Ano', 'Monto', 'Monto por Año en Millones de USD', color_monto)
        chart_porcentaje_monto = line_chart_with_labels(combined_df, 'Ano', 'Porcentaje del Monto', 'Porcentaje del Monto Desembolsado por Año', color_porcentaje)
        chart_porcentaje_monto_acumulado = line_chart_with_labels(combined_df, 'Ano', 'Porcentaje del Monto Acumulado', 'Porcentaje del Monto Acumulado por Año', color_acumulado)

        # Mostrar los gráficos en Streamlit
        st.altair_chart(chart_monto, use_container_width=True)
        st.altair_chart(chart_porcentaje_monto, use_container_width=True)
        st.altair_chart(chart_porcentaje_monto_acumulado, use_container_width=True)

if __name__ == "__main__":
    run()
