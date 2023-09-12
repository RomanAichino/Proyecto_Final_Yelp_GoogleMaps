import streamlit as st
import requests
from streamlit_lottie import st_lottie
from PIL import Image

st.set_page_config(page_title= 'Documentación', page_icon= '📃')

def css_load(css_file):
  with open(css_file) as file:
    st.markdown(f"<style>{file.read()}</style>", unsafe_allow_html= True)

email_contact = "romanaichino2002@gmail.com"

css_load('Style/main.css')

#funcion para animaciones
def load_lottieurl(url):
  r = requests.get(url)
  if r.status_code != 200:
    return None
  return r.json()


lottie_coding = load_lottieurl("https://assets3.lottiefiles.com/packages/lf20_0yfsb3a1.json")
Logo_principal = Image.open("Imagenes/Logo principal.jpg")
Alcance_proyecto = Image.open("Imagenes/Alcance del proyecto.jpg")
Gantt = Image.open("Imagenes/GANTT.jpeg")
Diagrama_er = Image.open("Imagenes/diagrama er.jpg")
Diagrama_googlemaps = Image.open("Imagenes/diagrama_googlemaps.jpg")
Diagrama_yelp = Image.open("Imagenes/diagrama_yelp.jpg")
Tabla1 = Image.open("Imagenes/tabla1.png")
Tabla2 = Image.open("Imagenes/tabla2.png")
Tabla3_1 = Image.open("Imagenes/tabla3.png")
Tabla3_2 = Image.open("Imagenes/tabla3_2.png")
Tabla4 = Image.open("Imagenes/tabla4.png")
Tabla5 = Image.open("Imagenes/tabla5.png")

caping1 = Image.open("Imagenes/cap_ing1.jpg")
caping2 = Image.open("Imagenes/cap_ing2.jpg")
caping3 = Image.open("Imagenes/cap_ing3.jpg")
caping4 = Image.open("Imagenes/cap_ing4.jpg")


# Crea un archivo HTML con los estilos CSS
#with open("estilos.html", "w") as f:
#    f.write("""
#    <style>
#      body {
#          background-color: #3498db; /* Color de fondo sólido */
#      }
#  </style>
#  """)

# Carga el archivo HTML con los estilos CSS
st.markdown(open("estilos.html").read(), unsafe_allow_html=True)

with st.container():
  st.write("""
    <style>
        div.stApp {
            text-align: center;
        }
    </style>
  """, unsafe_allow_html=True)

  st.subheader("Bienvenidos!! somos DREM DATA INSIGTHS, y esta es la documentación de el proyecto grupal de Reviews de Yelp y Google Maps. :wave: :wave: ")
  st.title("Documentación")

with st.container():
  st.image(Logo_principal, use_column_width=True, output_format="JPEG")
  st.write("(27/08/2023 al 15/09/1023)")

with st.container():
  st.write("---")
  st.subheader("Integrantes")
  st.markdown("<p style='text-align: left; font-size: 18px;'><strong>Roman Aichino: </strong>Data Engineer<br><strong>Max Jeffer:</strong> Data Engineer & Machine Learning<br><strong>Diego Campos:</strong> Data Engineer<br><strong>Eduardo Vivar:</strong> Data Analytics</p>", unsafe_allow_html=True)

with st.container():
  st.write("---")
  st.subheader("Introducción")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Este es un proyecto que fue realizado desde la disciplina Ciencia de Datos, que busca recolectar datos para procesarlos, analizarlos y poder tomar decisiones en base a ellos. El proyecto cuenta con un EDA, una automatización de el ETL en la nube ‘Google Cloud Platform’, una carga incrementa a partir de los datos de una API, un dashboard con KPIs, y por último, un modelo de Machine Learning que devuelve cual es la mejor opción para una decisión empresarial.</p>", unsafe_allow_html=True)
  st.lottie(lottie_coding, height= 300, key = "coding")

with st.container():
  st.write("---")
  st.subheader("Quiénes Somos y Quién es nuestro cliente?")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Nosotros somos Drem Data Insights, una empresa dedicada a la ciencia de los datos, y en este proyecto en particular nuestra tarea es crear un sistema que proporcione información sobre los restaurantes en Estados Unidos, en el estado de Florida, con el objetivo de que nuestro cliente, una empresa que quiere abrir un restaurante, pueda tomar buenas decisiones en cuanto a la ubicación y otros detalles del negocio. </p>", unsafe_allow_html=True)

with st.container():
  st.write("---")
  st.subheader("Objetivos")
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>Generales:</strong></p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>          ❖	Identificar ubicaciones y atributos estratégicos para abrir un restaurante mediante el análisis de datos a partir de la información recolectada, con el fin de maximizar la rentabilidad y la satisfacción del cliente.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>Especificos:</strong></p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>          ❖	Definir metodología de gestión general (roles, tareas, diagrama de Gantt, etc).<br>          ❖	Definir el stack tecnológico.<br>          ❖	Automatización de pipelines (carga incremental, etl, envío de datasets al datawarehouse).<br>          ❖	Análisis exploratorio de los datos para la creación de KPIs.<br>          ❖	Diseño de un dashboard interactivo en Power Bi para realizar el seguimiento y monitoreo de los KPIs<br>          ❖	Análisis de sentimiento mediante un modelo de ML.</p>", unsafe_allow_html=True)

with st.container():
  st.write("---")
  st.subheader("Alcance del Proyecto")
  st.image(Alcance_proyecto, use_column_width=True, output_format="JPEG")

with st.container():
  st.write("---")
  st.subheader("Planificación")
  st.markdown("<p style='text-align: center; font-size: 21px;'><strong>Metodología Scrum: Roles y Planificación</strong></p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 21px;'><strong>Roles en Scrum</strong></p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>❖	Product Owner:</strong><br>          Responsable de definir y priorizar el Product Backlog.<br>          Responsable de definir y priorizar el Product Backlog.<br>          Responsable de definir y priorizar el Product Backlog.<br>          Asegura que el equipo trabaje en las tareas más valiosas para el cliente.<br>          Colabora con interesados y el equipo para mantener la visión clara del producto.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>❖	Scrum Master:</strong><br>          Facilitadora del proceso Scrum.<br>          Garantiza que el equipo siga las prácticas y principios de Scrum.<br>          Elimina obstáculos y fomenta un entorno de trabajo colaborativo.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>❖	Equipo de Desarrollo:</strong><br>Campos Diego<br>Aichino Roman<br>Jeffer Max<br>Vivar Eduardo</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Transforman elementos del Product Backlog en incrementos de producto entregables.<br>Transforman elementos del Product Backlog en incrementos de producto entregables.<br>Autónomos y autoorganizados, toman decisiones sobre cómo abordar tareas.<br>Colaboran para lograr los objetivos del Sprint.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>Planificación en Scrum</strong></p>", unsafe_allow_html=True)

with st.container():
  st.write("---")
  st.subheader("Diagrama de Gantt")
  st.image(Gantt, use_column_width=True, output_format="JPEG")

with st.container():
  st.write("---")
  st.subheader("Stack Tecnológico")
  st.markdown("<p style='text-align: justify; font-size: 21px;'><strong>Parte técnica:</strong></p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Nube: Google Cloud Platform</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Google Cloud Storage: para nuestro Datalake.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>BigQuery: para nuestro Datawarehouse.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Google Colab: para la creación de archivos jupyter que automáticamente hagan la carga incremental, el ETL y la subida al Datawarehouse de todos nuestros datasets. Además, también lo utilizamos para los EDAs que quedarán subidos en el repositorio.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Cloud Functions: para la automatización de los archivos jupyter creados en Google Colab.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Pub/Sub y Cloud Scheduler: para el trigger de las funciones creadas en Cloud Functions.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Lenguaje de programación: Python, ya que es el que nuestra empresa utiliza habitualmente para procesar y analizar datos.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Librerías: </p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 16px;'>          ❖	Pandas: la mejor para procesar datasets en forma de dataframes. <br>          ❖	Mathplotlib: para la creación de gráficos que nos permitan mirar los datos desde otra perspectiva más analizable. <br>          ❖	Numpy: para ecuaciones matemáticas. <br>          ❖	Sk-learn: para nuestro modelo predictivo.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 21px;'><br><strong>Parte analítica:</strong></p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Power BI: para la creación del Dashboard interactivo.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Google Colab: para el análisis de los datos.</p>", unsafe_allow_html=True)

with st.container():
  st.write("---")
  st.subheader("Modelo de datos")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Se dispone como fuente de información los archivos de las dos plataformas, compuestos por archivos de tipo json, parquet, pkl. Durante los procesos de ETL y EDA a dichos archivos, y tomando como referencia los diccionarios de las fuentes. El modelo de datos que contiene el datawarehouse del proyecto contiene tablas de ambas plataformas.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 21px;'><strong>Tablas de la Plataforma Google Maps:</strong></p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>1. dim_gm_business: </strong>Esta tabla almacena información detallada sobre los negocios en Google Maps, incluyendo detalles de ubicación, categorías, horarios de operación entre otros.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>2. fact_gm_review: </strong>Aquí se almacenan las reseñas y calificaciones de los usuarios en relación con los negocios registrados en Google Maps, lo que nos da la posibilidad de analizar la interacción entre negocios y usuarios.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 22px;'><strong>Tablas de la Plataforma Yelp:</strong></p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>1. dim_yelp_business: </strong>En esta tabla, mantenemos información esencial sobre los negocios en Yelp, como categorías, atributos y valoraciones.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>2. dim_yelp_user: </strong>En esta tabla se encuentran registrados los usuarios de Yelp, a diferencia de googlemaps podemos tener mayor información sobre el perfil del usuario, como sus contactos y sus valoraciones.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>3. fact_yelp_review: </strong>Al igual que en el caso de Google Maps, esta tabla contiene revisiones y calificaciones de usuarios relacionadas con los negocios en Yelp, lo que nos permite evaluar la satisfacción del cliente y realizar análisis de sentimiento.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><strong>4. fact_yelp_checkin: </strong>Esta tabla contiene información sobre las visitas realizadas por los usuarios de Yelp en los negocios, lo que puede proporcionar indicios sobre la actividad y la popularidad de los lugares reflejada en la plataforma.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 21px;'><br> <strong>A.	DIAGRAMA DE ENTIDAD RELACION</strong><br>Las estructuras de datos para el datawarehouse del proyecto en la plataforma BIGQUERY, se han asignado a dos datasets:</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>googlemaps</p>", unsafe_allow_html=True)

  st.image(Diagrama_googlemaps, output_format="JPEG")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>yelp</p>", unsafe_allow_html=True)
  st.image(Diagrama_yelp, output_format="JPEG")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>A continuación, tenemos el <strong>DER</strong> (Diagrama Entidad Relación)</p>", unsafe_allow_html=True)

  width = 845
  height = 590
  Diagrama_er = Diagrama_er.resize((width, height))
  st.image(Diagrama_er, use_column_width=True, output_format="JPEG")
  st.markdown("<p style='text-align: justify; font-size: 21px;'><strong>B.	DICCIONARIO DE DATOS</strong></p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 16px;'>Usamos la nomenclatura dim_% para tablas de dimensiones, y fact_% para tablas de hechos</p>", unsafe_allow_html=True)

  st.image(Tabla1, use_column_width=True, output_format="png")
  st.image(Tabla2, use_column_width=True, output_format="png")
  st.image(Tabla3_1, use_column_width=True, output_format="png")
  st.image(Tabla3_2, use_column_width=True, output_format="png")
  st.image(Tabla4, use_column_width=True, output_format="png")
  st.image(Tabla5, use_column_width=True, output_format="png")

with st.container():
  st.write("---")
  st.subheader("Análisis de Datos")

with st.container():
  st.write("---")
  st.subheader("Ingeniería de Datos")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Ya habiendo nombrado anteriormente nuestro stack tecnológico, lo que queda es explicar cómo usamos cada herramienta, y el ciclo de vida del dato.<br>Nuestros datos principales son extraídos desde un Google Drive que nos aportó el cliente, a partir de ellos lo primero que hicimos fue realizarles el ETL con Google Colab.<br>Imagen que demuestra la creación de varios colabs en una carpeta e Google Drive:</p>", unsafe_allow_html=True)
  st.image(caping1, use_column_width=True, output_format="JPEG")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Luego de realizar tanto los ETLs como los EDAs en Google Colab, subimos los datasets a Google Cloud Storage para poder implementar los ETLs que realizamos en Google Colab, pero de una manera más automatizada a través de Google Cloud Functions.<br>Imagen que demuestra los datos en varios buckets de GCS:</p>", unsafe_allow_html=True)
  st.image(caping2, use_column_width=True, output_format="JPEG")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Imagen que demuestra la creación de funciones para los ETLs, la API y la carga incremental: </p>", unsafe_allow_html=True)
  st.image(caping3, use_column_width=True, output_format="JPEG")

  st.markdown("<p style='text-align: justify; font-size: 18px;'>Explicación de lo que hace cada función de la imagen:</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>          ❖	Apiloader: Esta función es la encargada de llamar a la API, extraer los datos y almacenarlos en GSC. Además, se ejecuta cada una hora ya que la API tiene ese tiempo de actualización.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>          ❖	carga_incremental_intento_1: Esta función cumple varios roles, primero en principal es la encargada de llevar un registro en cuanto a los archivos que ya fueron cargados en BigQuery (nuestro datawarehouse) con el objetivo de que, si la API llegase a fallar, no se almacenen datos repetidos; y, en segundo lugar, la función también es la encargada de llevar los datos a una tabla de BigQuery en donde se van almacenando debajo de los datos ya existentes.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>          ❖	ETL: Aquí están los ETLs, que como previamente mencioné, fueron creados en Google Colab de forma provisoria. Los objetivos de este pipeline, son el de juntar todos los datasets del mismo tipo para crear uno que pueda ser procesado y enviado a tablas de BigQuery. </p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'>La diferencia entre esta función y la de carga incremental, es que los datos de Google Drive jamás serán actualizados, por lo que esta función quedaría automatizada y lista para recibir actualizaciones, pero no ocurrirá a menos que sea manualmente mediante un dataset creado por nosotros.</p>", unsafe_allow_html=True)
  st.markdown("<p style='text-align: justify; font-size: 18px;'><br>(Tanto ETL_GoogleMaps_Review_Florida como gco_to_bigquery son funciones que implementamos en un principio pero que luego descartamos ante la creación de otras mejores)<br>Imagen que demuestra la presencia de las tablas con los datos en BigQuery:</p>", unsafe_allow_html=True)
  st.image(caping4, use_column_width=True, output_format="JPEG")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>Como vemos a la izquierda, tenemos 3 conjuntos de datos llamados ‘googlemaps’, ‘inremental_load’ y ‘yelp’, y cada uno tiene dentro las tablas que le corresponden.</p>", unsafe_allow_html=True)

with st.container():
  st.write("---")
  st.subheader("Modelo de Machine Learning")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>aaa</p>", unsafe_allow_html=True)

with st.container():
  st.write("---")
  st.subheader("KPIs (Indicadores Clave de Desempeño)")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>aaa</p>", unsafe_allow_html=True)

with st.container():
  st.write("---")
  st.subheader("Dashboard")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>aaa</p>", unsafe_allow_html=True)

with st.container():
  st.write("---")
  st.subheader("Conclusiones")
  st.markdown("<p style='text-align: justify; font-size: 18px;'>aaa</p>", unsafe_allow_html=True)

with st.container():
  st.write("---")
  st.subheader("Referencias")
  st.write("[Página donde se encuentra la API utilizada en el proyecto >>](https://www.iqair.com/)")
##enctype="multipart/form-data"
with st.container():
  st.write("---")
  st.subheader("Si tienes alguna consulta, no dudes en escribirnos! 📩")
  contact_form = f'''
  <form action="https://formsubmit.co/{email_contact}" method="POST">
  <input type="hidden" name="_captcha" value="false">  
    <input type="text" name="Nombre y Apellido" placeholder='Nombre y apellido:' required>
    <input type="email" name="Mail" placeholder= 'Mail:'>
    <textarea type="message" name="message" placeholder="Detalles de su duda:" required></textarea>
    <button type="submit">Enviar</button>
  </form>
'''
  st.markdown(contact_form, unsafe_allow_html=True)
