# CARGA INICIAL: From Google Drive to Google Cloud Storage 
(Primera solución)

## Primer paso: 
Copiar los datos (archivos y carpetas) del Google Drive compartido por cliente al Google Drive del proyecto.

## Segundo paso: 
Montar en Google Colab el Google Drive del proyecto.

    from google.colab import drive
    drive.mount('/content/drive')

## Tercer paso:
Autenticar usuario.

    from google.colab import auth
    auth.authenticate_user()

Usamos codigos de gsutil tool para enlazar el usuario autenticado y el proyecto en GCP usando como plataforma Google Colab.

    project_id = 'finalprojectprototype-397114'
    !gcloud config set project {project_id}
    !gsutil ls

## Cuarto paso: 
Copiamos las carpetas deseadas de Google Drive a Google Cloud Storage usando como puente Google Colab.

Para folder yelp:

    bucket_name = 'importing_drive_gcs/yelp_folder_copy'
    
    !gsutil -m cp -r /content/drive/My\ Drive/yelp/* gs://{bucket_name}/ # Este último es el directorio del drive montado en           google   colab.

para folder google maps:

    bucket_name = 'importing_drive_gcs/google_maps_copy'
      
    !gsutil -m cp -r /content/drive/My\ Drive/google_maps/* gs://{bucket_name}/

## De esta manera establecemos la conexión entre nuestro google drive y GCS para realizar la carga inicial.

## Autor: Max J.


