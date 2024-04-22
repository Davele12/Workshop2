from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

credentials_path = 'DavidWorkshop2/dags/credentials.json'

def upload_to_drive(filename, filepath):
    # Configuración del archivo client_secrets.json
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = credentials_path
    
    gauth = GoogleAuth()
    # Intenta cargar las credenciales existentes
    gauth.LoadCredentialsFile("credentials.json")
    
    if gauth.credentials is None:
        # Si no hay credenciales guardadas, realiza la autenticación
        # Esto abrirá un navegador para que el usuario inicie sesión y autorice la aplicación
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Si el token de acceso expiró, refresca el token
        gauth.Refresh()
    else:
        # Si el token está presente y no ha expirado, autoriza las operaciones
        gauth.Authorize()

    # Guarda el token de acceso actual para futuras sesiones
    gauth.SaveCredentialsFile("credentials.json")
    
    drive = GoogleDrive(gauth)

    # Crear y subir un nuevo archivo
    file = drive.CreateFile({'title': filename})
    file.SetContentFile(filepath)
    file.Upload()

    print(f'Archivo {filename} subido con éxito.')

# Asegúrate de ajustar las rutas a 'client_secrets.json' y 'credentials.json' correctamente
upload_to_drive('Merged_data.csv', '/path_to_your_file/merged_data.csv')