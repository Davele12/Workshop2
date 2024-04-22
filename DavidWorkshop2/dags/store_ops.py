from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

credentials_path = 'DavidWorkshop2/dags/credentials.json'

def upload_to_drive(filename, filepath):
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = credentials_path
    
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile("credentials.json")
    
    if gauth.credentials is None:
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()

    gauth.SaveCredentialsFile("credentials.json")
    
    drive = GoogleDrive(gauth)

    file = drive.CreateFile({'title': filename})
    file.SetContentFile(filepath)
    file.Upload()

    print(f'Archivo {filename} subido con éxito.')

# Asegúrate de ajustar las rutas a 'client_secrets.json' y 'credentials.json' correctamente
upload_to_drive('Merged_data.csv', '/path_to_your_file/merged_data.csv')