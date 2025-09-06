from io import BytesIO
from anubis_core.ports.cloud_documents import ICloudDocumentsAdapter

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload


class GoogleDriveDocumentsAdapters(ICloudDocumentsAdapter):

    def __init__(self,
                 credentials_file_path,
                 email):
        SCOPES = ['https://www.googleapis.com/auth/drive']

        credentials = service_account.Credentials.from_service_account_file(
            credentials_file_path,
            scopes=SCOPES,
            subject=email
        )

        self.service = build('drive', 'v3', credentials=credentials)


        
    def push(self,  folder, filename, content):
        file_metadata = {
                'name': filename,
                'mimeType': 'application/vnd.google-apps.document',
                'parents': [folder]
                }
        content_stream = BytesIO(content.encode('utf-8'))
        media = MediaIoBaseUpload(content_stream, mimetype='text/plain', resumable=True)
        file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"Documento creado con ID: {file.get('id')}")
        pass

    def pull(self, folder, filename):
        # Buscar el archivo por nombre dentro de la carpeta
        query = f"'{folder}' in parents and name = '{filename}' and trashed = false"
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])

        if not files:
            print(f"No se encontró el archivo '{filename}' en la carpeta '{folder}'")
            return None

        file_id = files[0]['id']

        # Descargar el contenido
        request = self.service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)
        content = fh.read().decode('utf-8')
        return content


    def list(self, folder):
        query = f"'{folder}' in parents and trashed = false"
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])

        file_list = [file['name'] for file in files]
        return file_list

    
        