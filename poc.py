from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.client_credential import ClientCredential

# Configurações do SharePoint


# Autenticação
credentials = ClientCredential(client_id, client_secret)
ctx = ClientContext(site_url).with_credentials(credentials)

# Acessando a pasta
folder = ctx.web.get_folder_by_server_relative_url(folder_url)
files = folder.files
ctx.load(files)
ctx.execute_query()

# Listando arquivos
for file in files:
    print(file.properties["Name"])
