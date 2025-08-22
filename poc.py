from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

# =========================
# CREDENCIAIS FIXAS
# =========================
client_id = ""
client_secret = ""
site_url = ""
base_folder_url = ""

# =========================
# AUTENTICAÇÃO
# =========================
credentials = ClientCredential(client_id, client_secret)
ctx = ClientContext(site_url).with_credentials(credentials)

# Lista para armazenar os caminhos dos arquivos
all_file_paths = []

# =========================
# FUNÇÃO RECURSIVA
# =========================
def explore_folder(folder_url, relative_path=""):
    folder = ctx.web.get_folder_by_server_relative_url(folder_url)
    folder.expand(["Folders", "Files"]).get().execute_query()

    # Adiciona arquivos da pasta atual
    for file in folder.files:
        full_path = f"{relative_path}{file.properties['Name']}"
        if full_path.endswith((".txt", ".lst")):
            all_file_paths.append(full_path)

    # Recursão para subpastas
    for subfolder in folder.folders:
        subfolder_name = subfolder.properties['Name']
        subfolder_url = f"{folder_url}/{subfolder_name}"
        subfolder_relative_path = f"{relative_path}{subfolder_name}/"
        explore_folder(subfolder_url, subfolder_relative_path)

# =========================
# INÍCIO DA RECURSÃO
# =========================
explore_folder(base_folder_url)

# =========================
# IMPRIME RESULTADO FINAL
# =========================
print("\n📁 Lista completa de arquivos:\n")
for path in all_file_paths:
    print(path)
