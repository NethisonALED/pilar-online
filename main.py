import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env para o ambiente
load_dotenv()

# Acessa as variáveis de ambiente do Sysled
api_sysled_url = os.getenv("API_SYSLED_URL")
api_sysled_key = os.getenv("API_SYSLED_KEY")

# Acessa as variáveis de ambiente do Supabase (essenciais para o seu projeto Pilar)
api_supabase_url = os.getenv("API_SUPABASE_URL")
api_supabase_key = os.getenv("API_SUPABASE_KEY")

# ---- A partir daqui, você usa as variáveis no seu código ----

# Exemplo de como verificar se foram carregadas
print("Verificando variáveis de ambiente...")
if all([api_sysled_url, api_sysled_key, api_supabase_url, api_supabase_key]):
    print("✅ Todas as chaves e URLs foram carregadas com sucesso!")
else:
    print("❌ Erro: Uma ou mais variáveis de ambiente não foram encontradas.")

