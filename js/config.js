// js/config.js - VERSÃO HÍBRIDA (DEV + PROD)

let serverConfig = {
    SUPABASE: { URL: "", ANON_KEY: "" },
    SYSLED: { API_URL: "", AUTH_TOKEN: "" }
};

try {
    // 1. Define a URL padrão (Funciona na Hostinger/Produção)
    let proxyUrl = `sysled-proxy.php?get_config=1&t=${Date.now()}`;

    // 2. Se estiver local (Live Server), aponta para o servidor PHP local
    if (window.location.port === '5500' || window.location.port === '3000') {
        proxyUrl = `http://localhost:8080/sysled-proxy.php?get_config=1&t=${Date.now()}`;
    }

    const response = await fetch(proxyUrl);
    
    if (response.ok) {
        const data = await response.json();
        if (data.SUPABASE) {
            serverConfig.SUPABASE.URL = data.SUPABASE.URL;
            serverConfig.SUPABASE.ANON_KEY = data.SUPABASE.ANON_KEY;
        }
    } else {
        console.error("Config fetch falhou:", response.status);
    }
} catch (error) {
    console.error("Erro crítico config (verifique se o PHP server está rodando):", error);
}

export const CONFIG = {
    SUPABASE: {
        URL: serverConfig.SUPABASE.URL,
        ANON_KEY: serverConfig.SUPABASE.ANON_KEY,
    },
    SYSLED: {
        // Lógica Híbrida para a API também
        API_URL: (window.location.port === '5500' || window.location.port === '3000') 
            ? "http://localhost:8080/sysled-proxy.php" 
            : "sysled-proxy.php", 
        AUTH_TOKEN: "",
    },  
};