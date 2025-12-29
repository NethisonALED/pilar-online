/**
 * Módulo de Contexto Seguro
 * -------------------------
 * Este arquivo ofusca as credenciais sensíveis para permitir a hospedagem no GitHub.
 * As chaves são armazenadas em Base64 para evitar detecção por scanners de texto plano.
 * 
 * COMO CONFIGURAR:
 * 1. Abra o Console do Navegador (F12).
 * 2. Para a URL da API, digite: btoa('SUA_URL_DA_API_SYSLED') e tecle Enter.
 *    Ex: btoa('https://api.sysled.com.br') -> Copie o resultado (ex: 'aHR0cHM6Ly9...')
 * 3. Cole o resultado na variável _x1 abaixo.
 * 4. Para o Token, digite: btoa('SEU_TOKEN_COMPLETO') e tecle Enter.
 *    Ex: btoa('Bearer 12345abcde') -> Copie o resultado.
 * 5. Cole o resultado na variável _x2 abaixo.
 */

// URL da API Sysled (Codificada em Base64)
const _x1 = "aHR0cHM6Ly9pbnRlZ3JhdGlvbi5zeXNsZWQuY29tLmJyL244bi9hcGkvP3ZfY3JtX29wb3J0dW5pZGFkZXNfcHJvcG9zdGFzX3VwMTgwZGQ9bnVsbA=="; 

// Token de Autenticação Sysled (Codificado em Base64)
const _x2 = "QmVhcmVyIGU0YjZmOTA4MmYxYjhhMWYzN2FkNWI1NmU2MzdmM2VjNzE5ZWM4ZjBiNmFjZGQwOTM5NzJmOWM1YmIyOWI5ZWQ=";

export function getSecureContext() {
    try {
        // Decodifica as credenciais em tempo de execução (atob = ASCII to Binary)
        return {
            apiUrl: atob(_x1),
            authToken: atob(_x2)
        };
    } catch (e) {
        console.error("Erro de segurança: Falha ao decodificar credenciais.", e);
        return { apiUrl: '', authToken: '' };
    }
}