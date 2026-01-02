<?php
// sysled-proxy.php
// Versão compatível com Hostinger/Hostgator

// Habilita exibição de erros temporariamente para você ver o que está acontecendo
ini_set('display_errors', 0);
error_reporting(E_ALL);

// Função para capturar erros fatais e devolver JSON
register_shutdown_function(function() {
    $error = error_get_last();
    if ($error !== NULL && $error['type'] === E_ERROR) {
        header('Content-Type: application/json');
        http_response_code(500);
        echo json_encode(['error' => true, 'message' => 'Fatal Error: ' . $error['message']]);
    }
});

// Configurações de CORS
header("Access-Control-Allow-Origin: *");
header("Access-Control-Allow-Methods: GET, POST, OPTIONS");
header("Access-Control-Allow-Headers: Content-Type, Authorization");
header("Content-Type: application/json");

// Trata requisição OPTIONS (Pre-flight do navegador)
if ($_SERVER['REQUEST_METHOD'] == 'OPTIONS') {
    http_response_code(200);
    exit();
}

// --- FUNÇÃO ROBUSTA PARA LER .ENV ---
function getEnvValue($key) {
    $envPath = __DIR__ . '/.env';
    
    if (!file_exists($envPath)) {
        throw new Exception("Arquivo .env não encontrado em: " . $envPath);
    }

    $lines = file($envPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    foreach ($lines as $line) {
        if (strpos(trim($line), '#') === 0) continue;
        
        // Quebra na primeira ocorrência de '='
        $parts = explode('=', $line, 2);
        
        if (count($parts) === 2) {
            $name = trim($parts[0]);
            $value = trim($parts[1]);
            // Remove aspas simples ou duplas se existirem
            $value = trim($value, '"\'');
            
            if ($name === $key) {
                return $value;
            }
        }
    }
    return null;
}

try {
    // 1. Busca as credenciais
    $sysledUrl = getEnvValue('API_SYSLED_URL');
    $sysledToken = getEnvValue('API_SYSLED_KEY');

    if (!$sysledUrl || !$sysledToken) {
        throw new Exception("Variáveis API_SYSLED_URL ou API_SYSLED_KEY não encontradas no .env");
    }

    // // 2. Garante que o Token tenha "Bearer"
    // if (strpos($sysledToken, 'Bearer') === false) {
    //     // Se no .env tiver chaves ou JSON, limpa tudo, queremos só o hash
    //     // Ajuste: Se o token no .env for limpo, apenas adiciona Bearer
    //     $sysledToken = "Bearer " . $sysledToken; 
    // }

    // 3. Prepara a URL final (mantendo query params do JS)
    $queryString = $_SERVER['QUERY_STRING'] ?? '';
    $finalUrl = $sysledUrl;
    
    // Se o JS mandou filtros, adiciona na URL
    if (!empty($queryString)) {
        $separator = (strpos($sysledUrl, '?') !== false) ? '&' : '?';
        $finalUrl .= $separator . $queryString;
    }

    // 4. Inicializa o CURL
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $finalUrl);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    // IMPORTANTE PARA HOSTINGER: Desabilita verificação SSL estrita se der erro de certificado
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false); 
    
    curl_setopt($ch, CURLOPT_HTTPHEADER, [
        "Authorization: " . $sysledToken,
        "Content-Type: application/json"
    ]);

    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $curlError = curl_error($ch);
    
    curl_close($ch);

    if ($curlError) {
        throw new Exception("Erro no CURL: " . $curlError);
    }

    // 5. Retorna para o JS
    http_response_code($httpCode);
    echo $response;

} catch (Exception $e) {
    http_response_code(500);
    echo json_encode([
        "error" => true,
        "message" => $e->getMessage()
    ]);
}
?>