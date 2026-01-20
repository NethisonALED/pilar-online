// --- CONFIGURAÇÃO E INICIALIZAÇÃO DO SUPABASE ---
// Esta instância é exportada para ser usada em toda a aplicação.
// Importa e configura o dotenv
import { CONFIG } from './config.js';

let supabaseInstance = null;

if (CONFIG.SUPABASE.URL && CONFIG.SUPABASE.ANON_KEY) {
    try {
        supabaseInstance = window.supabase.createClient(
            CONFIG.SUPABASE.URL, 
            CONFIG.SUPABASE.ANON_KEY
        );
    } catch (e) {
        console.error("Erro ao inicializar Supabase:", e);
    }
} else {
    // Falha silenciosa na inicialização, a UI deve tratar isso
    console.error("CONFIGURAÇÃO DE SEGURANÇA AUSENTE: Não foi possível carregar as chaves do Supabase via proxy PHP.");
    // Exibe overlay de erro fatal
    window.addEventListener('load', () => {
         document.body.innerHTML = `
            <div style="position:fixed;top:0;left:0;width:100%;height:100%;background:#0f172a;color:white;display:flex;flex-direction:column;align-items:center;justify-content:center;z-index:9999;text-align:center;font-family:sans-serif;">
                <h1 style="color:#ef4444;font-size:2rem;margin-bottom:1rem;">Erro de Configuração de Segurança</h1>
                <p>O sistema não conseguiu carregar as chaves de acesso de forma segura.</p>
                <p style="margin-top:1rem;color:#94a3b8;">Motivo provável: O servidor PHP não está rodando ou não está acessível.</p>
                <code style="background:#1e293b;padding:1rem;border-radius:0.5rem;margin-top:2rem;font-family:monospace;">php -S localhost:8000</code>
                <p style="margin-top:1rem;color:#94a3b8;">Execute o comando acima na pasta do projeto e acesse via <a href="http://localhost:8000" style="color:#38bdf8;">http://localhost:8000</a></p>
            </div>
         `;
    });
}

export const supabase = supabaseInstance;


// --- FUNÇÕES UTILITÁRIAS EXPORTADAS ---

/**
 * Converte um objeto de arquivo (File) para uma string Base64.
 * @param {File} file - O arquivo a ser convertido.
 * @returns {Promise<string>} Uma promessa que resolve com a URL de dados Base64.
 */
export const fileToBase64 = (file) => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.readAsDataURL(file);
    reader.onload = () => resolve(reader.result);
    reader.onerror = error => reject(error);
});

/**
 * Converte um array de objetos JSON para uma Data URL de um arquivo XLSX (Excel).
 * @param {Array<Object>} jsonData - Os dados a serem convertidos.
 * @returns {string} A URL de dados para o arquivo XLSX.
 */
export const jsonToXLSXDataURL = (jsonData) => {
    const ws = XLSX.utils.json_to_sheet(jsonData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Sysled Import");
    const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'base64' });
    return "data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64," + wbout;
};

/**
 * Converte um número ou string vindo da API (com vírgula decimal) para um número.
 * @param {*} value - O valor a ser convertido.
 * @returns {number} O valor convertido para número.
 */
export const parseApiNumber = (value) => {
    if (value === null || value === undefined) return 0;
    return Number(String(value).replace(',', '.')) || 0;
};

/**
 * Converte uma string de moeda formatada (ex: "R$ 1.234,56") para um número.
 * @param {*} value - A string de moeda.
 * @returns {number} O valor numérico.
 */
export const parseCurrency = (value) => {
    if (typeof value === 'number') return value;
    if (typeof value !== 'string' || value === null) return 0;
    return parseFloat(String(value).replace(/R\$\s?/, '').replace(/\./g, '').replace(',', '.')) || 0;
};

/**
 * Formata um número como uma string de moeda brasileira (BRL).
 * @param {number|string} value - O valor a ser formatado.
 * @returns {string} A string formatada (ex: "R$ 1.234,56").
 */
export const formatCurrency = (value) => {
    const number = parseCurrency(value);
    return number.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
};

/**
 * Formata uma string de data do tipo ISO (YYYY-MM-DDTHH:mm:ss) para o formato brasileiro (DD/MM/YYYY).
 * @param {string} dateString - A string de data.
 * @returns {string} A data formatada.
 */
export const formatApiDateToBR = (dateString) => {
     if (!dateString || typeof dateString !== 'string') return '';
     const datePart = dateString.split('T')[0];
     const parts = datePart.split('-');
     if (parts.length !== 3) return dateString; 
     const [year, month, day] = parts;
     return `${day}/${month}/${year}`;
};

/**
 * Formata um número vindo da API como uma string com formatação brasileira (ponto como milhar, vírgula como decimal).
 * @param {*} value - O valor a ser formatado.
 * @returns {string} A string numérica formatada.
 */
export const formatApiNumberToBR = (value) => {
    if (value === null || value === undefined || value === '') return '';
    const number = parseApiNumber(value);
    if (isNaN(number)) return value;
    return number.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};

/**
 * Escapa caracteres HTML perigosos para prevenir XSS.
 * @param {string} str - A string a ser escapada.
 * @returns {string} A string segura para inserção em HTML.
 */
export const escapeHTML = (str) => {
    if (!str) return "";
    return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
};

/**
 * Exibe uma notificação do tipo Toast.
 * @param {string} message - A mensagem a ser exibida.
 * @param {string} type - O tipo da notificação ('success', 'error', 'info', 'warning').
 * @param {number} duration - Duração em ms (padrão 3000).
 */
export const showToast = (message, type = 'info', duration = 3000) => {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    
    // Icon selection
    let icon = 'info-circle';
    if (type === 'success') icon = 'check-circle';
    if (type === 'error') icon = 'exclamation-circle';
    if (type === 'warning') icon = 'exclamation-triangle';

    // Title translation
    const titles = {
        success: 'Sucesso',
        error: 'Erro',
        info: 'Informação',
        warning: 'Atenção'
    };

    toast.innerHTML = `
        <div class="toast-icon"><i class="fas fa-${icon}"></i></div>
        <div class="toast-content">
            <div class="toast-title">${titles[type] || 'Notificação'}</div>
            <div class="toast-message">${escapeHTML(message)}</div>
        </div>
        <button class="toast-close"><i class="fas fa-times"></i></button>
        <div class="toast-progress">
            <div class="toast-progress-bar" style="transition-duration: ${duration}ms;"></div>
        </div>
    `;

    container.appendChild(toast);

    // Trigger animation
    requestAnimationFrame(() => {
        toast.classList.add('show');
        const progressBar = toast.querySelector('.toast-progress-bar');
        if (progressBar) {
            progressBar.style.transform = 'scaleX(0)';
        }
    });

    const removeToast = () => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 400);
    };

    const timeout = setTimeout(removeToast, duration);

    const closeBtn = toast.querySelector('.toast-close');
    closeBtn.addEventListener('click', () => {
        clearTimeout(timeout);
        removeToast();
    });
};

